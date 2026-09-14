# Local Dev — Running the Prototype

Deliberately cut-down single-instrument prototype: **Binance spot, BTCUSDT only**,
per [docs/03-mvp-scope.md](./03-mvp-scope.md). This stack proves out collection →
broker → normalization → storage → serving (REST + MCP). Extensible later to more
exchanges/symbols/segments.

## Services

10 services, defined in `docker-compose.yml` (repo root):

- `redis` — Redis Streams broker (`noeviction` maxmemory policy; this prototype has
  no cache layer yet, so the whole instance is broker-only).
- `clickhouse` — storage; schema auto-applied from `infra/clickhouse/migrations/*.sql`
  on first startup (empty data volume only).
- `collector` — connects to Binance public WS/REST (no API key needed), writes raw
  messages to Redis Streams.
- `normalizer` — consumes Redis Streams, normalizes, batch-inserts into ClickHouse.
- `api` — read-only REST + MCP serving layer over ClickHouse (`src/api/`). Depends
  only on `clickhouse` (the read path never touches the broker). See "API / MCP
  service" below.
- `loki`, `cadvisor`, `prometheus`, `grafana` — logs + container metrics. See
  "Logging & monitoring" below.
- `traefik` — single entry point fronting `api`/`grafana`/`prometheus`/`cadvisor`.
  See "Single entry point (Traefik)" below.

## Run it

```bash
# optional: copy and tweak host ports / instrument selection
cp .env.example .env

# one-time host setup: install the Loki logging driver plugin (redis/clickhouse/
# collector/normalizer/api all ship their stdout logs to Loki through it)
docker plugin install grafana/loki-docker-driver:latest --alias loki --grant-all-permissions

docker compose up --build
```

First startup runs the ClickHouse migrations (`infra/clickhouse/migrations/001..004`)
against a fresh `clickhouse-data` volume. Collector and normalizer start once their
dependencies report healthy.

## Check data landed

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM market_data.trades"
```

Other tables to spot-check: `market_data.orderbook_events`, `market_data.orderbook_snapshots`,
`market_data.incidents`.

You can also hit the HTTP interface directly:

```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+market_data.trades"
```

## API / MCP service

`api` serves both a REST API and an MCP server from one FastAPI process
(`src/api/main.py`), reading (read-only) from the same ClickHouse tables the
normalizer writes. Default `exchange`/`segment` come from the `EXCHANGE`/`SEGMENT`
env vars (same defaults as collector/normalizer: `binance`/`spot`) but every
endpoint accepts them as optional overrides, so a later multi-segment setup is
just a query param, not a rewrite.

Before testing, confirm data has actually landed (give the pipeline a minute or
two after `docker compose up`):

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM market_data.trades"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM market_data.orderbook_snapshots"
```

`orderbook_snapshots` only gets its first row after `BOOK_SNAPSHOT_INTERVAL_S`
(default 60s) has elapsed — `/orderbook/at` will 404 until then.

### REST endpoints

```bash
# Recent trades
curl "http://api.localhost/trades?symbol=BTCUSDT&limit=5"

# Order book reconstructed at a point in time (use a recent ISO-8601 UTC timestamp,
# at/after the first orderbook_snapshots row)
curl "http://api.localhost/orderbook/at?symbol=BTCUSDT&ts=2026-09-13T12:00:00Z"

# Raw order-book diff events, paginated via the opaque `next_cursor`
curl "http://api.localhost/orderbook/events?symbol=BTCUSDT&limit=5"
curl "http://api.localhost/orderbook/events?symbol=BTCUSDT&limit=5&cursor=<next_cursor from previous response>"

# Data-quality incidents (collector disconnects, order-book sequence breaks)
curl "http://api.localhost/incidents?symbol=BTCUSDT"

# Funding rate + mark/index price, open interest, and liquidations, combined
curl "http://api.localhost/derivatives?symbol=BTCUSDT&segment=usdtm"

# Static list of known data-collection gaps (see src/api/known_limitations.py;
# currently one entry -- Binance forceOrder's partial liquidation coverage)
curl "http://api.localhost/known-limitations"
```

Interactive OpenAPI docs are also available at `http://api.localhost/docs`.

### MCP endpoint

The same process exposes an MCP server (streamable-http transport) mounted at
`http://api.localhost/mcp`. Any MCP client configured with that URL can
discover and call all 5 tools that match docs/03-mvp-scope.md's tool list:
`get_trades`, `get_orderbook_at`, `get_orderbook_events`, `get_derivatives_metrics`,
`list_data_incidents`. Each tool is a thin wrapper over the same
`src/api/queries.py` functions the REST routes call, so both protocols return
identical data for the same query.

## Logging & monitoring

Every app/infra service (except `loki` itself, to avoid a bootstrap loop) ships
its stdout logs to Loki via the Docker `loki` logging driver — no code changes,
no Promtail, no per-container log config beyond what's already in
`docker-compose.yml`. Container-level CPU/memory/network metrics come from
`cadvisor` (reads the same cgroups every other container in the stack runs
under, so it works unmodified under Docker Desktop's Linux VM too), scraped by
`prometheus`. `grafana` comes up with both as pre-provisioned datasources
(`infra/monitoring/grafana/provisioning/datasources/datasources.yml`) — no
manual datasource setup needed.

```
Grafana:     http://grafana.localhost      (login: admin / $GRAFANA_ADMIN_PASSWORD, default "admin")
Prometheus:  http://prometheus.localhost
cAdvisor UI: http://cadvisor.localhost
Loki API:    http://localhost:${LOKI_HOST_PORT:-3100}   (queried through Grafana's Explore view, not directly)
```

(Grafana/Prometheus/cAdvisor are routed through Traefik by hostname — see
"Single entry point (Traefik)" below for why. Loki is the exception: its push
endpoint is called by the Docker logging driver itself, not a browser, so it
keeps a direct published port.)

In Grafana → Explore, pick the **Loki** datasource and query e.g.
`{service="api"}` or `{service="normalizer"}` to tail a service's logs (labels
are `service=<name>,project=market-data`, set per-container in
`docker-compose.yml`). Pick **Prometheus** and query
`container_memory_usage_bytes{name=~"market-data.*"}` or
`rate(container_cpu_usage_seconds_total[1m])` for container resource graphs.

### Default dashboard

Grafana opens straight into a provisioned **"Market Data Stack — Overview"**
dashboard (`infra/monitoring/grafana/dashboards/market-data-overview.json`) —
no manual setup, no empty home screen. It has per-container CPU/memory/network
panels (Prometheus) and a combined service-logs panel (Loki, `{project="market-data"}`).

To change what's shown by default:
- **Edit the existing dashboard**: edit it in the Grafana UI, then "Save
  dashboard" → "Export" → "Save to file", and overwrite
  `infra/monitoring/grafana/dashboards/market-data-overview.json` with the
  exported JSON (`updateIntervalSeconds: 30` in
  `infra/monitoring/grafana/provisioning/dashboards/dashboards.yml` means
  edits made only in the UI don't survive a container recreate — commit them
  back to the JSON file to persist).
- **Add more dashboards**: drop additional `*.json` files into
  `infra/monitoring/grafana/dashboards/` — the `market-data` provider picks up
  anything in that folder automatically.
- **Point at a different default**: change
  `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` in `docker-compose.yml`'s
  `grafana` service to another file under `/var/lib/grafana/dashboards/`.

Requires the one-time `docker plugin install grafana/loki-docker-driver ...`
step above — without it, `docker compose up` still works, but the app
containers will fail to start with a `"logging: cannot find plugin loki"`-style
error since `docker-compose.yml` hardcodes the `loki` driver for those
services. Re-run the plugin install command if you see that.

## Single entry point (Traefik)

`api`, `grafana`, `prometheus`, and `cadvisor` — the 4 human-facing web
UIs/APIs in this stack — no longer publish their own host ports. They're only
reachable through `traefik` on one published port
(`${TRAEFIK_HOST_PORT:-80}`), routed by `Host` header:

```
http://api.localhost         -> api (REST + MCP + /docs)
http://grafana.localhost     -> grafana
http://prometheus.localhost  -> prometheus
http://cadvisor.localhost    -> cadvisor
http://traefik.localhost     -> traefik's own dashboard (routing/health at a glance)
```

`*.localhost` hostnames resolve to `127.0.0.1` in browsers and most tools
without editing `/etc/hosts` (reserved by RFC 6761); if some tool on your
machine doesn't honor that, add explicit entries or use `curl --resolve`.

Traefik discovers routes from Docker labels
(`traefik.http.routers.<name>.rule=Host(...)` on each service in
`docker-compose.yml`) — to front a new service, add its labels there, no
central proxy config file to hand-edit. `redis`/`clickhouse`/`loki` are
data-plane, not browser-facing, so they intentionally keep their own direct
ports (`REDIS_HOST_PORT`/`CLICKHOUSE_HTTP_PORT`/`CLICKHOUSE_NATIVE_PORT`/`LOKI_HOST_PORT`)
rather than going through Traefik.

**Planned next step (unified auth)**: this is set up so an auth layer can be
added as one Traefik middleware (e.g. `forwardauth` against an auth service,
or `basicauth` for a quick start) applied via a `traefik.http.routers.<name>.middlewares=...`
label on each of the 4 services above — no per-app auth code, and it's added
once you're ready rather than as part of this change.

## Stop

```bash
docker compose down
```

## Clean wipe (for a fresh 1-day test run)

```bash
docker compose down -v
```

This drops the named `redis-data`, `clickhouse-data`, `loki-data`,
`prometheus-data`, and `grafana-data` volumes, so the next `docker compose up`
starts from an empty ClickHouse (migrations re-run), an empty Redis broker, and
empty logs/metrics/dashboards.
