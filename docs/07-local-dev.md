# Local Dev — Running the Prototype

Deliberately cut-down single-instrument prototype: **Binance spot, BTCUSDT only**,
per [docs/03-mvp-scope.md](./03-mvp-scope.md). This stack proves out collection →
broker → normalization → storage → serving (REST + MCP). Extensible later to more
exchanges/symbols/segments.

## Services

5 services, defined in `/workspace/docker-compose.yml`:

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

## Run it

```bash
# optional: copy and tweak host ports / instrument selection
cp .env.example .env

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
curl "http://localhost:8000/trades?symbol=BTCUSDT&limit=5"

# Order book reconstructed at a point in time (use a recent ISO-8601 UTC timestamp,
# at/after the first orderbook_snapshots row)
curl "http://localhost:8000/orderbook/at?symbol=BTCUSDT&ts=2026-09-13T12:00:00Z"

# Raw order-book diff events, paginated via the opaque `next_cursor`
curl "http://localhost:8000/orderbook/events?symbol=BTCUSDT&limit=5"
curl "http://localhost:8000/orderbook/events?symbol=BTCUSDT&limit=5&cursor=<next_cursor from previous response>"

# Data-quality incidents (collector disconnects, order-book sequence breaks)
curl "http://localhost:8000/incidents?symbol=BTCUSDT"

# Static list of known data-collection gaps (empty in this prototype -- see
# src/api/known_limitations.py; this prototype doesn't collect forceOrder/
# liquidations data at all, so there's nothing to declare yet)
curl "http://localhost:8000/known-limitations"
```

Interactive OpenAPI docs are also available at `http://localhost:8000/docs`.

### MCP endpoint

The same process exposes an MCP server (streamable-http transport) mounted at
`http://localhost:8000/mcp`. Any MCP client configured with that URL can
discover and call the 4 tools that match docs/03-mvp-scope.md's tool list:
`get_trades`, `get_orderbook_at`, `get_orderbook_events`, `list_data_incidents`.
(`get_derivatives_metrics` is intentionally not implemented — this prototype is
spot-only with no funding/OI/liquidations data to back it.) Each tool is a thin
wrapper over the same `src/api/queries.py` functions the REST routes call, so
both protocols return identical data for the same query.

## Stop

```bash
docker compose down
```

## Clean wipe (for a fresh 1-day test run)

```bash
docker compose down -v
```

This drops the named `redis-data` and `clickhouse-data` volumes, so the next
`docker compose up` starts from an empty ClickHouse (migrations re-run) and an
empty Redis broker.
