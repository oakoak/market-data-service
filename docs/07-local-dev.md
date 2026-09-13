# Local Dev — Running the Prototype

Deliberately cut-down single-instrument prototype: **Binance spot, BTCUSDT only**,
per [docs/03-mvp-scope.md](./03-mvp-scope.md). No API/MCP service yet — this stack
only proves out collection → broker → normalization → storage. Extensible later to
more exchanges/symbols/segments and the API/MCP layer.

## Services

4 services, defined in `/workspace/docker-compose.yml`:

- `redis` — Redis Streams broker (`noeviction` maxmemory policy; this prototype has
  no cache layer yet, so the whole instance is broker-only).
- `clickhouse` — storage; schema auto-applied from `infra/clickhouse/migrations/*.sql`
  on first startup (empty data volume only).
- `collector` — connects to Binance public WS/REST (no API key needed), writes raw
  messages to Redis Streams.
- `normalizer` — consumes Redis Streams, normalizes, batch-inserts into ClickHouse.

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
