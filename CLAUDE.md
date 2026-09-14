# Market Data Service — CLAUDE.md

Historical/real-time crypto market-data service (tardis.dev analog), closing specific
gaps (point-in-time reconstruction, machine-readable incidents, dual timestamps) —
not a 1:1 clone. Full context: `docs/` (start with `docs/01-idea-and-market.md`,
`docs/03-mvp-scope.md`, `docs/04-architecture/00-overview.md`).

## Current state

Local prototype only: **Binance spot BTCUSDT**, trades + L2 order book, no auth,
no cache layer. Gap vs. full MVP scope tracked in `docs/08-prototype-roadmap.md`.
Don't assume multi-exchange, multi-instrument, or auth exist — check the roadmap
doc before building on top of them.

## Stack (see docs/04-architecture/01-stack.md for rationale)

- Python, `websockets` for exchange WS clients (not `aiohttp`) — "dumb collector",
  minimal parsing at the collection layer.
- Redis Streams as the broker (`redis.asyncio`, consumer groups). Broker and cache
  (when the cache layer lands) must live in **separate logical Redis DBs**
  (different eviction policy: `noeviction` for broker, `allkeys-lru` for cache) —
  never share one DB for both.
- ClickHouse via `clickhouse-connect` (async since 0.12.0) for storage.
- FastAPI serves REST + MCP from one process (`src/api/`), read-only over ClickHouse.
- Docker Compose for local dev; systemd (`Restart=always`) for process supervision
  in production — no Supervisor/k8s at this stage.

## Repo layout

- `src/collector/` — exchange WS/REST → Redis Streams (raw, minimal parsing).
- `src/exchanges/<name>/` — per-exchange adapters. New exchange = new subpackage;
  see `docs/04-architecture/exchanges/README.md` for the convention before adding one.
- `src/normalizer/` — Redis Streams → normalized batch inserts into ClickHouse.
- `src/api/` — REST + MCP serving layer, read-only, ClickHouse only (never touches
  the broker).
- `src/common/` — shared utilities (logging, etc.).
- `infra/clickhouse/migrations/*.sql` — schema, auto-applied on first startup
  against an empty data volume only. New migration = new numbered file, never
  edit an already-applied one.
- `infra/monitoring/` — Prometheus/Grafana/Loki provisioning.
- `docs/` — the actual design record (Document layer: idea, data model, MVP scope,
  architecture, risks, roadmap, hardening plan). Read the relevant doc before
  changing behavior it documents; update it when behavior changes.

## Hard rules

- **Every record gets both `ts_exchange` and `ts_received`.** No exceptions —
  this is one of the three gaps the whole product exists to close.
- **`api` is read-only.** It must never write to Redis or ClickHouse, and must
  never depend on the broker being up — only on ClickHouse.
- **Collector stays dumb.** Parsing/normalization logic belongs in `normalizer`,
  not `collector` — don't move logic upstream to "simplify" something.
- **New exchange or new data type**: update the relevant doc under
  `docs/04-architecture/exchanges/` or `docs/03-mvp-scope.md` in the same change,
  not as a follow-up.
- MCP tool set is fixed by `docs/03-mvp-scope.md` (`get_trades`, `get_orderbook_at`,
  `get_orderbook_events`, `get_derivatives_metrics`, `list_data_incidents`) —
  adding/removing a tool is a scope decision, flag it rather than doing it silently.

## Common commands

```bash
cp .env.example .env                     # first-time setup
docker compose up --build                # run the full local stack
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM market_data.trades"
docker compose down                      # stop
docker compose down -v                   # stop + wipe all volumes (fresh test run)
```

Details, service list, REST/MCP examples, Grafana/Loki/Prometheus access:
`docs/07-local-dev.md`.

## Before marking anything "done"

Check `docs/08-prototype-roadmap.md` (gap vs MVP) and `docs/09-hardening-tests-load-auth.md`
(known weak points: tests, load testing, authorization) — don't claim scope that
those docs list as open.

## Agents — routing

Five subagents live in `.claude/agents/`. Route by what the task touches, not by
who asks — most changes span one directory and one agent:

| Task touches | Agent | Model |
|---|---|---|
| `src/collector/` (supervision, broker sink, reconnect/backoff) | `collector-agent` | default |
| `src/exchanges/<name>/` (new exchange, protocol quirks) | `exchange-adapter` | haiku |
| `src/normalizer/`, `infra/clickhouse/migrations/` (parsing, schema, incidents) | `normalizer-agent` | default |
| `src/api/` (REST/MCP serving) | `api-agent` | default |
| Anything, before calling it "done" | `verify-gate` | haiku |

Rules:
- **A task spanning multiple layers is multiple agent calls, not one agent doing
  everything.** E.g. "add Bybit spot" = `exchange-adapter` (new adapter + doc)
  first, then `normalizer-agent` only if the normalized schema actually needs to
  change for Bybit's fields — don't let one agent touch another's directory.
- **`verify-gate` runs last, always**, after the implementing agent(s) finish and
  before you report the change as complete — it's read-only and cheap (haiku), so
  there's no reason to skip it.
- Model choice: `exchange-adapter` and `verify-gate` are pattern-matching /
  checklist work against an existing convention — haiku is enough. `collector-agent`,
  `normalizer-agent`, `api-agent` involve schema/correctness judgment on financial
  data — keep them on the default (stronger) model.
- If a task doesn't clearly belong to one of the five (e.g. `docker-compose.yml`,
  `infra/monitoring/`, cross-cutting docs), handle it directly rather than forcing
  it into one of these roles.

## Context hygiene

- **Switching agent/domain (e.g. `exchange-adapter` → `api-agent`, or moving to an
  unrelated task) → `/clear`.** Don't carry collector-layer debugging context into
  an api-layer task — it's pure noise for that agent.
- **Long single-domain session (e.g. extended normalizer debugging, multi-step
  migration work) → `/compact` before it gets unwieldy**, not after the window is
  already full of dead ends and retries.
- Don't `/compact` mid-way through an unresolved multi-step edit — compact after
  a task is verified (post `verify-gate`), not before, so you don't collapse
  half-finished reasoning into the summary.

## Memory layer

`memory/` holds short, current, working facts — not a second copy of `docs/`.
Full rules: `memory/README.md`. The short version: a fact goes in `memory/` only
if it isn't already in `docs/`; it's dated and sourced; conflicts get superseded,
not deleted; once a fact stabilizes into `docs/`, remove it from memory rather
than keeping it in both places.

Per-domain files match the agents above: `memory/collector.md`,
`memory/normalizer.md`, `memory/api.md`, `memory/exchanges.md`, and
`memory/general.md` for anything cross-cutting. Each agent should check its own
memory file before starting work (fresher than docs for operational state) and
add an entry when it learns something durable that docs don't already say.
