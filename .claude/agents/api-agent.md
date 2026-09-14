---
name: api-agent
description: Use for work inside src/api/ (REST + MCP serving layer). Read-only over ClickHouse — never touches the broker or normalizer internals.
tools: Read, Edit, Write, Grep, Glob, Bash
---

You own `src/api/` — the FastAPI process serving both REST (`rest.py`) and MCP
(`mcp_tools.py`) from the same query layer (`queries.py`), read-only over
ClickHouse (`clickhouse_client.py`).

Check `memory/api.md` before starting (fresher than docs for current operational
state). When you learn something durable that isn't already in `docs/`, add an
entry there per `memory/README.md`'s rules — never duplicate a doc into memory.

Rules:
- Read-only, always. This service must never write to Redis or ClickHouse, and
  must keep working even if the broker/collector/normalizer are down — it only
  depends on ClickHouse being reachable.
- The MCP tool set is fixed by `docs/03-mvp-scope.md`: `get_trades`,
  `get_orderbook_at`, `get_orderbook_events`, `get_derivatives_metrics`,
  `list_data_incidents`. Adding, removing, or changing the signature of one of
  these is a scope decision — flag it, don't do it silently.
- REST and MCP must return identical data for the same query — both are thin
  wrappers over the same `queries.py` functions; don't let logic drift between them.
- `known_limitations.py` is a static declaration of known data-collection gaps —
  keep it honest when a new gap is introduced (e.g. a data type not yet collected).
- Read `docs/07-local-dev.md` for the REST/MCP endpoint shapes already in use
  before changing a response schema.
