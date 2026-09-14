# Prototype → MVP Roadmap

The running stack ([docs/07-local-dev.md](./07-local-dev.md)) is a deliberately
cut-down slice: **Binance spot, BTCUSDT only**, trades + L2 order book, no
funding/OI/liquidations, no MCP-facing derivatives tool, single host,
no auth, no tests. This document lays out what's still needed to go from that
slice to the MVP as scoped in [03-mvp-scope.md](./03-mvp-scope.md) and
[Binance map](./04-architecture/exchanges/binance.md), organized as buildable
phases with an explicit gap table and the decisions each phase needs before
work starts.

## 1. What the prototype already proves

- Full pipeline works end to end: collector (WS) → Redis Streams → normalizer
  → ClickHouse → REST + MCP serving, with Loki/Prometheus/Grafana/Traefik
  wired in.
- Order-book bootstrap + reconciliation, sequence-break detection, periodic
  snapshotting, point-in-time reconstruction (`get_orderbook_at`) all work
  against a real exchange feed.
- Incidents schema is implemented for the two types reachable from a spot-only
  feed (`orderbook_sequence_break`, `collector_disconnect`).
- 4 of the 5 MVP MCP tools exist (`get_trades`, `get_orderbook_at`,
  `get_orderbook_events`, `list_data_incidents`), sharing query code with REST
  so the two surfaces can't drift.

## 2. Gap vs. MVP scope

| Area | Prototype now | MVP target | Gap |
|---|---|---|---|
| Instruments | 1 symbol (BTCUSDT spot) | 8 spot + 13 USDT-M perp + 2 COIN-M perp (~23 symbols), see [Binance map](./04-architecture/exchanges/binance.md) | Collector must run many symbols/streams per segment, not one |
| Segments | Spot only | Spot + USDT-M perp + COIN-M perp | 2 new exchange adapters, 2 new WS connection schemes (`/public`+`/market` vs `dstream`) |
| Derivatives data | **Implemented for USDT-M perp** (Phase B, see below): `BinanceUsdtmPerpAdapter` (aggTrade/depth/markPrice/forceOrder + OI REST poller), migration `005_derivatives.sql` (`mark_price`, `open_interest`, `liquidations` tables), normalizer parsing + `reference_price_freeze` incident detection (best-effort heuristic, not empirically validated -- see [Next Steps](./06-next-steps.md) item 1). COIN-M perp still has none of this (Phase C). | Funding, mark/index price, OI, liquidations (perp only) | COIN-M perp adapter/schema reuse (Phase C); multi-symbol collection (Phase A still not done -- one symbol per collector instance) |
| `get_derivatives_metrics` | **Implemented**: `GET /derivatives` + MCP tool, querying `mark_price`/`open_interest`/`liquidations` independently and combining client-side, per `queries.py` | Required MVP tool | None -- closed for USDT-M perp; will need re-verification once COIN-M perp lands (Phase C) if that segment's schema needs match this one's |
| Incidents coverage | `orderbook_sequence_break`, `collector_disconnect` | + `reference_price_freeze` (XAUUSDT/XAGUSDT), + `backfill_gap` | Needs markPrice stream + a freeze detector; needs a defined "confirmed unrecoverable gap" trigger |
| `known-limitations` | Empty (nothing to declare yet) | `liquidation_partial_coverage` (forceOrder) populated | Trivial once liquidations are collected — static config entry |
| Access | Local `docker compose up`, `*.localhost` via Traefik | Hosted REST/WS API reachable by 5-10 external users | Needs a real host, TLS, DNS, and some form of per-user access control (currently none). Concrete auth plan (API-key recommendation): [ADR-001](./09-hardening-tests-load-auth.md#3-authorization). |
| Historical backfill | None (live collection only) | Not required for MVP per se, but `data.binance.vision` dumps are the only way to backfill trades/klines before collector start | Decide whether MVP ships with zero pre-launch history or a one-time backfill job |
| Multi-region / redundancy | Single collector instance, single host | Deferred, accepted risk (see [Risks](./05-risks.md) item 2) | No action needed for MVP — re-confirm this is still acceptable once real users depend on uptime |
| Tests | None found in the repo | Not explicitly scoped, but sequence-break/reconciliation logic and incidents field-shape are exactly the kind of thing that silently breaks | Worth at least unit tests on `orderbook_state.py`, `parser.py`, `incidents.py` before adding 3x the surface area. Concrete plan: [ADR-001](./09-hardening-tests-load-auth.md#1-tests). |
| Clock/NTP monitoring | Not implemented | Needed for dual-timestamp claim to be trustworthy (see [Risks](./05-risks.md) item 3) | Add an NTP-drift check + `clock_drift` incident emission |
| Storage/egress estimate | Not done | Needed before retention/tier decisions | GB/day-per-instrument-per-exchange estimate ([Next Steps](./06-next-steps.md) item 2) |
| Legal | Risk documented, MVP proceeds at 5-10 user scale ([Risks](./05-risks.md) items 1, 7) | Written permission required before monetization/scaling | No blocker for prototype work; blocker for anything beyond validation |

## 3. Suggested build phases

**Phase A — multi-symbol spot.** Extend the collector to hold one combined WS
connection per (exchange, segment) across all 8 spot symbols instead of one
symbol per process (per the "≤80 streams, one connection per segment"
decision in the Binance map). Proves the multi-symbol collector/normalizer
path without yet touching a new WS protocol. Config-driven symbol list
(`.env` `SYMBOLS=...` instead of singular `SYMBOL=`) is the natural extension
of the current `EXCHANGE`/`SEGMENT`/`SYMBOL` pattern.

**Phase B — USDT-M perp. STATUS: implemented (see repo history / `memory/*.md` for the session trail).** New adapter against the `/public`+`/market`+`/private`
scheme: trades, L2 order book (same bootstrap/reconciliation logic, `pu`-based
continuity instead of spot's `U`/`u`), `markPrice@1s` WS, `forceOrder` WS,
and the one REST poller in the whole pipeline (`openInterest`, since it has no
WS stream). New ClickHouse table(s) for funding/mark-price/OI/liquidations
(one migration, `005_derivatives.sql`). Wire `get_derivatives_metrics` in
`queries.py`/`rest.py`/`mcp_tools.py` — the docstring in `mcp_tools.py`
already names it as the one deliberately-deferred tool.

**Phase C — COIN-M perp.** Second, different WS scheme (`dstream`, single
`/stream` endpoint, no `/public`/`/market`/`/private` split). Reuses the
Phase B derivatives schema/tooling — this is mostly "another adapter", not new
serving logic.

**Phase D — incidents completeness.** `reference_price_freeze` detector for
XAUUSDT/XAGUSDT (compare consecutive `markPrice` values against a
trading-hours calendar or a simple "unchanged for N seconds outside a known
active window" heuristic); populate `known-limitations` with
`liquidation_partial_coverage` once `forceOrder` is actually collected;
decide the concrete trigger for `backfill_gap` (e.g. collector downtime
longer than the snapshot interval).

**Phase E — hosted access.** Move off `*.localhost`/Traefik-dev to a real
host + domain + TLS. Decide the minimum viable access control for 5-10 named
users (API keys are the obvious minimum — there's currently no auth layer at
all, REST and MCP are wide open). Decide whether WS streaming (mentioned as
part of "Access method" in the MVP table) ships in v1 or REST+MCP is enough
to validate the point-in-time/incidents value prop first.

**Phase F — hardening.** NTP-drift monitoring + `clock_drift` incidents; a
storage/egress estimate now that Phase A-C give real per-symbol volume
numbers; enough unit tests on the reconciliation/incidents logic to catch
regressions as segments multiply; revisit the accepted no-redundancy risk
once real users are depending on the feed.

## 4. Decisions to make before starting (not yet resolved anywhere else)

1. **Backfill or not for launch** — ship with history starting at "whenever
   the collector for that symbol first ran", or invest in a one-time
   `data.binance.vision` import job first? Affects how compelling the
   point-in-time demo is to the first 5-10 users.
2. **Auth mechanism** — API key issued manually to each of the 5-10 target
   users is the minimum; decide now so `rest.py`/`mcp_tools.py` aren't
   retrofitted later. See [ADR-001](./09-hardening-tests-load-auth.md#3-authorization)
   for the options considered and the concrete recommendation.
3. **Host** — where does this actually run (existing MVP server referenced in
   [01-stack.md](./04-architecture/01-stack.md), a cloud VM, etc.) and does it
   have the resources for ~23 symbols × 3 segments' worth of WS connections +
   ClickHouse + the monitoring stack?
4. **Phase order** — this doc assumes A→B→C→D→E→F, but B (derivatives) is
   arguably the highest-value gap since it unlocks the one missing MCP tool;
   worth confirming that priority against the "5-10 users actually using
   point-in-time + MCP" success criterion in
   [03-mvp-scope.md](./03-mvp-scope.md) before committing to the order.
