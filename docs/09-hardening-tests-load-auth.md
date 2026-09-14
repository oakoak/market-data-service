# ADR-001: Hardening the Prototype — Tests, Load Testing, Authorization

**Status:** Proposed (tests/load-testing/authorization sub-specs now implementation-ready — see companion docs)
**Date:** 2026-09-13 (updated 2026-09-14)
**Deciders:** Project owner

## Context

The running prototype ([docs/07-local-dev.md](./07-local-dev.md)) proves the
collector → Redis Streams → normalizer → ClickHouse → REST/MCP pipeline
end-to-end for one instrument (Binance spot BTCUSDT). Its own gap table
([docs/08-prototype-roadmap.md](./08-prototype-roadmap.md), §2) already flags
two of the three items below as open ("Tests: None found in the repo",
"Access: currently none"); load testing wasn't previously written down
anywhere. This ADR records architecture weak points found while reviewing
the code, and turns "add tests / load tests / authorization" into concrete
plans. No code is changed by this document or its companions — action items
are for a follow-up implementation pass.

**2026-09-14 update:** each of the three plans below was pushed from "here's
an approach" to "here's the implementation-ready spec" — enumerated test
cases with source-line citations, computed throughput numbers with a
concrete `k6` script and lag-simulation harness, and complete auth code
(middleware, storage, rollout plan). Full detail lives in three companion
documents so this ADR stays readable as the index/decision record:

- [docs/10-test-plan.md](./10-test-plan.md) — 140 enumerated test cases across 7 modules, proposed directory tree, 5 concrete test-blockers with minimal fixes.
- [docs/11-load-testing-plan.md](./11-load-testing-plan.md) — computed time-to-data-loss numbers, a runnable k6 script, and a Redis-lag simulation harness (bash + Python).
- [docs/12-authorization-spec.md](./12-authorization-spec.md) — complete API-key design: storage, hashing, FastAPI middleware code, revocation, rate limiting, staged rollout.

See also [ADR-002: Architecture Validation](./13-architecture-validation.md) — a separate review of whether the *documented architecture decisions themselves* (storage engine choices, process supervision, collector redundancy trade-off, stream keying) are sound, as opposed to this ADR's focus on implementation gaps.

## Architecture weak points found

The codebase is unusually good about calling out its own accepted trade-offs
in comments (e.g. `normalizer/consumer.py`'s at-most-once-on-crash note,
`05-risks.md`'s collector-redundancy deferral). The list below focuses on
what is **not** already written down elsewhere, plus turning the two known
gaps (tests, auth) into decisions.

1. **Redis Stream `maxlen` trim can silently destroy unrecoverable history.**
   `src/collector/sink.py::publish()` calls `XADD ... maxlen=<REDIS_STREAM_MAXLEN>
   approximate=True` on every message, regardless of whether the normalizer
   has consumed it yet. Per [00-overview.md §6.1](./04-architecture/00-overview.md#61-data-history--key-constraint),
   "our history depth = the age of our collection infrastructure" — an
   extended normalizer outage (crash loop, ClickHouse unreachable) at real
   message rates can trim depth-diff entries the normalizer never got to
   read, permanently destroying data the architecture treats as
   fundamentally irreplaceable. **Quantified in
   [docs/11-load-testing-plan.md](./11-load-testing-plan.md):** at the
   current default (`REDIS_STREAM_MAXLEN=1,000,000`), the system has
   somewhere between **~3.7 hours (burst trade rate) and ~28 hours
   (baseline depth cadence)** of normalizer downtime before the first
   stream starts silently trimming — comfortably within an unattended
   overnight/weekend outage window given weak point #5 below. Concrete fix
   proposed there: raise maxlen to 5,000,000 and add a lag-based alert at
   two thresholds. **Severity: high** — a silent violation of the project's
   core stated guarantee.
2. **Redelivery can produce duplicate rows, not just the documented loss
   window.** `src/normalizer/consumer.py`'s docstring calls out an
   at-most-once loss window (crash between XACK and ClickHouse flush) but
   not its mirror image: on crash *before* XACK, PEL redelivery is correct
   and expected — but `infra/clickhouse/migrations/001-004` define plain
   `MergeTree` tables with no `ReplacingMergeTree` / insert-dedup token, so a
   redelivered message becomes a genuine duplicate row on the next flush.
   **Severity: medium** — skews counts and can double-count rows returned by
   `get_trades` / `get_orderbook_events` after any normalizer restart.
   ClickHouse's native `insert_deduplication_token` (via
   `clickhouse-connect`) is a direct fit and isn't used anywhere in
   `clickhouse_sink.py`. Now pinned as a reproducible regression test in
   [docs/10-test-plan.md §8](./10-test-plan.md) (case 132).
3. **A third, previously-undocumented data-loss path: failed ClickHouse
   inserts always lose the batch, never retry.** Found while enumerating
   tests for `clickhouse_sink.py` ([docs/10-test-plan.md §5](./10-test-plan.md),
   case 94/98): `_flush_table` pops the in-memory buffer *before* calling
   `.insert()`. On insert failure the exception is re-raised, but the batch
   is already gone from the buffer — silently swallowed by
   `_periodic_flush`'s broad `except Exception` (batch permanently lost) or
   propagated to `consumer.py`'s `_process_one`, which catches it, logs, and
   **XACKs anyway** (also lost). This is distinct from both #1 (maxlen trim)
   and #2 (redelivery duplication) and was not previously named anywhere.
   Worse, `flush_all`'s per-table loop has no per-table try/except, so a
   ClickHouse hiccup on the `trades` table (first in iteration order) can
   currently block that tick's `orderbook_events`/`orderbook_snapshots`/
   `incidents` flushes too. **Severity: medium-high** — a transient
   ClickHouse blip (a restart, a brief disk-pressure pause) can lose data
   with zero indication beyond a log line.
4. **No TTL / cold-tier implementation despite the docs claiming one.**
   [00-overview.md §6.5](./04-architecture/00-overview.md#65-storage) states
   "ClickHouse supports TTL-based part migration to S3 out of the box" —
   `grep TTL infra/clickhouse/migrations/*.sql` returns nothing. The
   hot/cold tiering strategy is 0% implemented, not just "not yet needed at
   this scale." **Severity:** low for the 1-day local prototype, high once
   real retention starts — today, losing the `clickhouse-data` volume loses
   the entire unrecoverable order-book archive, with no S3 copy anywhere.
5. **Single ClickHouse instance, no replication or backup strategy.** One
   container, one named volume (`docker-compose.yml`). Combined with #4,
   there is currently no recovery path from disk/volume loss.
6. **No health signal for `collector`/`normalizer`.** `docker-compose.yml`
   only defines `healthcheck:` blocks for `redis` and `clickhouse`; `api`,
   `collector`, and `normalizer` rely solely on `restart: unless-stopped`,
   which restarts a crashed process but gives no signal for a
   running-but-stuck collector (e.g. a WS socket that silently stops
   emitting without closing) — exactly the failure class §6.1's "collector
   redundancy" reasoning is built around, and exactly the failure mode that
   makes the #1 downtime windows above plausible in practice.
   **Severity: medium.**
7. **No rate limiting / no auth = fully open read-amplification surface.**
   `/trades` and `/orderbook/events` accept `limit` up to 10,000 with no
   per-caller throttling and (see #8) no identity check at all.
   [docs/12-authorization-spec.md §6](./12-authorization-spec.md) also found
   the **MCP tool layer has no upper bound on `limit` at all** (unlike
   REST's `le=10_000`), making this surface strictly worse via MCP than via
   REST today.
8. **No authentication/authorization anywhere in `src/api`.** Confirmed by
   reading `rest.py` / `mcp_tools.py` / `main.py` end to end: every REST
   route and every MCP tool is reachable by anyone who can reach the `api`
   container/Traefik route. Fully specified (storage, hashing, middleware,
   revocation, rate limiting, rollout) in
   [docs/12-authorization-spec.md](./12-authorization-spec.md).
9. **Secrets default to well-known values.** `GRAFANA_ADMIN_PASSWORD`
   defaults to `admin` in both `.env.example` and `docker-compose.yml`'s
   fallback; `CLICKHOUSE_PASSWORD` defaults to an empty string with
   `CLICKHOUSE_USER=default`. Fine for a localhost-only prototype; there is
   no mechanism yet (secrets file, vault, or even a README warning) to stop
   these from being carried as-is into a hosted deployment.
10. **No automated tests anywhere.** `grep -rn "def test_" src/` and
    `requirements.txt` both come back empty — no pytest/unittest dependency,
    no test files. Fully specified in
    [docs/10-test-plan.md](./10-test-plan.md), including 5 concrete
    test-blocking issues (no `src/`-layout packaging config, no
    dependency-injection points for the Redis/ClickHouse client wrappers)
    with minimal, described fixes.
11. **No load/perf testing, and not previously written down anywhere.** The
    closest existing item was [05-risks.md #5](./05-risks.md) ("storage
    growth and egress cost estimate"), which is about cost, not
    throughput/latency behavior under load. Fully specified in
    [docs/11-load-testing-plan.md](./11-load-testing-plan.md).

## Decisions

### 1. Tests — see [docs/10-test-plan.md](./10-test-plan.md)

**Decision: adopt the full plan** — `pytest` + `pytest-asyncio` +
`testcontainers-python` (Redis + ClickHouse, pinned to the same
`clickhouse/clickhouse-server:24.8` image already in
[01-stack.md](./04-architecture/01-stack.md)), organized as
`tests/{unit,integration,contract,regression}/`. 140 enumerated test cases
across `orderbook_state.py`, `parser.py`, `incidents.py`, `consumer.py`,
`clickhouse_sink.py`, `queries.py`'s pure-logic functions, and the Binance
collector adapter, plus a dedicated REST/MCP contract-parity suite and a
schema-drift regression guard for the incidents table. Two real,
currently-existing REST/MCP asymmetries were found while designing the
contract suite (MCP's `get_trades` has no `limit` upper bound; MCP tools
don't wrap `queries.parse_ts` in a try/except the way `rest.py` does, so an
invalid timestamp raises unhandled inside a tool call instead of a clean
error) — both are written up as tests that stay red until fixed, not
silently normalized away.

**Before writing tests, two blockers need a minimal, non-invasive fix**
(both described in detail in the companion doc): (a) no `pyproject.toml`/
`pythonpath` config for the `src/`-layout imports to resolve outside Docker,
and (b) `ClickHouseSink`/`ClickHouseReadClient`/`RedisStreamSink` build their
own clients internally rather than accepting one via the constructor, which
should get an optional injection parameter so unit tests can pass a fake
client instead of monkeypatching module-level functions.

### 2. Load testing — see [docs/11-load-testing-plan.md](./11-load-testing-plan.md)

**Decision: adopt the computed numbers and scripts as the acceptance
baseline**, not as a "run it and see" open question:

- Raise `REDIS_STREAM_MAXLEN` from 1,000,000 to 5,000,000 (stretches the
  worst-case downtime-before-loss window from ~18.5h to ~3.9 days), paired
  with a `maxmemory` cap on the `redis` service (currently unset) so the 5x
  increase doesn't turn into unbounded memory growth.
- Add a consumer-lag alert on Redis 7's native `XINFO GROUPS` `lag` field:
  WARNING at 1,000,000 (20% of new maxlen), CRITICAL/page at 2,500,000
  (50%) — this is the concrete trigger for a new `consumer_lag` incident
  type, distinct from `backfill_gap`.
- REST API load target: 20 req/s sustained, 50 req/s / 30s burst, with
  per-endpoint p50/p99 latency thresholds already encoded in the provided
  `k6` script (`/orderbook/at` given a looser threshold than the other
  three endpoints, since it does snapshot+replay reconstruction rather than
  a flat row scan).
- Redis-lag ground-truth measurement: run the provided `lag_sim.sh` against
  the real stack (stop `normalizer`, watch `XLEN`/`XINFO GROUPS` until the
  trim threshold hits) for a real number, and/or the accelerated
  `synthetic_producer.py` variant to get the same answer in minutes instead
  of real hours — both reference the actual service/stream names from
  `docker-compose.yml`/`config.py`, not placeholders.

All numeric targets in the companion doc are explicitly labeled as either a
fact taken from this repo's docs/code or a stated assumption (trade
frequency, researcher query rate) — treat the assumption-derived numbers as
the current best estimate to build against, and replace them with measured
values once the collector runs against real exchange traffic at fuller
scale.

### 3. Authorization — see [docs/12-authorization-spec.md](./12-authorization-spec.md)

**Decision: per-user API keys, stored in a static `src/api/api_keys.py`
registry (not ClickHouse — see the spec's §1 for why), enforced by one
app-level Starlette middleware that covers both the REST router and the MCP
mount** (a plain FastAPI `Depends()` structurally cannot reach the MCP
mount — `mcp_asgi_app` is a separate Starlette app per `main.py`'s own
docstring, so only middleware sitting in front of `app`'s whole ASGI
dispatch covers both). Concretely specified: SHA-256 key hashing (justified
against bcrypt/argon2 — wrong tool for a high-entropy token, not a
low-entropy password), a `mds_live_<43 chars>` key format, a one-off
`scripts/issue_api_key.py` issuance script (no existing admin tooling in the
repo to build on), soft-revocation via `revoked_at` (never hard-delete, to
keep historical logs/rate-limit counters attributable — consistent with why
`incidents.py` already avoids in-place mutation), a Redis fixed-window
rate limiter (120 req/min default, reusing the Redis instance already in
the stack, a separate logical DB from the Streams broker), and a 5-stage
non-breaking rollout (`off` → `log_only` → issue keys → `enforce` → drop the
mode flag).

## Consequences

- **Easier:** confidence that a normalizer restart doesn't silently
  duplicate or drop data (once #2/#3 are also addressed); confidence that
  REST and MCP genuinely can't drift (contract tests, with the two existing
  asymmetries now visible instead of hidden); a measured answer to "how
  long can the normalizer be down" instead of an assumption, with a
  concrete mitigation (raise maxlen + lag alerting); a fully specified,
  implementation-ready auth mechanism instead of an open-ended "add auth
  later" item.
- **Harder:** CI now needs Redis + ClickHouse (via testcontainers or a
  docker-compose CI service) rather than being pure unit tests — slower CI,
  worth it given where the real bugs live (integration-level, not
  unit-level, per docs/10 case 131-135). The auth middleware adds an
  app-level `BaseHTTPMiddleware` whose interaction with FastMCP's
  streaming responses needs a smoke test before relying on it (flagged
  explicitly in docs/12 §3).
- **To revisit:** the maxlen/redelivery/insert-failure findings (#1, #2, #3)
  are architecture gaps, not just missing tests — the load-testing plan is
  designed to validate the maxlen fix's headroom empirically once
  implemented, and #3 (silent insert-failure data loss) has no proposed fix
  yet beyond "now documented and pinned by a test" — worth a dedicated
  follow-up decision (retry-with-backoff vs. dead-letter buffer vs. accept
  as a known risk the way collector redundancy was accepted in
  [05-risks.md item 2](./05-risks.md)).

## Action Items

1. [ ] Fix the two test-infra blockers (`pyproject.toml`/`pythonpath` config;
   optional client-injection params on `ClickHouseSink`/`ClickHouseReadClient`/
   `RedisStreamSink`) — see [docs/10-test-plan.md §11](./10-test-plan.md).
2. [ ] Implement the `tests/{unit,integration,contract,regression}/` suite
   per [docs/10-test-plan.md](./10-test-plan.md)'s 140 enumerated cases.
3. [ ] Fix the two real REST/MCP asymmetries the contract-test design
   surfaced: bound MCP's `get_trades` `limit` the same way REST does; wrap
   `mcp_tools.py`'s `queries.parse_ts` calls in the same try/except pattern
   `rest.py::_parse_ts` uses.
4. [ ] Raise `REDIS_STREAM_MAXLEN` to 5,000,000 and add a `redis` service
   `maxmemory` cap; implement the `consumer_lag` alert/incident at the two
   thresholds in [docs/11-load-testing-plan.md](./11-load-testing-plan.md).
5. [ ] Run `lag_sim.sh` (or the accelerated synthetic producer) against the
   real stack to replace the assumption-derived downtime numbers with
   measured ones.
6. [ ] Run the provided `k6` script against the REST API; build an
   equivalent async client harness for the MCP tools (no off-the-shelf tool
   speaks MCP natively).
7. [ ] Implement the authorization spec end to end per
   [docs/12-authorization-spec.md](./12-authorization-spec.md):
   `api_keys.py`, `auth.py` middleware, `rate_limit.py`, `scripts/issue_api_key.py`,
   the `main.py` wiring, and the staged `API_AUTH_MODE` rollout.
8. [ ] Decide a fix for weak point #3 (silent ClickHouse insert-failure data
   loss) — retry-with-backoff, a dead-letter buffer, or an explicit
   accept-as-risk decision recorded in [05-risks.md](./05-risks.md).
9. [ ] Re-evaluate weak points #4-#6 (TTL/cold tier, ClickHouse redundancy,
   health checks) once the above give real numbers — decide fix vs.
   accept-and-document for each, the same way existing risks in
   [05-risks.md](./05-risks.md) are handled.
