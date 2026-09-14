# ADR-002: Architecture Validation — Are the Documented Decisions Sound?

**Status:** Proposed
**Date:** 2026-09-14
**Deciders:** Project owner

## Context

[ADR-001](./09-hardening-tests-load-auth.md) and its companions audited the
*implementation* — bugs, missing tests, missing auth, unquantified load
risk. This ADR asks a different question: taking the architecture exactly
as documented ([00-overview.md](./04-architecture/00-overview.md),
[01-stack.md](./04-architecture/01-stack.md), the
[Binance](./04-architecture/exchanges/binance.md) and
[Bybit](./04-architecture/exchanges/bybit.md) maps), are the decisions
themselves sound for the stated goals — 5-10 users, point-in-time
reconstruction as the headline feature, an unrecoverable-history guarantee,
and a growth path to more symbols/exchanges? This is a design review, not a
bug hunt; several items below overlap with ADR-001's weak points but are
addressed here as architecture-*decision* questions (should the decision
change) rather than implementation gaps (what's missing).

## Decision

Adopt the two concrete architectural changes proposed below (ReplacingMergeTree
for append-as-new-row tables; a documented redundancy-revisit trigger), fix
the one found documentation/implementation drift (process supervision), and
treat the remaining items as confirmed-sound with a noted caveat.

## Findings

### 1. Storage engine choice: plain `MergeTree` is the wrong engine for `incidents`, and switching it also fixes an ADR-001 weak point

**Current decision** ([00-overview.md §6.5](./04-architecture/00-overview.md#65-storage), [004_incidents.sql](./04-architecture/00-overview.md)): all four tables use plain `MergeTree`. `incidents.py`'s own docstring already documents the consequence for the `incidents` table specifically: resolving an incident inserts a *new* row with the same `id` rather than updating in place, "because ClickHouse's MergeTree doesn't do in-place updates," and callers must "query the latest row per incident id."

**Validation:** the *reasoning* (MergeTree can't update cheaply) is correct, but the *conclusion* (accept append-and-query-latest as a permanent pattern) leaves a real correctness gap: nothing in `queries.py`'s `list_incidents` was found to actually implement "latest row per id" deduplication — worth confirming during the [test-plan](./10-test-plan.md) implementation pass whether `list_incidents` already does this or would currently return both the open and resolved row for a single incident.

**Better decision:** `ReplacingMergeTree(ingested_at)` (or a dedicated version column) for the `incidents` table, ordered the same way (`exchange, start_ts, id`). This is a one-line engine change (`ENGINE = ReplacingMergeTree(ingested_at)` instead of `ENGINE = MergeTree`) that makes "latest row per id" ClickHouse's job (`FINAL` modifier or `argMax`-style query, not a memory-side dedup the API layer must remember to do) — and it composes cleanly with existing querying patterns without changing `incidents.py`'s open/resolve dict-producing functions at all.

**This same fix also closes ADR-001 weak point #2** (redelivery-produced duplicate rows in `trades`/`orderbook_events`): those tables could adopt `ReplacingMergeTree` with a natural dedup key (`(exchange, symbol, ts_exchange, trade_id)` for trades already **is** the `ORDER BY` — a redelivered trade has an identical dedup key and would collapse to one row on merge) instead of, or alongside, the `insert_deduplication_token` approach ADR-001 suggested. **Trade-off to weigh, not a free win:** `ReplacingMergeTree` only deduplicates during background merges (not synchronously — a `SELECT` right after insert can still see both rows until a merge runs, or until `FINAL`/`argMax` is used at query time), so this doesn't replace `insert_deduplication_token` for the trades/events tables if synchronous correctness matters more than eventual — but for `incidents` specifically, where the query pattern was already going to be "latest row per id," `ReplacingMergeTree` is a strict improvement with no such downside, since incident resolution already tolerates the eventual-consistency window (the `open`→`resolved` transition isn't read on a hot path with a synchronicity requirement).

**Recommendation:** switch `incidents` to `ReplacingMergeTree` now (low risk, clear win); leave `trades`/`orderbook_events` on `insert_deduplication_token` as ADR-001 already proposed (better fit given the synchronous-read caveat above), and note this distinction explicitly so a future implementer doesn't apply the same engine change to all four tables uniformly.

### 2. Process supervision: the documented decision was silently superseded by the implementation

**Documented decision** ([01-stack.md](./04-architecture/01-stack.md)): "a systemd unit with `Restart=always` — the simplest option for a single server, without extra tooling (**Supervisor/Docker orchestration are overkill at this stage**)." `src/collector/runner.py`'s own docstring references "docs/04-architecture/01-stack.md's systemd Restart=always note" as the layer that covers process death.

**What's actually built:** `docker-compose.yml` runs every service (`collector`, `normalizer`, `api`, and everything else) under Docker Compose with `restart: unless-stopped` — i.e., the project ended up using exactly the "Docker orchestration" the stack doc calls "overkill" for this stage, and there is no systemd unit anywhere in the repo (confirmed: no `.service` file exists).

**This isn't wrong** — Docker Compose's restart policy does the same job `01-stack.md` wanted from systemd, arguably more consistently (it's the same supervision mechanism for all 10 services, not a special case for collector/normalizer only). But it's an **undocumented decision reversal**: a future reader of `01-stack.md` gets an inaccurate mental model of how the deployed system is actually supervised, and `runner.py`'s docstring points at a document describing infrastructure that doesn't exist. Low severity, cheap fix.

**Recommendation:** update `01-stack.md`'s "Process supervision" bullet to describe the actual Docker Compose `restart: unless-stopped` mechanism (and drop or revise the "Docker orchestration are overkill" framing, since the project uses it for everything else already), and update `runner.py`'s docstring reference accordingly. Documentation-only fix — flagged here rather than applied, per this session's no-code-changes scope, but small enough to bundle into whichever change touches `01-stack.md` next.

### 3. The core "unrecoverable history" guarantee and the "no collector redundancy" decision are in tension, and the revisit trigger is too vague to act on

**Documented decisions**, both already explicit: [00-overview.md §6.1](./04-architecture/00-overview.md#61-data-history--key-constraint) states collector redundancy is needed "from day one, not as a 'future' task" because a collector failure is "an unrecoverable hole in history, forever." [05-risks.md item 2](./05-risks.md) then accepts the opposite for the MVP — zero redundancy — reasoning that a second collector on the *same server* wouldn't add real fault tolerance anyway (shared machine = shared failure point), and says to "revisit when moving to separate infrastructure."

**Validation:** the risk item's own reasoning is sound *for a single-server deployment* — it correctly identifies that duplicating a collector process without duplicating the machine is theater, not redundancy. The tension isn't a logic error; it's that the stated revisit trigger ("when moving to separate infrastructure") is an infrastructure milestone, not a data-risk milestone, and the two don't necessarily coincide. [08-prototype-roadmap.md Phase E](./08-prototype-roadmap.md) (hosted access) is exactly the point where real, paying-attention users start depending on continuous history for the point-in-time feature to mean anything — that's the moment the "unrecoverable forever" language in §6.1 starts actually mattering to someone, independent of whether the infrastructure is still single-server at that point.

**Recommendation:** rephrase the revisit trigger in [05-risks.md item 2](./05-risks.md) from "revisit when moving to separate infrastructure" to something like "revisit before or at Phase E (hosted access, [08-prototype-roadmap.md](./08-prototype-roadmap.md)) — the point real users start depending on unbroken history, regardless of whether infrastructure has changed yet." This doesn't change the MVP decision (still zero redundancy today) — it just ties the re-evaluation to the risk actually materializing rather than to an unrelated infra milestone that could slip past Phase E without anyone reconsidering.

### 4. Per-(exchange, symbol) Redis Stream keying is the right call, but it changes what "monitoring" has to mean

**Documented/implemented decision**: `stream_prefix = f"raw:{exchange}:{symbol.lower()}"` — each symbol gets its own independent `:trades`/`:depth` stream and its own `maxlen`. Confirmed sound during the [load-testing analysis](./11-load-testing-plan.md): it correctly isolates a hot symbol's trim clock from a cold symbol's, so BTCUSDT filling up doesn't get subsidized headroom from 1000PEPEUSDT's much slower stream, nor does it falsely appear "safe in aggregate" while one specific stream is actually close to trimming.

**Architectural consequence worth stating explicitly** (not previously called out): at full Binance+Bybit scale this is **86 independent streams** (43 symbols × 2), each needing its own lag check per the consumer-lag alerting ADR-001 proposes — "monitor Redis lag" is not a single number to watch, it's 86 of them. This is the right trade-off (per-symbol isolation is worth the monitoring fan-out), but the alerting implementation needs to be built as a loop over the symbol list from config, not a single hardcoded stream name, or it will quietly only cover whichever symbol it was written against.

**Recommendation:** when implementing the lag alert from [ADR-001](./09-hardening-tests-load-auth.md#2-load-testing), derive the stream list from the same symbol configuration source `runner.py`/exchange adapters use (or, once multi-symbol collection lands per [08-prototype-roadmap.md Phase A](./08-prototype-roadmap.md), from that config), not a hardcoded list — otherwise the alert silently stops covering new symbols as they're added.

### 5. Stack choices (`websockets`, Redis Streams as broker, `clickhouse-connect`, FastAPI+MCP ASGI mount) — confirmed sound for stated scale

Quick validation table, since these are the "should we use X or Y" style decisions the ADR skill's template is built around, and each already has documented reasoning in [01-stack.md](./04-architecture/01-stack.md) worth confirming rather than re-litigating:

| Decision | Documented reasoning | Validation |
|---|---|---|
| `websockets` over `aiohttp` for the WS client | Both handle protocol-level pings automatically; `aiohttp` drags in a full HTTP framework for one feature | **Sound.** At 60-86 total streams (well under `websockets`' comfortable range for a single process) this is correctly a "don't drag in more than needed" call, not a performance-driven one — matches the actual bottleneck (message volume/state, not connection-library overhead). |
| Redis Streams over Kafka/NATS for the broker | Reuses the Redis already needed for the (not-yet-built) hot cache; consumer-group support via `redis.asyncio` is mature enough | **Sound for MVP scale, with the caveat ADR-001/§11 already surfaces** (no `maxmemory` cap alongside `noeviction`, single logical-DB separation from the future cache is a policy convention, not enforced isolation). Revisit only if consumer-group fan-out (multiple normalizer replicas, explicitly deferred per `consumer.py`'s own docstring) becomes necessary — Kafka's stronger multi-consumer/partition story would matter then, not now. |
| `clickhouse-connect` (official async client) over alternatives | Native async since 0.12.0, avoids `asyncio.to_thread` wrapping | **Sound**, no issues found reading `clickhouse_sink.py`/`clickhouse_client.py`. |
| MCP mounted as a separate ASGI app under the same FastAPI process, via `streamable_http_app()` | Keeps REST and MCP sharing one process/one set of query functions ("can never drift apart") | **Sound for 5-10 users**, but [the authorization spec](./12-authorization-spec.md §3) found this exact mounting choice is *why* auth can't be a simple FastAPI `Depends()` — worth knowing this coupling exists before the next feature that assumes REST-only middleware patterns "just work" for MCP too. Not a defect, just a consequence to keep in mind. |

## Consequences

- **Easier:** `incidents` queries no longer need to trust every caller to
  implement "latest row per id" correctly (once #1 lands); a documented,
  accurate supervision story for anyone reading `01-stack.md` fresh (#2); a
  concrete, risk-tied trigger for reconsidering collector redundancy instead
  of an infrastructure-shaped one that could quietly pass by (#3); lag
  alerting that scales with the symbol list instead of needing a manual
  update per new exchange/symbol (#4).
- **Harder:** `ReplacingMergeTree`'s eventual (merge-time) deduplication
  means anyone querying `incidents` without `FINAL`/`argMax` can still
  transiently see both the open and resolved row — this needs to be a known
  query-pattern rule (documented in `queries.py`'s `list_incidents`
  docstring once implemented), not an assumption.
- **To revisit:** whether `trades`/`orderbook_events` should eventually also
  move off plain `MergeTree` once the synchronous-read requirement is
  better understood (today, nothing in `queries.py` was found to require
  read-your-own-write consistency immediately after ingest, but this
  wasn't exhaustively verified across every query path).

## Action Items

1. [ ] Change `incidents`' ClickHouse engine to `ReplacingMergeTree(ingested_at)`
   (migration `005_incidents_replacing_mergetree.sql` or equivalent) and
   confirm/add `FINAL`/`argMax`-based "latest row per id" logic in
   `queries.list_incidents` — cross-check against
   [docs/10-test-plan.md](./10-test-plan.md) test case 130, which already
   exercises exactly this "two rows, same id, open then resolved" scenario.
2. [ ] Update [01-stack.md](./04-architecture/01-stack.md)'s process
   supervision section to describe Docker Compose `restart: unless-stopped`
   instead of the undeployed systemd-unit plan; update the corresponding
   docstring reference in `src/collector/runner.py`.
3. [ ] Reword the revisit trigger in [05-risks.md item 2](./05-risks.md) to
   tie collector-redundancy reconsideration to Phase E (real hosted users)
   rather than "moving to separate infrastructure."
4. [ ] When implementing the ADR-001 consumer-lag alert, source the
   stream/symbol list from shared config rather than hardcoding it, so
   coverage doesn't silently lag behind the symbol list as it grows.
5. [ ] No action needed on stack choices (§5) — recorded here as a
   confirmed-sound validation, useful the next time someone reconsiders one
   of these libraries.
