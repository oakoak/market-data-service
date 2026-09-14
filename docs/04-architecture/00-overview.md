# Architecture — Overview

## 6.1 Data History — Key Constraint

- **Trades/OHLCV** — can be partially backfilled from exchange bulk dumps (e.g. `data.binance.vision`).
- **L2/L3 order book** — exchanges **do not provide** history for diff streams of the order book. The only way to have such data is to **listen to the WS stream ourselves and accumulate an archive live from the moment the collector starts**. Our history depth = the age of our collection infrastructure, not the age of the exchange.
- **Consequence**: one collector failure = an unrecoverable hole in history, forever → collector redundancy is needed (at minimum 2 independent instances per exchange/channel pair with cross-check and de-dup) from day one, not as a "future" task.

## 6.2 Data Collection Layer

- Not "one heavy-image container per exchange" — Docker image size barely affects RAM (layers are shared). The real RAM drivers are: number of WS connections and their buffers, in-memory order-book state, runtime overhead.
- One async process per exchange (Node/Python asyncio/Go/Rust tokio) holds hundreds-to-thousands of WS connections in a single process.
- Stateless/stateful split: the collector (dumb, writes raw bytes + receive_ts to the queue) is stateless and easily scales horizontally; order-book state (needed only for point-in-time snapshots) is moved out to a separate service.
- **Collector generic contract (2026-09):** one adapter instance (one exchange/segment/symbol) can open several independently-managed named WS connections concurrently -- not just one -- since some exchanges split their WS surface (e.g. Binance USDT-M perp's `/public`+`/market` split, see [Binance map](./exchanges/binance.md)); it can also declare periodic REST-only pollers for data with no WS stream at all (e.g. Open Interest). Both stay dumb-collector plumbing (`src/collector/adapter.py`, `src/collector/runner.py`) -- no parsing, just transport.
- A message broker (Kafka/NATS/Redis Streams) between collection and processing decouples the "received" lifecycle from "processed/stored".
- The collector is isolated behind a simple contract (raw bytes + timestamp at queue entry) — rewriting exactly this layer in Rust in the future becomes a local change.

## 6.3 Diagram

```
[Collectors: 1 process/exchange, async, WS+REST poll, ×2 for redundancy]
        → raw messages + receive_ts
[Message broker: Kafka/NATS/Redis Streams]
        →
[Normalizer/processor: parsing into a unified schema, gap/sequence-break detection → incidents]
        →
[Storage: ClickHouse (hot) + S3/Parquet (cold), periodic book snapshots]
        →
[Serving: REST/WS API + MCP tools] ← [Cache: Redis hot tier, CDN/reverse-proxy immutable tier]
        →
[Observability: collector heartbeat/reconnect metrics → incidents]
```

### 6.3.1 Incidents Schema

Detail for `GET /incidents?symbol=&from=&to=` ([MVP decisions](../03-mvp-scope.md)), shaped by the source-specific constraints found in the exchange maps under [architecture/exchanges](./exchanges/).

**`type` values:**

| `type` | What it captures | Rationale |
|---|---|---|
| `collector_disconnect` | Collector WS connection drop/recovery | §6.1: a collector failure is an unrecoverable hole in history, forever |
| `orderbook_sequence_break` | Break in update-sequence continuity (Binance: `U`/`u`/`pu`; Bybit: `u`/`seq`) → resync via new REST snapshot (Binance) or WS resubscribe (Bybit) | See [Binance map](./exchanges/binance.md) and [Bybit map](./exchanges/bybit.md): on a break — resync with an incident record |
| `reference_price_freeze` | `markPrice`/`indexPrice` frozen at the last value outside XAU/XAG trading hours | See [Binance map](./exchanges/binance.md): the gap detector must not confuse this with a feed outage |
| `backfill_gap` | Confirmed unrecoverable history hole (L2 after collector downtime, or unconfirmed bulk-dump coverage) | §6.1, [Binance map](./exchanges/binance.md) (XAUUSDT/XAGUSDT bulk-dump coverage unconfirmed) |
| `clock_drift` | NTP drift on the collection server beyond a threshold | [Risks](../05-risks.md) item 3 |

`liquidation_partial_coverage` (incompleteness of `forceOrder`) is **not** part of this list, see below.

**Common record fields (generalized for multiple exchanges):**

| Field | Type | Notes |
|---|---|---|
| `id` | string | unique incident id |
| `exchange` | string enum | `binance`, `bybit`, ... — open enum, extended as exchanges are added. Needed because `segment` and `affected_channel` alone don't disambiguate across exchanges. |
| `type` | string enum | see table above |
| `severity` | string enum | `info` / `warning` / `critical` |
| `segment` | string | **exchange-native vocabulary, not unified** — Binance: `spot`/`usdtm`/`coinm`; Bybit: `spot`/`linear`/`inverse`; interpreted together with `exchange`. Forcing a single cross-exchange vocabulary (e.g. mapping Binance `usdtm`→`linear`) was considered and rejected: it breaks fidelity to each exchange's own docs/terminology for marginal consumer convenience, and an invented mapping would need re-litigating at every new exchange. If cross-exchange rollups become a common query pattern, add a separate derived `market_class` field later rather than overloading `segment`. |
| `symbol` | string, nullable | `null` if the incident affects the whole segment |
| `affected_channel` | string enum (normalized) | `orderbook` / `trades` / `mark_price` / `funding` / `liquidations` / `collector` — a logical, exchange-agnostic name. Unlike `segment`, this is a good candidate for a shared vocabulary: the underlying question ("was this an order-book problem or a trade-feed problem?") is the same across exchanges, and the literal WS channel names (Binance `depth`/`aggTrade`/`markPrice`, Bybit `orderbook`/`publicTrade`/`tickers`) are internal plumbing, not external product terminology. The literal exchange-native channel name is preserved in `details.source_channel` so nothing is lost for debugging. Note: Bybit's `tickers` channel carries both mark-price and funding-rate updates, so `mark_price` and `funding` incidents from Bybit can share the same `source_channel` value while differing in `affected_channel`. |
| `start_ts` | string (ISO 8601 UTC) | |
| `end_ts` | string (ISO 8601 UTC), nullable | `null` while still open |
| `status` | string enum | `open` / `resolved` |
| `description` | string | human-readable summary |
| `detected_by` | string | detector/rule name or system id |
| `details` | object | `type`- and exchange-specific payload, always includes `source_channel` (literal exchange-native WS channel name); e.g. for `orderbook_sequence_break` on Binance — `last_valid_update_id`, `resync_source`; on Bybit — `u`, `seq`, `resync_source: "ws_resubscribe"`; for `collector_disconnect` — `downtime_ms`, `reconnect_attempts` |

**Static vs dynamic — split into two endpoints:**

The `forceOrder` limitation (see [Binance map](./exchanges/binance.md)) is structurally unlike the other types — it's not an event with a start and end, but an inherent property of the source since the stream started: it's physically impossible to put an honest `start_ts`/`end_ts` on a specific lost liquidation (otherwise the source wouldn't be incomplete). If this were shoved into `/incidents` as a regular record, a client polling a date range would see the same disclaimer in every response — noise instead of a signal that "something happened specifically in this range".

Solution:
- `GET /incidents?symbol=&from=&to=` — dynamic types only from the table above, time-ranged, answers "what happened in this range".
- `GET /known-limitations` — a static, rarely-changing list of structural source limitations (`liquidation_partial_coverage` and future similar ones), with no `end_ts`. At MVP scale this doesn't need a separate table/daemon — a static config/JSON is sufficient (§6.7 principle — don't drag in more than needed).

`reference_price_freeze` is deliberately kept dynamic rather than static: unlike `forceOrder`, each freeze is detectable as a concrete range for a concrete symbol — just with `severity: info`. **Implementation note (2026-09, src/normalizer/incidents.py):** the MVP detector is a best-effort heuristic ("N consecutive unchanged `markPrice` values for XAUUSDT/XAGUSDT") with no real trading-hours/holiday calendar behind it — see docs/06-next-steps.md's equivalent open item for Bybit. `severity: info` is exactly what makes an eventual false positive from this heuristic (e.g. a genuinely flat price during open hours) low-cost rather than misleading.

**Example — order book sequence break, Binance:**

```json
{
  "id": "6f2b3a10-1e4d-4a3c-9c3e-2a7f0e1b9d01",
  "exchange": "binance",
  "type": "orderbook_sequence_break",
  "severity": "warning",
  "segment": "usdtm",
  "symbol": "BTCUSDT",
  "affected_channel": "orderbook",
  "start_ts": "2026-09-10T14:22:31.104Z",
  "end_ts": "2026-09-10T14:22:31.980Z",
  "status": "resolved",
  "description": "Break in pu/u continuity in the order book diff stream, full resync performed",
  "detected_by": "normalizer",
  "details": {
    "source_channel": "depth",
    "last_valid_update_id": 48213099120,
    "resync_source": "REST snapshot lastUpdateId=48213100210"
  }
}
```

**Example — order book sequence break, Bybit** (see [Bybit map](./exchanges/bybit.md) for why the continuity check and `details` shape differ from Binance):

```json
{
  "id": "inc_20260913_0002",
  "exchange": "bybit",
  "type": "orderbook_sequence_break",
  "severity": "warning",
  "segment": "linear",
  "symbol": "BTCUSDT",
  "affected_channel": "orderbook",
  "start_ts": "2026-09-13T09:47:31.005Z",
  "end_ts": null,
  "status": "open",
  "description": "Orderbook 'u' sequence counter skipped an increment; no formal continuity guarantee from Bybit, resubscribed to recover",
  "detected_by": "normalizer",
  "details": {
    "source_channel": "orderbook",
    "u": 883021,
    "seq": 91728340155,
    "resync_source": "ws_resubscribe"
  }
}
```

**Example — static record in `/known-limitations` (not in `/incidents`):**

```json
{
  "exchange": "binance",
  "type": "liquidation_partial_coverage",
  "affected_channel": "liquidations",
  "source_channel": "forceOrder",
  "symbol": null,
  "since_ts": "2026-01-01T00:00:00.000Z",
  "description": "forceOrder only emits the largest liquidation per 1000ms per symbol; smaller ones in the same window are lost permanently, no complete REST history exists"
}
```

## 6.4 Caching

Historical data is immutable — the whole strategy is built on this:
- **Immutable range cache** (CDN/reverse-proxy) — historical queries are cached aggressively and indefinitely, keyed by (exchange, symbol, type, from, to).
- **Hot cache** (Redis) — the latest book snapshot + last N trades, short TTL, realtime tail.
- **Periodic materialized book snapshots** (every 1-5 min) — bound the replay depth needed for point-in-time queries.
- File exports (Parquet/CSV), if they appear — an ideal candidate for the CDN.

## 6.5 Storage

- Columnar storage with compression — ClickHouse (delta/double-delta codecs for timestamps, ZSTD) — realistically 5-20x compression vs naive JSON.
- Tiering: hot data on fast disk in ClickHouse; cold data as Parquet on S3-compatible object storage. ClickHouse supports TTL-based part migration to S3 out of the box.
- Delta-encoded order book — we store diff events, not the full state on every message; full state only in periodic snapshots.
