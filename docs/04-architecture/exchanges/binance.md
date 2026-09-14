# Binance — Instrument and Data Source Map (spot + perp/futures)

First concrete collection slice: Binance only, spot and perp/futures only (options, Bybit, and equity-perps via Nest Exchange are out of this round — Bybit is covered separately in [bybit.md](./bybit.md)).

**Instruments (supersedes the list in [MVP decisions](../../03-mvp-scope.md)):**
- **Spot**: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, BNBUSDT, DOGEUSDT, BTCFDUSD, ETHFDUSD — FDUSD pairs are mandatory; Binance's zero fees drive a huge flow into them, without them the liquidity picture is incomplete.
- **USDT-M perp**: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, BNBUSDT, ADAUSDT, LINKUSDT, AVAXUSDT, 1000PEPEUSDT, BTCUSDC, ETHUSDC, XAUUSDT, XAGUSDT. All on the same `fapi`/`fstream` host, with identical stream names; USDC-perps differ only in the `quoteAsset`/`marginAsset` field in `exchangeInfo` (affects collateral/PnL calculation, not market data collection) — no separate routing needed.
- **COIN-M perp**: BTCUSD_PERP, ETHUSD_PERP — needed for basis and funding arbitrage between linear and inverse contracts.
- **Explicitly excluded for now**: equity-perps (stocks/ETFs) — go through Nest Exchange Limited, a separate regulated Binance entity (ADGM), likely a separate API host; require a separate infrastructure check before inclusion.

**Technical details found while checking current documentation (September 2026):**
- **USDT-M futures WS has already switched connection schemes.** Legacy `wss://fstream.binance.com/ws` and `/stream` are officially being retired after 2026-04-23 — the deadline has already passed as of writing. New base URLs: `/public` (high-frequency book/ticker), `/market` (aggTrade, markPrice, kline), `/private` (user data). The USDT-M collector is written directly against the new scheme.
- **COIN-M futures** was not affected by the restructuring — it still uses the single `wss://dstream.binance.com/stream` scheme. I.e. USDT-M and COIN-M are not just different symbols on one protocol, but two different connection protocols.
- **Spot** also remains on the old unified scheme (`stream.binance.com/ws` or `/stream`).
- **`forceOrder` (liquidations) is not a full tape.** The stream emits a snapshot of "the largest liquidation in the last 1000ms per symbol"; smaller liquidations in the same window are permanently lost, and no REST source for full liquidation history exists. This is recorded as a known source limitation (a candidate for the incidents schema), not a collector bug.
- **Open Interest is REST-only**, there is no WS stream on either USDT-M or COIN-M (`GET /fapi/v1/openInterest`, weight 1). This is the only mandatory REST polling in the perp pipeline, on par with periodic polling at a fixed interval.
- **XAUUSDT/XAGUSDT — market data runs on the same infrastructure** as crypto perps (same streams, same `exchangeInfo`), with two caveats: (1) `markPrice`/`indexPrice` are derived from the COMEX/LBMA reference and **freeze at the last value outside gold/silver trading hours** (weekends, TradFi holidays), while `trades`/`depth`/`aggTrade` keep flowing — the gap/incident detector must not confuse this with a feed outage; (2) funding interval is 8h (originally 4h) with a much narrower cap (~±0.05%) versus ~±2% for regular crypto perps. Contracts are legally issued via Nest Exchange Limited (ADGM), but this only concerns the trading/account side — public market-data endpoints require no separate onboarding.

**Data → source map:**

| Segment | Trades | Order book L2 | Funding + mark/index price | Open Interest | Liquidations |
|---|---|---|---|---|---|
| Spot | WS `{symbol}@trade` | REST snapshot `GET /api/v3/depth` + WS `{symbol}@depth@100ms` | — | — | — |
| USDT-M perp | WS `{symbol}@aggTrade` (`/market`) | REST `GET /fapi/v1/depth` + WS `{symbol}@depth@100ms` (`/public`) | WS `{symbol}@markPrice@1s` (`/market`) | REST polling `GET /fapi/v1/openInterest` | WS `{symbol}@forceOrder` (`/market`) |
| COIN-M perp | WS `{symbol}@aggTrade` | REST `GET /dapi/v1/depth` + WS `{symbol}@depth@100ms` | WS `{symbol}@markPrice@1s` | REST polling `GET /dapi/v1/openInterest` | WS `{symbol}@forceOrder` |

**Order book bootstrap + reconciliation (spot and futures follow the same idea, differ in fields):**
1. Open WS `@depth` and buffer incoming diff events.
2. Fetch a REST snapshot (`lastUpdateId`).
3. Discard buffered events with `u <= lastUpdateId`.
4. The first applied event must satisfy `U <= lastUpdateId+1 <= u`.
5. Then check continuity: on spot — current event's `U` == previous event's `u` + 1; on futures — via the `pu` field (previous update id) of the current event against the previous event's `u`. On a break — full resync (new snapshot), with an incident recorded.

**Connection strategy for this slice:** the instrument list yields roughly 60-80 streams total (spot + USDT-M + COIN-M) — far from the 1024 streams/connection and 10 incoming messages/sec limits. So there's no need to complicate the MVP with sharding across multiple WS connections: **one combined connection per (exchange, segment) pair** — separately for spot, USDT-M, COIN-M. This fits the MVP server's resource constraints (one process per segment, no extra daemons); sharding into multiple connections is a task for when the list grows by an order of magnitude (Bybit, options).

**REST rate limit budget** (per-IP weight, 1-minute window, counted separately per API):

| API | Limit/min | `depth` (limit=1000) | `openInterest` | `fundingRate` |
|---|---|---|---|---|
| Spot | 6000 | 50/request | — | — |
| USDS-M futures | 2400 | 20/request | 1/request | 1/request (separate limit 500 req/5min) |
| COIN-M futures | 2400 (own pool, not shared with USDS-M) | 20/request | 1/request | 1/request |

With order-book resync every 30-60s across ~30 symbols (spot+USDT-M+COIN-M), and the same polling interval for OI/funding — total load is ~12% of the limit on spot and USDT-M, negligible for COIN-M. There is enough headroom that backoff/throttling doesn't need to be designed as a critical MVP component — but it's worth laying it in as a config parameter for the future.

**USDT-M perp adapter implementation notes (2026-09, src/exchanges/binance/usdtm.py):** the `/public`/`/market` host was not spelled out above -- implemented as `wss://fstream.binance.com/public` and `wss://fstream.binance.com/market` (same host as the legacy `/stream` endpoint, new paths), same `?streams=...` combined-stream query param as before; verify against current Binance docs before relying on it in production. Open Interest poll interval chosen: 45s (middle of the 30-60s window this doc's REST budget section discusses; weight 1/request is trivially within budget at that cadence).

**Historical backfill (`data.binance.vision`):**
- Per-day/per-month zip dumps available: `trades`, `aggTrades`, `klines` — for all three segments (spot, `futures/um`, `futures/cm`); futures additionally has `markPriceKlines`, `indexPriceKlines`, `premiumIndexKlines`, `bookTicker`, `liquidationSnapshot`, and periodic `bookDepth` snapshots (not a full diff-event tape, just snapshots — doesn't replace our live WS archive, but useful as a sanity check and a backup cross-verification source).
- Layout: `data/{spot|futures/um|futures/cm}/{daily|monthly}/{type}/{SYMBOL}/[{interval}/]{SYMBOL}-{type}-{YYYY-MM[-DD]}.zip` + `.CHECKSUM`; daily files are published the next day, monthly ones on the first Monday of the following month.
- History depth: spot since 2017, USDT-M futures since 2019-09, COIN-M since 2019-11/2020 — but **only from a given symbol's listing date**, not from these base dates. For our list this means significantly different backfill depths: 1000PEPEUSDT — since 2023-05, BTCFDUSD/ETHFDUSD — since 2023-08, and XAUUSDT/XAGUSDT — only ~8 months (launched January 2026), and it is unconfirmed whether they are even present in bulk dumps yet — needs checking before relying on them for these two symbols.
