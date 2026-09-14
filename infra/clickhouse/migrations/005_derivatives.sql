-- Migration 005: derivatives (perp-only) data -- funding/mark/index price, open
-- interest, liquidations.
-- Phase B of docs/08-prototype-roadmap.md: Binance USDT-M perp collection on top of
-- the spot-only prototype. Scope per docs/03-mvp-scope.md's `get_derivatives_metrics`
-- tool ("funding + OI + liquidations together") and docs/02-data-model.md's
-- "Instrument-specific data" column for Perpetual: funding rate, open interest,
-- mark/index price, liquidations. Perp-only (spot has none of these); futures-with-
-- expiry (COIN-M) can reuse the same tables later (`segment` disambiguates), no
-- schema change expected.
--
-- Design call -- one table for mark/index price + funding, not three:
-- Binance's `{symbol}@markPrice@1s` (docs/04-architecture/exchanges/binance.md,
-- "Data -> source map" / USDT-M perp / "Funding + mark/index price" column) emits
-- mark price (`p`), index price (`i`) and funding rate (`r`) together, atomically,
-- in one WS message at 1s cadence -- there is no independent cadence or independent
-- source for any of the three that would justify separate tables (unlike Open
-- Interest and Liquidations, which are genuinely separate REST/WS sources with their
-- own cadences -- see below). Splitting them would only require re-joining what the
-- exchange already gives us joined, for no benefit -- so `mark_price` below carries
-- all three. Note this is the funding *rate* published ahead of settlement, not a
-- record of funding *payment* events (Binance does not publish a payment-event
-- stream/REST endpoint for that; out of scope).
--
-- Open Interest and Liquidations are separate tables: OI is REST-poll-only on its
-- own ~45s cadence (no WS stream at all, `open_interest`), liquidations are WS
-- `@forceOrder` events with no natural relationship to mark price or OI's cadence
-- (`liquidations`) -- both concerns and both cadences are independent of markPrice
-- and of each other, so each gets its own table per the same reasoning 001-004 use
-- (one source-shaped concern per table).

CREATE DATABASE IF NOT EXISTS market_data;

-- Mark price / index price / funding rate, combined (see header). Binance USDT-M
-- `{symbol}@markPrice@1s` message fields: `p` (mark price), `i` (index price),
-- `r` (funding rate), `T` (next funding time), `E` (event time).
CREATE TABLE IF NOT EXISTS market_data.mark_price
(
    exchange          LowCardinality(String),           -- e.g. 'binance'
    segment           LowCardinality(String),            -- exchange-native vocabulary, e.g. 'usdtm' (Binance); not unified across exchanges
    symbol            LowCardinality(String),             -- e.g. 'BTCUSDT'

    mark_price        Float64,                            -- Binance `p`
    index_price       Float64,                            -- Binance `i`

    -- Nullable: funding rate is a perp-only concept and this table is written by
    -- perp segments only today, but kept Nullable rather than non-null so a future
    -- source that shares this table's shape (e.g. a futures-with-expiry contract
    -- with no funding) doesn't force a schema change, matching 002's precedent for
    -- `prev_update_id`.
    funding_rate      Nullable(Float64),                  -- Binance `r`
    next_funding_time Nullable(DateTime64(3, 'UTC')),      -- Binance `T`

    -- Dual timestamp, mandatory on every record per docs/03-mvp-scope.md and
    -- 00-overview.md §6.3.1/6.4
    ts_exchange       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),  -- Binance `E` (event time)
    ts_received       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),  -- local receive time at the collector

    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange)
SETTINGS index_granularity = 8192;

-- Open Interest. Binance `GET /fapi/v1/openInterest` REST response fields:
-- `openInterest`, `time`. REST-only, no WS stream exists (binance.md) -- collected
-- via the collector's `rest_pollers()` mechanism, published on the `{stream_prefix}:poll`
-- Redis stream with `type: "open_interest"` (per the collector-agent's Phase B change,
-- see memory/collector.md / memory/exchanges.md).
CREATE TABLE IF NOT EXISTS market_data.open_interest
(
    exchange        LowCardinality(String),
    segment         LowCardinality(String),
    symbol          LowCardinality(String),

    open_interest   Float64,                              -- Binance `openInterest` (contracts, not notional)

    -- Dual timestamp. Binance's openInterest REST response does carry its own
    -- `time` field (unlike the spot depth-snapshot REST response, which doesn't) --
    -- so, unlike 003_orderbook_snapshots.sql, ts_exchange is NOT forced equal to
    -- ts_received here; it is the exchange-reported `time`.
    ts_exchange     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),   -- Binance `time`
    ts_received     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),   -- local receive time (poll completion)

    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange)
SETTINGS index_granularity = 8192;

-- Liquidations. Binance `{symbol}@forceOrder` message, nested under `o`: `s` (symbol),
-- `S` (side), `p` (price), `ap` (average price), `q` (original quantity),
-- `z` (filled/last quantity), `X` (order status), `T` (order trade time).
-- IMPORTANT (binance.md, docs/04-architecture/00-overview.md §6.3.1): this stream is
-- NOT a full liquidation tape -- Binance emits only the largest liquidation per
-- symbol per 1000ms window; smaller ones in the same window are permanently lost.
-- This is recorded as a static `liquidation_partial_coverage` entry in
-- `/known-limitations` (see incidents.py / overview.md §6.3.1), not as a per-row flag
-- here -- the table simply stores what the stream actually delivers, verbatim.
-- No stable liquidation id is published by Binance for this stream, so unlike
-- trades/orderbook_events there is no natural dedup key beyond the full tuple; the
-- ORDER BY below accepts (exchange, symbol, ts_exchange) as "good enough" ordering
-- for range queries, same trade-off precedent as 003_orderbook_snapshots.sql (no
-- unique id column available from the source, not the normalizer's error).
CREATE TABLE IF NOT EXISTS market_data.liquidations
(
    exchange          LowCardinality(String),
    segment           LowCardinality(String),
    symbol            LowCardinality(String),

    side              LowCardinality(String),             -- Binance `S`: 'BUY' | 'SELL'
    price             Float64,                             -- Binance `p`
    avg_price         Float64,                             -- Binance `ap`
    quantity          Float64,                             -- Binance `q` (original order quantity)
    filled_quantity   Float64,                             -- Binance `z` (filled quantity, i.e. `q`'s executed portion)
    order_status      LowCardinality(String),              -- Binance `X`, e.g. 'FILLED'

    ts_exchange       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),  -- Binance `T` (order trade time)
    ts_received       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),

    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange)
SETTINGS index_granularity = 8192;
