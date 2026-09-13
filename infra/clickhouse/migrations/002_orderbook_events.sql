-- Migration 002: order book diff/delta events (not full state per message)
-- Sourced from Binance spot WS `{symbol}@depth@100ms` (docs/04-architecture/exchanges/binance.md).
-- Per docs/04-architecture/00-overview.md §6.5: "Delta-encoded order book -- we store diff
-- events, not the full state on every message; full state only in periodic snapshots" (see 003).

CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.orderbook_events
(
    exchange          LowCardinality(String),          -- e.g. 'binance'
    segment           LowCardinality(String),          -- e.g. 'spot' (Binance-native vocabulary)
    symbol            LowCardinality(String),           -- e.g. 'BTCUSDT'

    -- Binance spot depth-diff sequencing fields (docs/04-architecture/exchanges/binance.md,
    -- "Order book bootstrap + reconciliation"):
    --   U = first update id in this event, u = final update id in this event.
    --   Continuity check on SPOT: current event's U == previous event's u + 1.
    --   `prev_update_id` (Binance futures `pu`) does NOT exist on the spot stream -- kept
    --   here as Nullable so the same table serves USDT-M/COIN-M futures and Bybit later
    --   without a schema change; it will simply stay NULL for all spot rows.
    first_update_id   UInt64,                           -- Binance `U`
    final_update_id   UInt64,                            -- Binance `u`
    prev_update_id    Nullable(UInt64),                  -- Binance futures `pu` / Bybit `seq`-equivalent; NULL on spot

    -- Book delta payload: arrays of (price, quantity) tuples. A quantity of 0 means
    -- "remove this price level", matching Binance's raw diff semantics verbatim --
    -- no zero-filtering/interpretation is done at ingestion time.
    bids              Array(Tuple(price Float64, quantity Float64)),
    asks              Array(Tuple(price Float64, quantity Float64)),

    -- Dual timestamp, mandatory on every record
    ts_exchange       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),  -- Binance `E` (event time)
    ts_received       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),  -- local receive time at the collector

    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange, final_update_id)
SETTINGS index_granularity = 8192;
