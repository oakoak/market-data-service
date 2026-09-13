-- Migration 003: periodic full order-book snapshots
-- Per docs/04-architecture/00-overview.md §6.4: "Periodic materialized book snapshots
-- (every 1-5 min) -- bound the replay depth needed for point-in-time queries", and §6.5:
-- "full state only in periodic snapshots". Also used as the REST bootstrap snapshot
-- (`GET /api/v3/depth`, `lastUpdateId`) captured by the collector/normalizer for spot
-- (docs/04-architecture/exchanges/binance.md, "Order book bootstrap + reconciliation").

CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.orderbook_snapshots
(
    exchange        LowCardinality(String),            -- e.g. 'binance'
    segment         LowCardinality(String),             -- e.g. 'spot'
    symbol          LowCardinality(String),              -- e.g. 'BTCUSDT'

    -- Binance REST snapshot field: `lastUpdateId`. This is the update id that the
    -- corresponding orderbook_events row(s) must satisfy U <= last_update_id+1 <= u
    -- against, to splice a snapshot with the diff stream for point-in-time reconstruction.
    last_update_id  UInt64,

    -- Full book state at snapshot time (not a delta): every level present, no zero-qty
    -- "remove" semantics here (unlike orderbook_events).
    bids            Array(Tuple(price Float64, quantity Float64)),
    asks            Array(Tuple(price Float64, quantity Float64)),

    -- Snapshot-taking time. There is only one meaningful timestamp for a REST/periodic
    -- snapshot capture (no separate exchange-side event time is published for it), but we
    -- still record both to keep the dual-timestamp convention uniform across all tables:
    -- ts_exchange here is set to the same instant as ts_received unless a future exchange
    -- publishes its own snapshot-generation timestamp.
    ts_exchange     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
    ts_received     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),

    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_received)
ORDER BY (exchange, symbol, ts_received)
SETTINGS index_granularity = 8192;
