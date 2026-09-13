-- Top-level init file for the official clickhouse-server Docker image.
--
-- This file is an alternative to init-db.sh for the simplest possible docker-compose
-- mount: bind-mount ONLY this single file into /docker-entrypoint-initdb.d/, e.g.
--
--   volumes:
--     - ./infra/clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
--
-- The clickhouse-server entrypoint runs *.sql files in /docker-entrypoint-initdb.d/ via
-- `clickhouse-client --multiquery` on first startup (empty data dir only). ClickHouse's
-- client does not support an `\i`/`source`-style file-include directive, so this file
-- inlines the migrations verbatim (kept byte-identical to migrations/00N_*.sql) rather
-- than trying to "include" them -- this keeps a single mountable file in sync with the
-- authoritative, individually-runnable migration files under migrations/.
--
-- Preferred mount strategy (recommended to the docker-compose owner): mount the
-- migrations/ directory itself as /docker-entrypoint-initdb.d/, so 001_/002_/003_/004_
-- run in numeric filename order with no duplication risk:
--
--   volumes:
--     - ./infra/clickhouse/migrations:/docker-entrypoint-initdb.d:ro
--
-- Use THIS file only if a single-file mount is preferred instead. Do not mount both this
-- file and migrations/ into the same /docker-entrypoint-initdb.d/ directory -- that would
-- apply every CREATE TABLE statement twice (harmless due to IF NOT EXISTS, but redundant).

CREATE DATABASE IF NOT EXISTS market_data;

-- === 001_trades.sql ===

CREATE TABLE IF NOT EXISTS market_data.trades
(
    exchange        LowCardinality(String),
    segment         LowCardinality(String),
    symbol          LowCardinality(String),

    trade_id        UInt64,
    price           Float64,
    quantity        Float64,
    quote_qty       Float64      DEFAULT price * quantity CODEC(ZSTD),
    is_buyer_maker  Bool,

    buyer_order_id  Nullable(UInt64),
    seller_order_id Nullable(UInt64),

    ts_exchange     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
    ts_received     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),

    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange, trade_id)
SETTINGS index_granularity = 8192;

-- === 002_orderbook_events.sql ===

CREATE TABLE IF NOT EXISTS market_data.orderbook_events
(
    exchange          LowCardinality(String),
    segment           LowCardinality(String),
    symbol            LowCardinality(String),

    first_update_id   UInt64,
    final_update_id   UInt64,
    prev_update_id    Nullable(UInt64),

    bids              Array(Tuple(price Float64, quantity Float64)),
    asks              Array(Tuple(price Float64, quantity Float64)),

    ts_exchange       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
    ts_received       DateTime64(3, 'UTC') CODEC(Delta, ZSTD),

    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange, final_update_id)
SETTINGS index_granularity = 8192;

-- === 003_orderbook_snapshots.sql ===

CREATE TABLE IF NOT EXISTS market_data.orderbook_snapshots
(
    exchange        LowCardinality(String),
    segment         LowCardinality(String),
    symbol          LowCardinality(String),

    last_update_id  UInt64,

    bids            Array(Tuple(price Float64, quantity Float64)),
    asks            Array(Tuple(price Float64, quantity Float64)),

    ts_exchange     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
    ts_received     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),

    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_received)
ORDER BY (exchange, symbol, ts_received)
SETTINGS index_granularity = 8192;

-- === 004_incidents.sql ===

CREATE TABLE IF NOT EXISTS market_data.incidents
(
    id               String,

    exchange         LowCardinality(String),
    type             LowCardinality(String),
    severity         LowCardinality(String),
    segment          LowCardinality(String),
    symbol           Nullable(String),
    affected_channel LowCardinality(String),

    start_ts         DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
    end_ts           Nullable(DateTime64(3, 'UTC')),

    status           LowCardinality(String),
    description      String,
    detected_by      String,
    details          JSON,

    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(start_ts)
ORDER BY (exchange, start_ts, id)
SETTINGS index_granularity = 8192;
