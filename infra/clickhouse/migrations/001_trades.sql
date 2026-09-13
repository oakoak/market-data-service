-- Migration 001: trades table
-- Raw trade tape. For Binance spot this is sourced from the WS `{symbol}@trade` stream
-- (see docs/04-architecture/exchanges/binance.md, "Data -> source map" / Spot / Trades column).
-- NOTE: spot `@trade` gives a genuine per-execution trade id (`t`), unlike the perp `@aggTrade`
-- stream (which aggregates fills at the same price/ts under one id) -- perp support is out of
-- scope for this prototype (BTCUSDT spot only), but the column is named generically (`trade_id`)
-- so a later perp/aggTrade ingestion path can reuse this table without a rename.

CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.trades
(
    -- Identity / routing -- kept exchange-agnostic per architecture overview so Bybit and
    -- additional instruments can be added later without a schema rewrite.
    exchange        LowCardinality(String),           -- e.g. 'binance'
    segment         LowCardinality(String),           -- exchange-native vocabulary, e.g. 'spot' (Binance), not unified across exchanges
    symbol          LowCardinality(String),            -- e.g. 'BTCUSDT'

    -- Trade identity / economics
    trade_id        UInt64,                            -- Binance spot `t` (trade id)
    price           Float64,
    quantity        Float64,
    quote_qty       Float64      DEFAULT price * quantity CODEC(ZSTD),
    is_buyer_maker  Bool,                               -- Binance `m`: true if the buyer is the market maker (i.e. a sell-side aggressor)

    -- Binance spot trade stream also carries maker/taker order ids; kept as nullable
    -- exchange-specific extras since other exchanges may not provide them.
    buyer_order_id  Nullable(UInt64),                   -- Binance `b`
    seller_order_id Nullable(UInt64),                   -- Binance `a`

    -- Dual timestamp, mandatory on every record per docs/03-mvp-scope.md and 00-overview.md §6.3.1/6.4
    ts_exchange     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),   -- Binance `T`: trade time as reported by the exchange
    ts_received     DateTime64(3, 'UTC') CODEC(Delta, ZSTD),   -- local receive time at the collector (queue-entry time)

    -- Ingestion bookkeeping
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (exchange, symbol, ts_exchange, trade_id)
SETTINGS index_granularity = 8192;
