"""Env-var-driven config, mirroring collector/config.py's style/shape so the
two services stay consistent (docs/04-architecture/01-stack.md)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    redis_url: str
    redis_stream_db: int
    exchange: str
    segment: str
    symbol: str
    log_level: str

    # Consumer-group identity. Fixed group name per (exchange,segment,symbol)
    # deployment; consumer name defaults to hostname-ish but is overridable
    # for running multiple normalizer replicas against the same group later.
    consumer_group: str
    consumer_name: str

    # Redis read tuning.
    read_block_ms: int
    read_count: int

    # ClickHouse connection.
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str

    # Batched insert sink tuning.
    sink_batch_size: int
    sink_flush_interval_s: float

    # Periodic full-book snapshot materialization interval (docs §6.4: every
    # 1-5 min in production; configurable, default 60s is fine for a 1-day
    # local prototype per task scope).
    book_snapshot_interval_s: float

    @property
    def stream_prefix(self) -> str:
        return f"raw:{self.exchange}:{self.symbol.lower()}"

    @property
    def trades_stream(self) -> str:
        return f"{self.stream_prefix}:trades"

    @property
    def depth_stream(self) -> str:
        return f"{self.stream_prefix}:depth"


def load_config() -> Config:
    exchange = os.environ.get("EXCHANGE", "binance").lower()
    segment = os.environ.get("SEGMENT", "spot").lower()
    symbol = os.environ.get("SYMBOL", "BTCUSDT").upper()
    default_group = f"normalizer:{exchange}:{segment}:{symbol.lower()}"
    return Config(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        redis_stream_db=int(os.environ.get("REDIS_STREAM_DB", "1")),
        exchange=exchange,
        segment=segment,
        symbol=symbol,
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        consumer_group=os.environ.get("CONSUMER_GROUP", default_group),
        consumer_name=os.environ.get("CONSUMER_NAME", f"normalizer-{os.getpid()}"),
        read_block_ms=int(os.environ.get("REDIS_READ_BLOCK_MS", "5000")),
        read_count=int(os.environ.get("REDIS_READ_COUNT", "500")),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "market_data"),
        clickhouse_user=os.environ.get("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        sink_batch_size=int(os.environ.get("SINK_BATCH_SIZE", "500")),
        sink_flush_interval_s=float(os.environ.get("SINK_FLUSH_INTERVAL_S", "2")),
        book_snapshot_interval_s=float(os.environ.get("BOOK_SNAPSHOT_INTERVAL_S", "60")),
    )
