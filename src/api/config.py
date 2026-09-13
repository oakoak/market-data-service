"""Env-var-driven config, mirroring normalizer/config.py's shape so all three
services stay consistent (docs/04-architecture/01-stack.md).

`exchange`/`segment` here are only *defaults* for query params (see rest.py/
mcp_tools.py) -- unlike the collector/normalizer, where they pin the single
instrument being ingested, the API is meant to serve whatever's in ClickHouse,
so every default is overridable per-request.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    api_host: str
    api_port: int
    log_level: str
    default_exchange: str
    default_segment: str

    # ClickHouse connection (read-only client, separate from the normalizer's
    # write-only ClickHouseSink).
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str


def load_config() -> Config:
    return Config(
        api_host=os.environ.get("API_HOST", "0.0.0.0"),
        api_port=int(os.environ.get("API_PORT", "8000")),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        default_exchange=os.environ.get("EXCHANGE", "binance").lower(),
        default_segment=os.environ.get("SEGMENT", "spot").lower(),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "market_data"),
        clickhouse_user=os.environ.get("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
