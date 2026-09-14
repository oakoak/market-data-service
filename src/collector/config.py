"""Env-var-driven config.

Only Binance BTCUSDT spot is wired up in this prototype, but EXCHANGE/SYMBOL/
SEGMENT are read from the environment (not hardcoded) so a config-driven
multi-instrument/exchange expansion later doesn't require touching this
module's shape -- just adding branches to `build_adapter`.
"""

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
    stream_maxlen: int
    backoff_initial_s: float
    backoff_max_s: float

    @property
    def stream_prefix(self) -> str:
        # Must include segment -- multiple segments (spot, usdtm, coinm) for
        # the same exchange/symbol run as concurrent collector instances and
        # would otherwise collide on identical Redis stream names (e.g.
        # binance spot BTCUSDT and binance usdtm BTCUSDT trades/depth
        # interleaving on the same stream). Format mirrors
        # normalizer/config.py's stream_prefix -- both sides must agree.
        return f"raw:{self.exchange}:{self.segment}:{self.symbol.lower()}"


def load_config() -> Config:
    return Config(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        redis_stream_db=int(os.environ.get("REDIS_STREAM_DB", "1")),
        exchange=os.environ.get("EXCHANGE", "binance").lower(),
        segment=os.environ.get("SEGMENT", "spot").lower(),
        symbol=os.environ.get("SYMBOL", "BTCUSDT").upper(),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        stream_maxlen=int(os.environ.get("REDIS_STREAM_MAXLEN", "1000000")),
        backoff_initial_s=float(os.environ.get("BACKOFF_INITIAL_S", "1")),
        backoff_max_s=float(os.environ.get("BACKOFF_MAX_S", "60")),
    )
