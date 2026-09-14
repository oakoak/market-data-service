"""Env-var-driven config.

**Phase A (2026-09, multi-symbol spot, docs/08-prototype-roadmap.md):**
`SYMBOL` (singular) is replaced by `SYMBOLS` -- a comma-separated list. One
collector process still handles exactly one (exchange, segment), but now
*all* symbols in that list concurrently, multiplexed over the combined WS
connection(s) the adapter opens (binance.md: "one combined connection per
(exchange, segment) pair", ~60-80 streams total across the full instrument
list -- well inside Binance's 1024-streams/connection cap). `SYMBOL`
(singular) is still accepted as a fallback for a single-symbol deployment
(e.g. the USDT-M perp segment, which stays one-symbol-per-process for now --
Phase C territory to generalize) so existing `.env` files / USDT-M compose
blocks keep working unchanged.
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
    symbols: tuple[str, ...]
    log_level: str
    stream_maxlen: int
    backoff_initial_s: float
    backoff_max_s: float

    def stream_prefix(self, symbol: str) -> str:
        # Per-symbol prefix -- must include segment *and* symbol so that (a)
        # concurrent segments of the same exchange/symbol never collide
        # (e.g. binance spot BTCUSDT vs binance usdtm BTCUSDT), and (b),
        # since Phase A, multiple symbols handled by the *same* collector
        # process never collide with each other either (e.g. BTCUSDT vs
        # ETHUSDT trades streams). Format must match normalizer/config.py's
        # `stream_prefix` byte-for-byte -- each normalizer instance is still
        # scoped to a single symbol and derives this same string from its
        # own `SYMBOL` env var.
        return f"raw:{self.exchange}:{self.segment}:{symbol.lower()}"


def _parse_symbols(raw: str) -> tuple[str, ...]:
    symbols = tuple(s.strip().upper() for s in raw.split(",") if s.strip())
    if not symbols:
        raise ValueError(f"no symbols parsed from {raw!r}")
    # De-duplicate while preserving order -- a repeated symbol in the env
    # var would otherwise open the same WS stream twice and double-count
    # everything downstream.
    seen: dict[str, None] = {}
    for s in symbols:
        seen.setdefault(s, None)
    return tuple(seen)


def load_config() -> Config:
    # SYMBOLS (plural) is the primary knob; SYMBOL (singular) is kept as a
    # fallback so a single-symbol .env (e.g. the usdtm segment) still works
    # without every deployment needing to switch to the plural form.
    symbols_raw = os.environ.get("SYMBOLS") or os.environ.get("SYMBOL", "BTCUSDT")
    return Config(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        redis_stream_db=int(os.environ.get("REDIS_STREAM_DB", "1")),
        exchange=os.environ.get("EXCHANGE", "binance").lower(),
        segment=os.environ.get("SEGMENT", "spot").lower(),
        symbols=_parse_symbols(symbols_raw),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        stream_maxlen=int(os.environ.get("REDIS_STREAM_MAXLEN", "1000000")),
        backoff_initial_s=float(os.environ.get("BACKOFF_INITIAL_S", "1")),
        backoff_max_s=float(os.environ.get("BACKOFF_MAX_S", "60")),
    )
