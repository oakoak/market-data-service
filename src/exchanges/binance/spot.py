"""Binance spot adapter: WS URL construction, REST snapshot fetch, and raw
message classification for any number of spot symbols on one combined
connection.

Per docs/04-architecture/exchanges/binance.md "Data -> source map" / Spot row:
- Trades:     WS `{symbol}@trade`            (genuine per-execution trade id `t`,
              NOT `@aggTrade` -- aggTrade aggregates same-price/ts fills under
              one id; spot uses the plain trade stream, unlike USDT-M/COIN-M
              perp which use aggTrade. This prototype is spot-only.)
- Order book: REST snapshot `GET /api/v3/depth` + WS `{symbol}@depth@100ms`

Spot continuity rule (per binance.md "Order book bootstrap + reconciliation",
step 5): spot has no `pu` field (that's futures-only) -- continuity is
`current.U == previous.u + 1`. The collector only needs U/u to drive the
buffer-until-snapshot-ready state machine; actual gap/incident detection is
the normalizer's job (docs/04-architecture/00-overview.md §6.2, §6.3.1).

**Phase A (2026-09, docs/08-prototype-roadmap.md): multi-symbol.** This
adapter used to be scoped to a single symbol. binance.md's connection-
strategy section already called for "one combined connection per
(exchange, segment) pair" covering the full instrument list (~60-80 streams
total, well inside Binance's 1024-streams/connection cap) -- this class now
does that for spot: `symbols` is a tuple, `ws_urls()` builds one `/stream`
URL carrying every symbol's `@trade` and `@depth@100ms` streams interleaved,
`fetch_snapshot()` takes the symbol to fetch a REST snapshot for (the
bootstrap/reconcile dance runs once per symbol, independently, the first
time that symbol's first depth-diff arrives -- see collector/runner.py), and
`symbol_of()` lets the runner tell which symbol an incoming message belongs
to so it lands on that symbol's own Redis stream.
"""

from __future__ import annotations

from typing import Any

import httpx

# Spot stays on the old unified stream scheme (binance.md: "Spot also remains
# on the old unified scheme"), unlike USDT-M futures which migrated to
# /public //market //private in 2026.
WS_BASE_URL = "wss://stream.binance.com:9443"
REST_BASE_URL = "https://api.binance.com"

# binance.md recommends limit=1000 for the snapshot request (also the top end
# of the documented `depth` weight table).
DEPTH_SNAPSHOT_LIMIT = 1000


class BinanceSpotAdapter:
    """Binance spot adapter for any number of symbols, combined trade +
    depth-diff streams for all of them over one WS connection (binance.md:
    "one combined connection per (exchange, segment) pair" -- segment here
    is 'spot'). Satisfies collector.adapter.ExchangeAdapter's
    multi-connection contract with a single-entry `ws_urls()` dict and an
    empty `rest_pollers()` -- spot has no split WS scheme and no REST-only
    data, unlike USDT-M perp."""

    exchange = "binance"
    segment = "spot"

    def __init__(self, symbols: list[str] | tuple[str, ...] | str) -> None:
        # Accept a single symbol string too (convenience for callers/tests
        # that only care about one symbol) -- normalized to a tuple either
        # way. Binance stream names are lowercase; REST `symbol` query param
        # is case-insensitive but conventionally uppercase.
        if isinstance(symbols, str):
            symbols = [symbols]
        # De-duplicate while preserving order, same rule as
        # collector/config.py's `_parse_symbols` -- a repeated symbol would
        # otherwise subscribe to the same WS streams twice.
        seen: dict[str, None] = {}
        for s in symbols:
            seen.setdefault(s.upper(), None)
        self.symbols: tuple[str, ...] = tuple(seen)
        if not self.symbols:
            raise ValueError("BinanceSpotAdapter requires at least one symbol")

    # -- WS -----------------------------------------------------------------

    def ws_urls(self) -> dict[str, str]:
        """Single named connection ("combined") carrying the trade and
        depth-diff streams for *every* symbol in `self.symbols`, interleaved
        -- spot never split its WS scheme the way USDT-M perp did
        (binance.md: "Spot also remains on the old unified scheme"), so it
        stays a one-entry dict per the ExchangeAdapter.ws_urls() contract
        (collector/adapter.py), now just carrying more streams per entry
        than the single-symbol prototype did.

        Combined-stream envelope shape: {"stream": "<name>", "data": {...}}.
        """
        streams: list[str] = []
        for symbol in self.symbols:
            stream_symbol = symbol.lower()
            streams.append(f"{stream_symbol}@trade")
            streams.append(f"{stream_symbol}@depth@100ms")
        url = f"{WS_BASE_URL}/stream?streams={'/'.join(streams)}"
        return {"combined": url}

    def rest_pollers(self) -> dict[str, tuple]:
        """Spot has no REST-only data (no Open Interest, no funding) --
        nothing to poll, for any symbol."""
        return {}

    def stream_type(self, raw_message: dict[str, Any]) -> str:
        """Classify a combined-stream envelope by its `stream` field --
        symbol-agnostic, the stream name's suffix is what matters."""
        stream = raw_message.get("stream", "")
        if stream.endswith("@trade"):
            return "trade"
        if "@depth" in stream:
            return "depth_diff"
        return "unknown"

    def symbol_of(self, raw_message: dict[str, Any]) -> str:
        """Extract the symbol from a combined-stream envelope's `stream`
        field, e.g. "btcusdt@trade" -> "BTCUSDT", "ethusdt@depth@100ms" ->
        "ETHUSDT". The stream name always starts with the lowercase symbol
        followed by `@`, regardless of which stream type it is -- Binance's
        combined-stream naming convention, not something specific to
        trade/depth."""
        stream = raw_message.get("stream", "")
        symbol = stream.split("@", 1)[0]
        return symbol.upper()

    def depth_update_ids(self, raw_message: dict[str, Any]) -> tuple[int, int]:
        """Return (U, u) from a depth-diff combined-stream envelope."""
        data = raw_message["data"]
        return data["U"], data["u"]

    # -- REST -----------------------------------------------------------------

    async def fetch_snapshot(self, symbol: str) -> dict[str, Any]:
        """`GET /api/v3/depth?symbol=...&limit=1000` -- returns the raw JSON
        body untouched (bids/asks/lastUpdateId), per binance.md step 2 of the
        bootstrap/reconciliation dance. Runs independently per symbol -- see
        module docstring."""
        params = {"symbol": symbol.upper(), "limit": DEPTH_SNAPSHOT_LIMIT}
        async with httpx.AsyncClient(base_url=REST_BASE_URL, timeout=10.0) as client:
            resp = await client.get("/api/v3/depth", params=params)
            resp.raise_for_status()
            return resp.json()
