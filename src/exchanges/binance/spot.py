"""Binance spot adapter: WS URL construction, REST snapshot fetch, and raw
message classification for BTCUSDT (or any other spot symbol).

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
    """Binance spot adapter for a single symbol, combined trade + depth-diff
    streams over one WS connection (binance.md: "one combined connection per
    (exchange, segment) pair" -- segment here is 'spot')."""

    exchange = "binance"
    segment = "spot"

    def __init__(self, symbol: str) -> None:
        # Binance stream names are lowercase; REST `symbol` query param is
        # case-insensitive but conventionally uppercase.
        self.symbol = symbol.upper()
        self._stream_symbol = symbol.lower()

    # -- WS -----------------------------------------------------------------

    def ws_url(self) -> str:
        """Combined-stream URL carrying both the trade and depth-diff streams.

        Combined-stream envelope shape: {"stream": "<name>", "data": {...}}.
        """
        streams = [
            f"{self._stream_symbol}@trade",
            f"{self._stream_symbol}@depth@100ms",
        ]
        return f"{WS_BASE_URL}/stream?streams={'/'.join(streams)}"

    def stream_type(self, raw_message: dict[str, Any]) -> str:
        """Classify a combined-stream envelope by its `stream` field."""
        stream = raw_message.get("stream", "")
        if stream.endswith("@trade"):
            return "trade"
        if "@depth" in stream:
            return "depth_diff"
        return "unknown"

    def depth_update_ids(self, raw_message: dict[str, Any]) -> tuple[int, int]:
        """Return (U, u) from a depth-diff combined-stream envelope."""
        data = raw_message["data"]
        return data["U"], data["u"]

    # -- REST -----------------------------------------------------------------

    async def fetch_snapshot(self) -> dict[str, Any]:
        """`GET /api/v3/depth?symbol=...&limit=1000` -- returns the raw JSON
        body untouched (bids/asks/lastUpdateId), per binance.md step 2 of the
        bootstrap/reconciliation dance."""
        params = {"symbol": self.symbol, "limit": DEPTH_SNAPSHOT_LIMIT}
        async with httpx.AsyncClient(base_url=REST_BASE_URL, timeout=10.0) as client:
            resp = await client.get("/api/v3/depth", params=params)
            resp.raise_for_status()
            return resp.json()
