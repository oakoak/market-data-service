"""Exchange-agnostic adapter contract.

The collector runner (runner.py) depends only on this small protocol, not on
anything Binance-specific. Adding Bybit later means writing a new class that
satisfies this protocol under src/exchanges/bybit/ — no changes to the
runner's connection lifecycle / reconnect / Redis-write loop.
"""

from __future__ import annotations

from typing import Any, Protocol


class ExchangeAdapter(Protocol):
    """Everything the collector runner needs from a specific exchange/segment.

    An adapter instance is scoped to one (exchange, segment, symbol) —
    matching "one combined connection per (exchange, segment) pair" from
    docs/04-architecture/exchanges/binance.md; for this prototype that
    happens to be a single symbol too.
    """

    exchange: str
    segment: str
    symbol: str

    def ws_url(self) -> str:
        """Combined-stream WS URL for all channels this adapter subscribes to."""
        ...

    async def fetch_snapshot(self) -> dict[str, Any]:
        """Fetch the REST order-book snapshot used for the bootstrap/reconcile
        dance. Returns the exchange-native JSON body untouched (dumb collector
        principle -- no parsing into a unified schema here either, just enough
        field access to drive the snapshot-sync state machine, e.g.
        `lastUpdateId`)."""
        ...

    def stream_type(self, raw_message: dict[str, Any]) -> str:
        """Classify a raw WS message into a coarse type string used only for
        routing to the right Redis stream (e.g. 'trade' vs 'depth_diff').
        Must not require parsing beyond the combined-stream envelope."""
        ...

    def depth_update_ids(self, raw_message: dict[str, Any]) -> tuple[int, int]:
        """Return (U, u) -- first and final update ids -- for a depth-diff
        message, used by the runner to buffer-until-snapshot-ready and to
        detect gaps. Exchange-specific field names stay inside the adapter."""
        ...
