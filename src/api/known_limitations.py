"""Static list backing `GET /known-limitations` / the `list_known_limitations`
data (docs §6.3.1: "a static config/JSON is sufficient" -- no DB table).

Empty for now: this prototype is Binance spot BTCUSDT only and doesn't
collect `forceOrder`/liquidations or any derivatives data at all, so there's
nothing yet to declare as a known gap. Kept as its own module (rather than a
literal in queries.py) so a later pass can grow this list without touching
query logic.
"""

from __future__ import annotations

from typing import Any

KNOWN_LIMITATIONS: list[dict[str, Any]] = []
