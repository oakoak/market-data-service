"""Binance exchange adapter (spot only, for this prototype).

Exchange-specific concerns live here so they can be swapped out per
docs/04-architecture/00-overview.md §6.2 (adapter/plugin pattern): WS URL
construction, REST snapshot fetch, and message framing. The exchange-agnostic
collector runner (src/collector/) knows nothing about Binance's stream naming
or REST endpoints.
"""

from .spot import BinanceSpotAdapter

__all__ = ["BinanceSpotAdapter"]
