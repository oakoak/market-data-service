"""Binance USDT-M perpetual futures adapter: WS URL construction, REST
snapshot/poll fetch, and raw message classification for a single USDT-M
perp symbol (e.g. BTCUSDT, or the metals-as-perps XAUUSDT/XAGUSDT).

Per docs/04-architecture/exchanges/binance.md "Data -> source map" / USDT-M
perp row:
- Trades:      WS `{symbol}@aggTrade` (`/market`) -- perp has no `@trade`
               stream at all; `aggTrade` aggregates same-price/same-ts fills
               that land in the same 100ms window under one aggregate id
               (`a`), unlike spot's `@trade` which carries a genuine
               per-execution id (`t`). The normalizer must treat perp
               `trade_id` (`a`) as an aggregate-fill id, not a 1:1 execution
               id -- see spot.py's docstring for the contrast.
- Order book:  REST snapshot `GET /fapi/v1/depth` + WS `{symbol}@depth@100ms`
               (`/public`).
- Funding +
  mark/index
  price:       WS `{symbol}@markPrice@1s` (`/market`). Passed through
               untouched regardless of symbol -- XAUUSDT/XAGUSDT's
               freeze-outside-trading-hours behavior and 8h/narrower-cap
               funding are a normalizer-side `reference_price_freeze`
               incident-detection concern (binance.md), not an adapter-level
               branch.
- Open
  Interest:    REST-poll only, `GET /fapi/v1/openInterest` (weight 1) --
               binance.md: "the only mandatory REST polling in the perp
               pipeline". No WS stream exists for this at all.
- Liquidations: WS `{symbol}@forceOrder` (`/market`). binance.md: this is a
               best-largest-per-1000ms snapshot, not a full liquidation
               tape -- a known source limitation, not a collector bug.

Connection split (binance.md "Technical details", Sept 2026): USDT-M futures
WS migrated off the legacy combined `/stream` scheme (retired after
2026-04-23) onto three separate base paths -- `/public` (high-frequency
book/ticker), `/market` (aggTrade/markPrice/kline), `/private` (user data,
not used here -- no auth in this prototype). binance.md does not spell out
the literal host for the new paths; this adapter assumes they hang off the
same `fstream.binance.com` host that carried the legacy `/stream` endpoint,
with `/stream?streams=...` replaced by `/public?streams=...` and
`/market?streams=...` respectively (same combined-stream multi-plex query
param, just split by data category instead of unified) -- this is a
constructed-not-literally-documented assumption, flagged in binance.md and
memory/exchanges.md for the next person to verify against Binance's current
API docs.

Order book continuity (binance.md "Order book bootstrap + reconciliation",
step 5): futures continuity is `current.pu == previous.u` (via the `pu`
field), NOT `current.U == previous.u + 1` like spot. `depth_update_ids()`
still only returns `(U, u)` -- that's all the collector's buffer-until-
snapshot-ready state machine needs (collector/adapter.py). The `pu` field
itself is not read here and, per the dumb-collector/"pass raw messages
through untouched" rule, is never stripped from the raw message either --
the full depth-diff envelope (including `pu`) reaches Redis unmodified for
the normalizer to run the actual pu-based continuity check.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

import httpx

# Constructed per the /public + /market split described in binance.md (see
# module docstring for the "not literally spelled out" caveat) -- same host
# as the legacy fstream endpoint, new base paths.
WS_HOST = "wss://fstream.binance.com"
WS_PUBLIC_BASE_URL = f"{WS_HOST}/public"
WS_MARKET_BASE_URL = f"{WS_HOST}/market"

REST_BASE_URL = "https://fapi.binance.com"

# binance.md's REST rate-limit table: `depth` limit=1000 is the top end of
# the documented weight (20/request on the 2400/min USDS-M pool).
DEPTH_SNAPSHOT_LIMIT = 1000

# Open Interest poll interval. binance.md's REST budget section reasons
# about order-book resync "every 30-60s across ~30 symbols" using ~12% of
# the 2400/min USDS-M pool at weight 20/request, and says OI/funding should
# use "the same polling interval". openInterest is weight 1/request (20x
# cheaper than a depth snapshot), so polling it at the same cadence is
# trivially within budget -- picking the middle of that documented 30-60s
# window rather than the extreme ends.
OPEN_INTEREST_POLL_INTERVAL_S = 45.0


class BinanceUsdtmPerpAdapter:
    """Binance USDT-M perpetual futures adapter for a single symbol. Unlike
    spot (one combined connection), this adapter opens two independent named
    WS connections (`/public` for order-book depth, `/market` for
    aggTrade/markPrice/forceOrder) plus one REST poller (Open Interest, no WS
    stream exists) -- see collector/adapter.py's multi-connection contract.

    Symbol-parameterized like BinanceSpotAdapter; the full 14-symbol USDT-M
    instrument list from binance.md (including XAUUSDT/XAGUSDT) is a
    collector-config-level concern (one adapter instance per symbol,
    presumably one process per symbol per docs/04-architecture/01-stack.md),
    not something this class hardcodes -- flagged as out of scope, not
    implemented here.
    """

    exchange = "binance"
    segment = "usdtm"

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self._stream_symbol = symbol.lower()

    # -- WS -------------------------------------------------------------

    def ws_urls(self) -> dict[str, str]:
        """Two named connections: "public" carries only the depth-diff
        stream (so the runner's buffer-until-snapshot-ready dance triggers
        on it and only it); "market" carries aggTrade + markPrice +
        forceOrder, none of which are depth diffs, so that connection never
        triggers a snapshot fetch (runner.py module docstring).

        Combined-stream envelope shape on both connections:
        {"stream": "<name>", "data": {...}}.
        """
        public_streams = [f"{self._stream_symbol}@depth@100ms"]
        market_streams = [
            f"{self._stream_symbol}@aggTrade",
            f"{self._stream_symbol}@markPrice@1s",
            f"{self._stream_symbol}@forceOrder",
        ]
        return {
            "public": f"{WS_PUBLIC_BASE_URL}?streams={'/'.join(public_streams)}",
            "market": f"{WS_MARKET_BASE_URL}?streams={'/'.join(market_streams)}",
        }

    def rest_pollers(self) -> dict[str, tuple[Callable[[], Awaitable[Any]], float]]:
        """Open Interest has no WS stream (binance.md) -- declare it as a
        REST poller. See OPEN_INTEREST_POLL_INTERVAL_S above for the
        interval reasoning."""
        return {"open_interest": (self._poll_open_interest, OPEN_INTEREST_POLL_INTERVAL_S)}

    def stream_type(self, raw_message: dict[str, Any]) -> str:
        """Classify a combined-stream envelope by its `stream` field.

        The runner (src/collector/runner.py) routes every classification
        generically via `_stream_for_kind()`: "depth_diff"/"trade" keep
        their existing dedicated streams, and any other kind -- including
        "mark_price"/"liquidation" below -- gets its own
        `{stream_prefix}:{kind}` stream. This adapter never needs to
        special-case the runner; it just returns the semantically correct
        classification string.
        """
        stream = raw_message.get("stream", "")
        if stream.endswith("@aggTrade"):
            return "trade"
        if "@depth" in stream:
            return "depth_diff"
        if "@markPrice" in stream:
            return "mark_price"
        if stream.endswith("@forceOrder"):
            return "liquidation"
        return "unknown"

    def depth_update_ids(self, raw_message: dict[str, Any]) -> tuple[int, int]:
        """Return (U, u) from a depth-diff combined-stream envelope. The
        `pu` field (needed for futures' pu-based continuity check, per
        binance.md step 5) lives alongside U/u in the same `data` object and
        is left untouched here -- the collector passes the whole raw message
        through, the normalizer reads `pu` itself."""
        data = raw_message["data"]
        return data["U"], data["u"]

    # -- REST -------------------------------------------------------------

    async def fetch_snapshot(self) -> dict[str, Any]:
        """`GET /fapi/v1/depth?symbol=...&limit=1000` -- raw JSON body
        untouched (bids/asks/lastUpdateId), per binance.md step 2."""
        params = {"symbol": self.symbol, "limit": DEPTH_SNAPSHOT_LIMIT}
        async with httpx.AsyncClient(base_url=REST_BASE_URL, timeout=10.0) as client:
            resp = await client.get("/fapi/v1/depth", params=params)
            resp.raise_for_status()
            return resp.json()

    async def _poll_open_interest(self) -> Any:
        """`GET /fapi/v1/openInterest?symbol=...` (weight 1) -- raw JSON
        body untouched, published by the runner to `{stream_prefix}:poll`
        with `type` = "open_interest" (collector/adapter.py rest_pollers()
        contract)."""
        params = {"symbol": self.symbol}
        async with httpx.AsyncClient(base_url=REST_BASE_URL, timeout=10.0) as client:
            resp = await client.get("/fapi/v1/openInterest", params=params)
            resp.raise_for_status()
            return resp.json()
