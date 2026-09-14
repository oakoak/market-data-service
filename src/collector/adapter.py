"""Exchange-agnostic adapter contract.

The collector runner (runner.py) depends only on this small protocol, not on
anything Binance-specific. Adding Bybit later means writing a new class that
satisfies this protocol under src/exchanges/bybit/ -- no changes to the
runner's connection lifecycle / reconnect / Redis-write loop.

**Multi-connection contract (2026-09):** an adapter instance used to expose a
single `ws_url()` -- true for Binance spot, where trades + depth diffs share
one combined `/stream?streams=...` connection. That stopped being universal
once USDT-M perp split its WS surface into separate `/public` (book/ticker)
and `/market` (aggTrade/markPrice/kline) endpoints (see
docs/04-architecture/exchanges/binance.md) -- one adapter instance now needs
*multiple, independently-managed* WS connections. `ws_url()` is replaced by
`ws_urls()`, returning a `{connection_name: url}` mapping; the runner opens
and reconnects each entry as its own independent asyncio task (its own
backoff, its own disconnect/reconnect incident signal, its own
buffer-until-snapshot-ready state) -- see runner.py's module docstring.
`connection_name` is an opaque label chosen by the adapter (e.g. "combined",
"public", "market"); the runner only uses it for logging/incident payloads,
never to branch on exchange-specific logic.

Some data (Binance Open Interest) has no WS stream at all and is REST-poll
only. `rest_pollers()` lets an adapter declare periodic REST polls the
runner should schedule and publish to Redis the same way it publishes WS
messages -- see runner.py. Adapters with nothing to poll return `{}`.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol


class ExchangeAdapter(Protocol):
    """Everything the collector runner needs from a specific exchange/segment.

    An adapter instance is scoped to one (exchange, segment, symbol). It may
    open several concurrent WS connections (see `ws_urls()`) and/or declare
    periodic REST pollers (see `rest_pollers()`) -- both stay dumb-collector
    plumbing: URLs, opaque message classification, and raw REST bodies, never
    parsing into the unified schema (that's normalizer's job).
    """

    exchange: str
    segment: str
    symbol: str

    def ws_urls(self) -> dict[str, str]:
        """Named WS connection URLs this adapter needs open concurrently.

        Keys are opaque connection names used only for logging and incident
        payloads (e.g. `{"combined": "..."}` for Binance spot's single
        combined-stream connection, or `{"public": "...", "market": "..."}`
        for USDT-M perp's split scheme). The runner opens, reconnects, and
        backs off each entry independently -- one connection dropping must
        not interrupt the others.
        """
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
        Must not require parsing beyond the combined-stream envelope. Applies
        uniformly regardless of which named WS connection the message came
        from -- classification is driven by the message's own envelope, not
        by connection identity, so a connection that never produces
        'depth_diff' simply never triggers the snapshot-bootstrap dance."""
        ...

    def depth_update_ids(self, raw_message: dict[str, Any]) -> tuple[int, int]:
        """Return (U, u) -- first and final update ids -- for a depth-diff
        message, used by the runner to buffer-until-snapshot-ready and to
        detect gaps. Exchange-specific field names stay inside the adapter."""
        ...

    def rest_pollers(self) -> dict[str, tuple[Callable[[], Awaitable[Any]], float]]:
        """Optional periodic REST pollers, e.g. Binance Open Interest, which
        has no WS stream at all (docs/04-architecture/exchanges/binance.md:
        "the only mandatory REST polling in the whole pipeline"). Keys are
        opaque poller names (used as the published message's `type` and for
        logging); values are `(poll_fn, interval_seconds)`, where `poll_fn`
        is an async, zero-arg callable returning the raw REST JSON body
        untouched -- same dumb-collector contract as `fetch_snapshot()`. The
        runner schedules each entry on its own independent interval loop and
        publishes every result through the same `RedisStreamSink` used for
        WS messages. Adapters with nothing to poll return `{}` (e.g. Binance
        spot, which has no REST-only data)."""
        ...
