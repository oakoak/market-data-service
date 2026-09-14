"""Exchange-agnostic collector runner.

Owns: WS connection lifecycle, reconnect/backoff, the generic
buffer-until-snapshot-ready dance for depth-diff streams (driven through the
ExchangeAdapter protocol so the *exact* field names/URLs stay exchange
-specific), periodic REST polling, and writing every message to Redis
Streams.

**Multi-connection (2026-09):** an adapter can expose more than one named WS
connection (`ExchangeAdapter.ws_urls()`), e.g. Binance USDT-M perp's split
`/public` + `/market` scheme (docs/04-architecture/exchanges/binance.md).
Each named connection runs as its own independent asyncio task with its own
reconnect/backoff and its own disconnect/reconnect incident signal -- one
connection dropping must never interrupt the others. The buffer-until-
snapshot-ready dance is *per connection* too, but lazily: a connection only
starts it the moment it actually sees its first `depth_diff` message, so a
connection that never carries depth diffs (e.g. `/market`, which only ever
carries aggTrade/markPrice/kline) never fetches a REST snapshot at all. This
keeps the runner fully generic -- it never needs to know *which* named
connection is "the depth one", it reacts to message content as it arrives,
and the buffer-then-apply guarantee (nothing between WS-open and
snapshot-applied is lost) holds exactly as before: buffering starts at the
same instant the snapshot fetch is kicked off, before this first message is
even done being handled.

An adapter can also declare periodic REST-only pollers
(`ExchangeAdapter.rest_pollers()`, e.g. Binance Open Interest, which has no
WS stream at all). Each declared poller runs as its own independent
interval-loop task and publishes its raw result through the same
`RedisStreamSink` used for WS messages, tagged with the poller's own name as
`type` so downstream consumers can tell REST-poll results apart.

Explicitly NOT owned here (per task scope): sequence-break / gap detection,
incident records, unified-schema parsing -- all normalizer responsibilities.
This module only guarantees messages are not dropped or reordered before
they hit Redis, and that a `snapshot` message is emitted once the REST
snapshot dance completes so the normalizer can build book state.
"""

from __future__ import annotations

import asyncio
import json
import random
from typing import Any

import redis.exceptions
import websockets
from websockets.exceptions import ConnectionClosed

from collector.adapter import ExchangeAdapter
from collector.config import Config
from collector.sink import RedisStreamSink, now_ts_ms
from common import get_logger

log = get_logger("collector.runner")


class CollectorRunner:
    def __init__(self, config: Config, adapter: ExchangeAdapter, sink: RedisStreamSink) -> None:
        self._config = config
        self._adapter = adapter
        self._sink = sink
        self._trades_stream = f"{config.stream_prefix}:trades"
        self._depth_stream = f"{config.stream_prefix}:depth"
        # New stream for REST-poll results (Open Interest and similar) --
        # kept separate from trades/depth so an adapter with no pollers
        # doesn't need it, and so normalizer-side wiring for it is additive.
        self._poll_stream = f"{config.stream_prefix}:poll"
        # Fallback stream for messages whose stream_type() is falsy/"unknown"
        # -- i.e. genuinely unclassifiable, not just "a kind other than
        # depth_diff/trade". Every other classification gets its own stream
        # (see `_stream_for_kind`) so normalizer consumers can subscribe
        # per-kind without the runner needing to know exchange-specific
        # kind names.
        self._other_stream = f"{config.stream_prefix}:other"

    async def run(self) -> None:
        """Top-level: connect to Redis once, then run one independent task
        per named WS connection plus one per declared REST poller, all
        concurrently, for the lifetime of the process (each task handles its
        own reconnect/backoff internally -- see `_run_ws_connection` and
        `_run_rest_poller`). Process supervision itself (the *process* dying)
        is covered by systemd `Restart=always` per
        docs/04-architecture/01-stack.md, not by this loop."""
        await self._sink.connect()
        try:
            ws_urls = self._adapter.ws_urls()
            pollers = self._adapter.rest_pollers()
            if not ws_urls and not pollers:
                raise RuntimeError(
                    f"adapter for exchange={self._adapter.exchange!r} "
                    f"segment={self._adapter.segment!r} declares no ws_urls() "
                    "and no rest_pollers() -- nothing to run"
                )

            tasks = [
                asyncio.create_task(
                    self._run_ws_connection(name, url), name=f"ws:{name}"
                )
                for name, url in ws_urls.items()
            ]
            tasks += [
                asyncio.create_task(
                    self._run_rest_poller(name, poll_fn, interval_s), name=f"poll:{name}"
                )
                for name, (poll_fn, interval_s) in pollers.items()
            ]
            # Any one task raising (e.g. an unhandled bug, not a normal
            # reconnect -- those are caught inside each task's own loop)
            # should bring the whole process down rather than silently
            # running degraded; gather with default settings does that.
            await asyncio.gather(*tasks)
        finally:
            await self._sink.close()

    async def _run_ws_connection(self, name: str, url: str) -> None:
        """Connect, stream until disconnect, backoff, repeat -- forever, for
        one named WS connection. Independent backoff state per connection so
        e.g. `/public` reconnecting rapidly doesn't affect `/market`'s
        schedule."""
        backoff = self._config.backoff_initial_s
        recovering = False
        while True:
            try:
                await self._connect_and_stream(name, url, emit_reconnect_signal=recovering)
                # A clean return (shouldn't normally happen) still counts
                # as a disconnect from the runner's perspective.
                backoff = self._config.backoff_initial_s
            except (
                ConnectionClosed,
                OSError,
                asyncio.TimeoutError,
                redis.exceptions.RedisError,
            ) as exc:
                log.warning(
                    "ws disconnected, will reconnect",
                    extra={"connection": name, "error": str(exc), "backoff_s": backoff},
                )
                await self._emit_signal(name, "disconnect", {"error": str(exc)})
                recovering = True
                jitter = random.uniform(0, backoff * 0.1)
                await asyncio.sleep(backoff + jitter)
                backoff = min(backoff * 2, self._config.backoff_max_s)

    async def _run_rest_poller(
        self, name: str, poll_fn: Any, interval_s: float
    ) -> None:
        """Interval loop for one declared REST poller. A failed poll is
        logged and skipped (not retried with backoff -- the next scheduled
        tick is the retry) rather than crashing the process; REST-only data
        like Open Interest has no WS fallback to fall back to, so a
        transient failure just means one missed sample, not a process
        restart."""
        while True:
            try:
                result = await poll_fn()
                message = {
                    "type": name,
                    "receive_ts": now_ts_ms(),
                    "exchange": self._adapter.exchange,
                    "segment": self._adapter.segment,
                    "symbol": self._adapter.symbol,
                    "raw": result,
                }
                await self._sink.publish(self._poll_stream, message)
            except Exception:  # noqa: BLE001 - one poller's failure must not kill the others
                log.exception("rest poller failed", extra={"poller": name})
            await asyncio.sleep(interval_s)

    async def _emit_signal(self, connection: str, kind: str, details: dict[str, Any]) -> None:
        """Push a raw disconnect/reconnect signal event so the normalizer can
        create a `collector_disconnect` incident -- the collector itself does
        not compute incidents (docs/04-architecture/00-overview.md §6.3.1).
        `connection` (the named WS connection this signal came from) is
        included in `raw` so the normalizer can eventually disambiguate
        concurrent disconnects across multiple connections -- today's
        `NormalizerContext` still tracks a single open disconnect incident
        at a time (see memory/collector.md), that's a normalizer-side
        follow-up, not fixed here."""
        message = {
            "type": kind,
            "receive_ts": now_ts_ms(),
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": {**details, "connection": connection},
        }
        try:
            await self._sink.publish(self._depth_stream, message)
        except Exception:  # noqa: BLE001 - never let a signal-publish failure crash the loop
            log.exception("failed to publish %s signal", kind)

    async def _connect_and_stream(
        self, name: str, url: str, emit_reconnect_signal: bool = False
    ) -> None:
        log.info("connecting", extra={"connection": name, "url": url})

        # Depth-diff buffering state for the snapshot bootstrap/reconcile
        # dance (binance.md steps 1-4), scoped to this one connection and
        # this one connect attempt. Unlike the old single-connection runner,
        # the snapshot fetch is *not* kicked off unconditionally at connect
        # time -- it starts lazily on this connection's first depth_diff
        # message (see module docstring). Buffering starts at that exact
        # same instant, so the "nothing lost between WS-open and
        # snapshot-applied" guarantee still holds; a connection that never
        # sees a depth_diff (e.g. USDT-M perp's `/market`) never fetches a
        # snapshot at all.
        buffering = False
        buffer: list[dict[str, Any]] = []
        snapshot_task: asyncio.Task[dict[str, Any]] | None = None

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=10,
            # Bound the receive queue so a slow Redis publish side applies
            # backpressure (via TCP, once this fills) instead of letting
            # `websockets` buffer unread messages in memory without limit.
            max_queue=2048,
        ) as ws:
            log.info("connected", extra={"connection": name, "url": url})
            if emit_reconnect_signal:
                # Only now that the WS connection is actually back up do we
                # tell the normalizer we've reconnected -- emitting this
                # earlier (e.g. right before the connect attempt) would
                # resolve the `collector_disconnect` incident prematurely if
                # this very attempt goes on to fail too.
                await self._emit_signal(name, "reconnect", {"attempt": True})
            recv_task: asyncio.Task[Any] | None = None

            try:
                while True:
                    if recv_task is None:
                        recv_task = asyncio.create_task(ws.recv())

                    waitables = {recv_task}
                    if buffering and snapshot_task is not None:
                        waitables.add(snapshot_task)

                    done, _ = await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)

                    # Apply the snapshot as soon as it's ready, independent of
                    # whether a new WS message has arrived -- otherwise a
                    # quiet stream could leave it un-applied indefinitely.
                    if buffering and snapshot_task is not None and snapshot_task in done:
                        await self._apply_snapshot(name, snapshot_task, buffer)
                        buffering = False
                        buffer = []

                    if recv_task in done:
                        raw_text = recv_task.result()
                        recv_task = None
                        receive_ts = now_ts_ms()
                        try:
                            message = json.loads(raw_text)
                        except (json.JSONDecodeError, TypeError):
                            log.warning("non-JSON WS message, forwarding as opaque text")
                            message = {"_raw_text": raw_text}

                        kind = (
                            self._adapter.stream_type(message)
                            if isinstance(message, dict)
                            else "unknown"
                        )

                        # Every classification gets published -- the
                        # buffer-until-snapshot-ready dance is the ONLY
                        # thing special-cased to "depth_diff"; every other
                        # kind (including exchange-specific ones the runner
                        # has never heard of, e.g. "mark_price",
                        # "liquidation") is forwarded untouched to its own
                        # stream. A connection carrying a mix of kinds (e.g.
                        # USDT-M perp's "market" connection: aggTrade +
                        # markPrice + kline) must not let a non-depth_diff
                        # message affect the depth_diff buffering state.
                        if kind == "depth_diff" and snapshot_task is None:
                            # First depth_diff seen on this connection since
                            # (re)connect -- lazily start the
                            # bootstrap/reconcile dance now.
                            snapshot_task = asyncio.create_task(self._bootstrap_snapshot())
                            buffering = True
                        if kind == "depth_diff" and buffering:
                            # Never drop; every raw message still gets
                            # published even while we accumulate the
                            # pre-snapshot buffer, per the "pass every raw
                            # message through untouched" requirement.
                            buffer.append({"receive_ts": receive_ts, "message": message})

                        if not kind or kind == "unknown":
                            log.debug(
                                "unclassified message",
                                extra={
                                    "connection": name,
                                    "stream": message.get("stream")
                                    if isinstance(message, dict)
                                    else None,
                                },
                            )
                        await self._publish(kind, message, receive_ts)
            finally:
                if recv_task is not None:
                    recv_task.cancel()
                if snapshot_task is not None and not snapshot_task.done():
                    snapshot_task.cancel()

    async def _bootstrap_snapshot(self) -> dict[str, Any]:
        return await self._adapter.fetch_snapshot()

    async def _apply_snapshot(
        self,
        connection: str,
        snapshot_task: "asyncio.Task[dict[str, Any]]",
        buffer: list[dict[str, Any]],
    ) -> None:
        """Publish the REST snapshot as its own message type so the
        normalizer can build book state, per the "additionally push a
        `snapshot` type message" requirement. The discard-events-with-u<=
        lastUpdateId / continuity bookkeeping is the normalizer's job -- the
        collector has already published every buffered event untouched by
        this point, it just also tags the snapshot's arrival."""
        try:
            snapshot = snapshot_task.result()
        except Exception:  # noqa: BLE001
            log.exception("snapshot fetch failed; depth stream continues unsnapshotted for now")
            return

        message = {
            "type": "snapshot",
            "receive_ts": now_ts_ms(),
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": snapshot,
        }
        await self._sink.publish(self._depth_stream, message)
        log.info(
            "snapshot applied",
            extra={
                "connection": connection,
                "last_update_id": snapshot.get("lastUpdateId"),
                "buffered_during_fetch": len(buffer),
            },
        )

    def _stream_for_kind(self, kind: str) -> str:
        """Route a `stream_type()` classification to a Redis stream name.
        `depth_diff`/`trade` keep their existing dedicated stream names
        unchanged (the normalizer already consumes those two specifically,
        per config.py's `trades_stream`/`depth_stream` -- this must stay an
        additive generalization, not a rename). Every other kind gets its
        own `{stream_prefix}:{kind}` stream, keyed by whatever string the
        adapter's `stream_type()` returns -- the runner never needs to know
        what "mark_price" or "liquidation" mean, it just needs a stable
        per-kind stream name for the normalizer to subscribe to. A falsy or
        "unknown" kind (couldn't be classified at all) falls back to a
        shared `{stream_prefix}:other` stream instead of minting a stream
        literally named "unknown" or "None"."""
        if kind == "depth_diff":
            return self._depth_stream
        if kind == "trade":
            return self._trades_stream
        if not kind or kind == "unknown":
            return self._other_stream
        return f"{self._config.stream_prefix}:{kind}"

    async def _publish(self, kind: str, message: dict[str, Any], receive_ts: int) -> None:
        """Publish one raw WS message, tagged with its `stream_type()`
        classification, to the stream `_stream_for_kind(kind)` routes it to.
        Same envelope shape used everywhere else in this module -- dumb
        collector principle, no parsing beyond the classification string
        itself."""
        payload = {
            "type": kind,
            "receive_ts": receive_ts,
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": message,
        }
        await self._sink.publish(self._stream_for_kind(kind), payload)
