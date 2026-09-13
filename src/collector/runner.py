"""Exchange-agnostic collector runner.

Owns: WS connection lifecycle, reconnect/backoff, the generic
buffer-until-snapshot-ready dance for depth-diff streams (driven through the
ExchangeAdapter protocol so the *exact* field names/URLs stay exchange
-specific), and writing every message to Redis Streams.

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

    async def run(self) -> None:
        """Top-level loop: connect, stream until disconnect, backoff, repeat.
        Runs forever (process is expected to be supervised -- see
        docs/04-architecture/01-stack.md's systemd Restart=always note, which
        covers the *process* dying; this loop covers the *WS connection*
        dropping while the process stays up)."""
        await self._sink.connect()
        backoff = self._config.backoff_initial_s
        first_attempt = True
        try:
            while True:
                if not first_attempt:
                    await self._emit_signal("reconnect", {"attempt": True})
                first_attempt = False
                try:
                    await self._connect_and_stream()
                    # A clean return (shouldn't normally happen) still counts
                    # as a disconnect from the runner's perspective.
                    backoff = self._config.backoff_initial_s
                except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                    log.warning(
                        "ws disconnected, will reconnect",
                        extra={"error": str(exc), "backoff_s": backoff},
                    )
                    await self._emit_signal("disconnect", {"error": str(exc)})
                    jitter = random.uniform(0, backoff * 0.1)
                    await asyncio.sleep(backoff + jitter)
                    backoff = min(backoff * 2, self._config.backoff_max_s)
        finally:
            await self._sink.close()

    async def _emit_signal(self, kind: str, details: dict[str, Any]) -> None:
        """Push a raw disconnect/reconnect signal event so the normalizer can
        create a `collector_disconnect` incident -- the collector itself does
        not compute incidents (docs/04-architecture/00-overview.md §6.3.1)."""
        message = {
            "type": kind,
            "receive_ts": now_ts_ms(),
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": details,
        }
        try:
            await self._sink.publish(self._depth_stream, message)
        except Exception:  # noqa: BLE001 - never let a signal-publish failure crash the loop
            log.exception("failed to publish %s signal", kind)

    async def _connect_and_stream(self) -> None:
        url = self._adapter.ws_url()
        log.info("connecting", extra={"url": url})

        # Depth-diff buffering state for the snapshot bootstrap/reconcile
        # dance (binance.md steps 1-4): buffer every depth-diff message from
        # the moment the WS is open, fetch the REST snapshot concurrently,
        # then discard/validate against lastUpdateId once it arrives.
        buffering = True
        buffer: list[dict[str, Any]] = []
        snapshot_task: asyncio.Task[dict[str, Any]] | None = None

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=10,
            max_queue=None,
        ) as ws:
            log.info("connected", extra={"url": url})
            snapshot_task = asyncio.create_task(self._bootstrap_snapshot())
            recv_task: asyncio.Task[Any] | None = None

            try:
                while True:
                    if recv_task is None:
                        recv_task = asyncio.create_task(ws.recv())

                    waitables = {recv_task}
                    if buffering:
                        waitables.add(snapshot_task)

                    done, _ = await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)

                    # Apply the snapshot as soon as it's ready, independent of
                    # whether a new WS message has arrived -- otherwise a
                    # quiet stream could leave it un-applied indefinitely.
                    if buffering and snapshot_task in done:
                        await self._apply_snapshot(snapshot_task, buffer)
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

                        if kind == "depth_diff" and buffering:
                            # Never drop; every raw message still gets
                            # published even while we accumulate the
                            # pre-snapshot buffer, per the "pass every raw
                            # message through untouched" requirement.
                            buffer.append({"receive_ts": receive_ts, "message": message})
                            await self._publish_depth(message, receive_ts)
                        elif kind == "depth_diff":
                            await self._publish_depth(message, receive_ts)
                        elif kind == "trade":
                            await self._publish_trade(message, receive_ts)
                        else:
                            log.debug(
                                "unclassified message",
                                extra={
                                    "stream": message.get("stream")
                                    if isinstance(message, dict)
                                    else None
                                },
                            )
            finally:
                if recv_task is not None:
                    recv_task.cancel()
                if not snapshot_task.done():
                    snapshot_task.cancel()

    async def _bootstrap_snapshot(self) -> dict[str, Any]:
        return await self._adapter.fetch_snapshot()

    async def _apply_snapshot(
        self, snapshot_task: "asyncio.Task[dict[str, Any]]", buffer: list[dict[str, Any]]
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
            extra={"last_update_id": snapshot.get("lastUpdateId"), "buffered_during_fetch": len(buffer)},
        )

    async def _publish_depth(self, message: dict[str, Any], receive_ts: int) -> None:
        payload = {
            "type": "depth_diff",
            "receive_ts": receive_ts,
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": message,
        }
        await self._sink.publish(self._depth_stream, payload)

    async def _publish_trade(self, message: dict[str, Any], receive_ts: int) -> None:
        payload = {
            "type": "trade",
            "receive_ts": receive_ts,
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": self._adapter.symbol,
            "raw": message,
        }
        await self._sink.publish(self._trades_stream, payload)
