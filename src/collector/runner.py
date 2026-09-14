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

**Multi-symbol (2026-09 Phase A, docs/08-prototype-roadmap.md):** a single
named connection can now also carry messages for *every* symbol an adapter
was configured with (`ExchangeAdapter.symbols`), interleaved -- Binance
spot's combined connection is the case in point: one `/stream?streams=...`
socket multiplexes `{symbol}@trade` + `{symbol}@depth@100ms` for the whole
symbol list, not just one symbol. The buffer-until-snapshot-ready dance
therefore has to be keyed **per (connection, symbol)**, not just per
connection: BTCUSDT's first depth_diff on the combined connection kicks off
BTCUSDT's own REST snapshot fetch and buffers only BTCUSDT's depth diffs
while it's in flight; ETHUSDT's first depth_diff on that same connection
does the exact same dance completely independently, concurrently, with its
own buffer and its own snapshot task. Which symbol an incoming message
belongs to is resolved via `ExchangeAdapter.symbol_of()`; per-symbol state
lives in a local `dict[str, _SymbolBufferState]` inside `_connect_and_stream`
(see that method), so it's naturally re-initialized fresh on every
reconnect, same as the old connection-scoped scalars were. Stream names
follow the same per-symbol split: `Config.stream_prefix()` now takes the
symbol explicitly, so this module never precomputes a single
`self._trades_stream`/`self._depth_stream`/etc. in `__init__` any more --
see `_stream_for()`.

An adapter can also declare periodic REST-only pollers
(`ExchangeAdapter.rest_pollers()`, e.g. Binance Open Interest, which has no
WS stream at all). Each declared poller runs as its own independent
interval-loop task and publishes its raw result through the same
`RedisStreamSink` used for WS messages, tagged with the poller's own name as
`type` so downstream consumers can tell REST-poll results apart. A poller
result isn't inherently "about" one symbol out of a multi-symbol adapter's
list (see `_run_rest_poller` for how that ambiguity is resolved today).

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
from dataclasses import dataclass, field
from typing import Any

import redis.exceptions
import websockets
from websockets.exceptions import ConnectionClosed

from collector.adapter import ExchangeAdapter
from collector.config import Config
from collector.sink import RedisStreamSink, now_ts_ms
from common import get_logger

log = get_logger("collector.runner")


@dataclass
class _SymbolBufferState:
    """Depth-diff buffer-until-snapshot-ready state for one symbol on one
    connection, for the lifetime of one connect attempt (scoped inside
    `_connect_and_stream`, so it's discarded and rebuilt fresh on every
    reconnect -- same lifetime the old connection-scoped scalars had, just
    now one instance per symbol instead of one set of scalars per
    connection). `snapshot_task` staying non-None after the snapshot is
    applied (see `_connect_and_stream`) is what prevents a second snapshot
    fetch for the same symbol on the same connect attempt -- mirrors the old
    single-scalar behavior exactly, just keyed by symbol now."""

    buffering: bool = False
    buffer: list[dict[str, Any]] = field(default_factory=list)
    snapshot_task: "asyncio.Task[dict[str, Any]] | None" = None


class CollectorRunner:
    def __init__(self, config: Config, adapter: ExchangeAdapter, sink: RedisStreamSink) -> None:
        self._config = config
        self._adapter = adapter
        self._sink = sink
        # No precomputed stream names here any more: `Config.stream_prefix()`
        # is now symbol-parameterized (Phase A), and since one connection can
        # carry many symbols' messages interleaved, the right stream name
        # can only be known once a given message's symbol has been resolved.
        # See `_stream_for()`, computed per message instead.

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
                # A poller result isn't inherently "about" one symbol out of
                # a multi-symbol adapter's `config.symbols` list -- neither
                # `ExchangeAdapter.rest_pollers()` nor a poll_fn's return
                # value tells the runner which symbol(s) a given result
                # covers, and generalizing that is out of scope here.
                # Flagging rather than silently guessing further: today this
                # is a non-issue in practice, since the only adapter with
                # pollers (USDT-M perp) is still strictly single-symbol
                # (`config.symbols` always has exactly one entry there) and
                # the only multi-symbol adapter (spot) declares no pollers
                # at all. `config.symbols[0]` is therefore always correct
                # today; a genuinely multi-symbol poller adapter would need
                # `rest_pollers()` to say which symbol(s) each poller covers.
                symbol = self._config.symbols[0]
                message = {
                    "type": name,
                    "receive_ts": now_ts_ms(),
                    "exchange": self._adapter.exchange,
                    "segment": self._adapter.segment,
                    "symbol": symbol,
                    "raw": result,
                }
                await self._sink.publish(f"{self._config.stream_prefix(symbol)}:poll", message)
            except Exception:  # noqa: BLE001 - one poller's failure must not kill the others
                log.exception("rest poller failed", extra={"poller": name})
            await asyncio.sleep(interval_s)

    async def _emit_signal(self, connection: str, kind: str, details: dict[str, Any]) -> None:
        """Push a raw disconnect/reconnect signal event so the normalizer can
        create a `collector_disconnect` incident -- the collector itself does
        not compute incidents (docs/04-architecture/00-overview.md section 6.3.1).
        `connection` (the named WS connection this signal came from) is
        included in `raw` so the normalizer can eventually disambiguate
        concurrent disconnects across multiple connections -- today's
        `NormalizerContext` still tracks a single open disconnect incident
        at a time (see memory/collector.md), that's a normalizer-side
        follow-up, not fixed here.

        Same symbol ambiguity as `_run_rest_poller` above: a disconnect/
        reconnect is a per-connection connectivity event, not inherently
        about one symbol, but the payload shape still carries a `symbol`
        field and needs one concrete stream to land on. Use
        `config.symbols[0]` as a defensible default -- neither adapter that
        exists today needs anything more precise (USDT-M perp is
        single-symbol; spot's combined connection has no pollers/signals
        ambiguity in practice since this path only fires on WS
        disconnect/reconnect, not per-message). Flagging rather than
        silently engineering a fuller per-symbol signal scheme."""
        symbol = self._config.symbols[0]
        message = {
            "type": kind,
            "receive_ts": now_ts_ms(),
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": symbol,
            "raw": {**details, "connection": connection},
        }
        try:
            await self._sink.publish(self._stream_for(symbol, "depth_diff"), message)
        except Exception:  # noqa: BLE001 - never let a signal-publish failure crash the loop
            log.exception("failed to publish %s signal", kind)

    async def _connect_and_stream(
        self, name: str, url: str, emit_reconnect_signal: bool = False
    ) -> None:
        log.info("connecting", extra={"connection": name, "url": url})

        # Depth-diff buffering state for the snapshot bootstrap/reconcile
        # dance (binance.md steps 1-4), scoped to this one connection and
        # this one connect attempt -- and, since Phase A, keyed *per symbol*
        # within that: `symbol_states[symbol]` holds that symbol's own
        # buffering flag, buffer, and snapshot task, completely independent
        # of every other symbol multiplexed over this same connection. The
        # snapshot fetch for a given symbol is not kicked off unconditionally
        # at connect time -- it starts lazily the moment *that symbol's*
        # first depth_diff message arrives (see module docstring). Buffering
        # for that symbol starts at that exact same instant, so the "nothing
        # lost between WS-open and snapshot-applied" guarantee still holds
        # per symbol; a symbol that never appears in a depth_diff on this
        # connection (shouldn't happen for symbols actually in
        # `self._adapter.symbols`, but e.g. USDT-M perp's `/market`
        # connection never carries depth diffs for its one symbol at all)
        # never fetches a snapshot.
        symbol_states: dict[str, _SymbolBufferState] = {}

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

                    # Wait on the recv task *and* every symbol's currently-
                    # pending snapshot task on this connection -- with
                    # several symbols multiplexed over one connection,
                    # multiple snapshot fetches can legitimately be in
                    # flight at once (e.g. BTCUSDT's and ETHUSDT's both
                    # kicked off moments apart), each needing to be applied
                    # the instant it completes regardless of whether a new
                    # WS message happens to arrive at the same time.
                    waitables: set[asyncio.Task[Any]] = {recv_task}
                    for state in symbol_states.values():
                        if state.buffering and state.snapshot_task is not None:
                            waitables.add(state.snapshot_task)

                    done, _ = await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)

                    # Apply any symbol's snapshot as soon as it's ready,
                    # independent of whether a new WS message has arrived --
                    # otherwise a quiet stream could leave it un-applied
                    # indefinitely. Iterate over a snapshot of the items
                    # since nothing here mutates symbol_states' keys, only
                    # the per-symbol state objects' fields.
                    for symbol, state in symbol_states.items():
                        if (
                            state.buffering
                            and state.snapshot_task is not None
                            and state.snapshot_task in done
                        ):
                            await self._apply_snapshot(name, symbol, state.snapshot_task, state.buffer)
                            state.buffering = False
                            state.buffer = []
                            # state.snapshot_task is intentionally left set
                            # (not reset to None) -- it's what prevents a
                            # second snapshot fetch for this symbol on this
                            # same connect attempt, mirroring the old
                            # connection-scoped `snapshot_task is None` gate.

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

                        # Resolve which symbol this message belongs to via
                        # the adapter's own envelope parsing -- only
                        # meaningful for a dict message that actually came
                        # through the exchange's combined-stream envelope.
                        # The non-JSON `_raw_text` fallback above has no
                        # `stream`/envelope field for `symbol_of()` to parse,
                        # so it can't be attributed to a real symbol; fall
                        # back to this connection's first configured symbol
                        # so the message still lands on *a* well-defined
                        # stream instead of being dropped (dumb-collector
                        # "never drop a raw message" rule still applies to
                        # this rare, genuinely-unparseable case).
                        if isinstance(message, dict) and "_raw_text" not in message:
                            symbol = self._adapter.symbol_of(message)
                        else:
                            symbol = self._adapter.symbols[0]

                        state = symbol_states.setdefault(symbol, _SymbolBufferState())

                        # Every classification gets published -- the
                        # buffer-until-snapshot-ready dance is the ONLY
                        # thing special-cased to "depth_diff"; every other
                        # kind (including exchange-specific ones the runner
                        # has never heard of, e.g. "mark_price",
                        # "liquidation") is forwarded untouched to its own
                        # stream. A connection carrying a mix of kinds and
                        # symbols (e.g. spot's combined connection: BTCUSDT
                        # trades, BTCUSDT depth diffs, ETHUSDT trades,
                        # ETHUSDT depth diffs, all interleaved) must not let
                        # a non-depth_diff message, or another symbol's
                        # depth_diff, affect this symbol's buffering state.
                        if kind == "depth_diff" and state.snapshot_task is None:
                            # First depth_diff seen for *this symbol* on
                            # this connection since (re)connect -- lazily
                            # start that symbol's bootstrap/reconcile dance
                            # now.
                            state.snapshot_task = asyncio.create_task(
                                self._bootstrap_snapshot(symbol)
                            )
                            state.buffering = True
                        if kind == "depth_diff" and state.buffering:
                            # Never drop; every raw message still gets
                            # published even while we accumulate this
                            # symbol's pre-snapshot buffer, per the "pass
                            # every raw message through untouched"
                            # requirement.
                            state.buffer.append({"receive_ts": receive_ts, "message": message})

                        if not kind or kind == "unknown":
                            log.debug(
                                "unclassified message",
                                extra={
                                    "connection": name,
                                    "symbol": symbol,
                                    "stream": message.get("stream")
                                    if isinstance(message, dict)
                                    else None,
                                },
                            )
                        await self._publish(symbol, kind, message, receive_ts)
            finally:
                if recv_task is not None:
                    recv_task.cancel()
                for state in symbol_states.values():
                    if state.snapshot_task is not None and not state.snapshot_task.done():
                        state.snapshot_task.cancel()

    async def _bootstrap_snapshot(self, symbol: str) -> dict[str, Any]:
        return await self._adapter.fetch_snapshot(symbol)

    async def _apply_snapshot(
        self,
        connection: str,
        symbol: str,
        snapshot_task: "asyncio.Task[dict[str, Any]]",
        buffer: list[dict[str, Any]],
    ) -> None:
        """Publish the REST snapshot as its own message type so the
        normalizer can build book state, per the "additionally push a
        `snapshot` type message" requirement. The discard-events-with-u<=
        lastUpdateId / continuity bookkeeping is the normalizer's job -- the
        collector has already published every buffered event untouched by
        this point, it just also tags the snapshot's arrival. Scoped to one
        symbol now, not the whole connection -- each symbol multiplexed on a
        connection gets its own `snapshot` message on its own depth stream
        the moment *its* snapshot fetch completes."""
        try:
            snapshot = snapshot_task.result()
        except Exception:  # noqa: BLE001
            log.exception(
                "snapshot fetch failed; depth stream continues unsnapshotted for now",
                extra={"connection": connection, "symbol": symbol},
            )
            return

        message = {
            "type": "snapshot",
            "receive_ts": now_ts_ms(),
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": symbol,
            "raw": snapshot,
        }
        await self._sink.publish(self._stream_for(symbol, "depth_diff"), message)
        log.info(
            "snapshot applied",
            extra={
                "connection": connection,
                "symbol": symbol,
                "last_update_id": snapshot.get("lastUpdateId"),
                "buffered_during_fetch": len(buffer),
            },
        )

    def _stream_for(self, symbol: str, kind: str) -> str:
        """Route a `stream_type()` classification, for a specific symbol, to
        a Redis stream name. Same routing rules as before Phase A
        (`depth_diff`/`trade` keep their existing dedicated stream suffixes
        -- the normalizer already consumes those two specifically, per
        `normalizer/config.py`'s `trades_stream`/`depth_stream` -- every
        other kind gets its own `:{kind}` stream, and a falsy/"unknown" kind
        falls back to a shared `:other` stream instead of minting a stream
        literally named "unknown" or "None"), just now built from
        `config.stream_prefix(symbol)` instead of a single precomputed
        connection-scoped prefix -- one connection can carry many symbols,
        so the stream name can only be resolved once the message's symbol is
        known (see `_connect_and_stream`)."""
        prefix = self._config.stream_prefix(symbol)
        if kind == "depth_diff":
            return f"{prefix}:depth"
        if kind == "trade":
            return f"{prefix}:trades"
        if not kind or kind == "unknown":
            return f"{prefix}:other"
        return f"{prefix}:{kind}"

    async def _publish(self, symbol: str, kind: str, message: dict[str, Any], receive_ts: int) -> None:
        """Publish one raw WS message, tagged with its `stream_type()`
        classification and its resolved symbol, to the stream
        `_stream_for(symbol, kind)` routes it to. Same envelope shape used
        everywhere else in this module -- dumb collector principle, no
        parsing beyond the classification string and the symbol lookup
        already done by the caller."""
        payload = {
            "type": kind,
            "receive_ts": receive_ts,
            "exchange": self._adapter.exchange,
            "segment": self._adapter.segment,
            "symbol": symbol,
            "raw": message,
        }
        await self._sink.publish(self._stream_for(symbol, kind), payload)
