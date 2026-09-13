"""Redis Streams consumer-group logic for the normalizer.

Stream-consumption split: **two independent consumer tasks in one process**,
one per Redis Stream (`raw:{exchange}:{symbol}:trades` and `...:depth`),
each running its own XREADGROUP loop against its own consumer group, rather
than a single XREAD across both streams. Rationale:
  - The two streams carry semantically unrelated payloads that feed
    different downstream state (trades -> ClickHouse only; depth ->
    ClickHouse *and* in-memory order-book state + sequence-break/incident
    detection + disconnect/reconnect signal handling). Keeping them on
    separate asyncio tasks means a slow/bursty depth stream (order book
    diffs arrive far more often than trades) can't starve trade processing
    behind a single blocking XREADGROUP call, and vice versa.
  - `redis.asyncio`'s consumer-group API (XREADGROUP/XACK) operates per
    stream key with its own group, so reading multiple streams in one call
    (XREADGROUP ... STREAMS trades depth) is supported, but you then still
    have to demux entries by which stream they came from before XACK'ing
    against the right stream name -- two independent loops are simpler and
    match "one task per concern" without any real downside at this message
    rate (single-symbol prototype).
  - Both tasks share one ClickHouseSink instance (batched, thread/task-safe
    via per-table asyncio.Lock) and one OrderBookState + incident-tracking
    context so depth-stream-detected incidents and trades share the same
    flush cadence.

Consumer-group recovery: a `create group if not exists` call with
`mkstream=True` and `id="0"` covers first-run bootstrap; XACK is only sent
after a message's row(s) are durably appended to the ClickHouse sink's
buffer (not necessarily flushed to ClickHouse yet -- see the trade-off note
below). On processor crash/restart, unacked (pending) entries are
redelivered via XREADGROUP's normal PEL mechanics; this prototype does not
implement XCLAIM/XAUTOCLAIM for entries stuck in *other* consumers' PELs
(single-consumer-per-group deployment, task scope) but notes it as the
natural next step for multi-replica normalizers.

Trade-off flagged: XACK happens after the row is buffered in the sink, not
after it's flushed to ClickHouse -- so a normalizer crash between ack and
flush can lose a buffered-but-unflushed batch (bounded by sink_batch_size /
sink_flush_interval_s). Acking only after a real ClickHouse flush would
require flushing per-message (defeats batching) or a more complex
ack-batch-on-flush scheme; given this is a single-instrument local
prototype (task scope explicitly says "keep it simple"), the small
at-most-once loss window on crash is accepted and called out here rather
than silently glossed over.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import redis.asyncio as redis

from common import get_logger
from normalizer import incidents, parser
from normalizer.clickhouse_sink import ClickHouseSink
from normalizer.orderbook_state import OrderBookState

log = get_logger("normalizer.consumer")


class StreamConsumer:
    """One XREADGROUP loop against a single Redis Stream + consumer group."""

    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        stream: str,
        group: str,
        consumer_name: str,
        block_ms: int,
        count: int,
        handler: Any,  # async def handler(fields: dict[str, str]) -> None
    ) -> None:
        self._redis = redis_client
        self._stream = stream
        self._group = group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._count = count
        self._handler = handler

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            log.debug("consumer group already exists", extra={"stream": self._stream, "group": self._group})

    async def run(self) -> None:
        await self.ensure_group()
        log.info("consumer started", extra={"stream": self._stream, "group": self._group})
        while True:
            try:
                response = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer_name,
                    streams={self._stream: ">"},
                    count=self._count,
                    block=self._block_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("xreadgroup failed, backing off", extra={"stream": self._stream})
                await asyncio.sleep(1.0)
                continue

            if not response:
                continue

            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    await self._process_one(entry_id, fields)

    async def _process_one(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            payload = json.loads(fields["payload"])
            await self._handler(payload)
        except Exception:
            # Do not crash the loop on a single bad message; log and still
            # ack so a persistently malformed message doesn't wedge the PEL
            # forever (best-effort, matches "keep it simple" task scope).
            log.exception(
                "failed to process message, acking to avoid poison-pill wedge",
                extra={"stream": self._stream, "entry_id": entry_id},
            )
        await self._redis.xack(self._stream, self._group, entry_id)


class NormalizerContext:
    """Shared state/handlers wired to both stream consumers: order-book
    tracking, incident open/resolve bookkeeping, and the ClickHouse sink."""

    def __init__(self, *, exchange: str, segment: str, symbol: str, sink: ClickHouseSink) -> None:
        self._exchange = exchange
        self._segment = segment
        self._symbol = symbol
        self._sink = sink
        self._book = OrderBookState(exchange=exchange, segment=segment, symbol=symbol)

        # Open incidents kept in memory until resolved (single-process
        # prototype scope -- a restart loses track of a still-open
        # incident's in-memory handle, but the ClickHouse row remains with
        # status='open' for manual/API follow-up).
        self._open_sequence_break: dict[str, Any] | None = None
        self._open_disconnect: dict[str, Any] | None = None

    async def handle_trade(self, envelope: dict[str, Any]) -> None:
        if envelope.get("type") != "trade":
            log.debug("unexpected type on trades stream", extra={"type": envelope.get("type")})
            return
        row = parser.parse_trade(envelope)
        await self._sink.add_trade(row)

    async def handle_depth(self, envelope: dict[str, Any]) -> None:
        kind = envelope.get("type")
        if kind == "depth_diff":
            await self._handle_depth_diff(envelope)
        elif kind == "snapshot":
            await self._handle_snapshot(envelope)
        elif kind == "disconnect":
            await self._handle_disconnect(envelope)
        elif kind == "reconnect":
            await self._handle_reconnect(envelope)
        else:
            log.debug("unclassified depth-stream message", extra={"type": kind})

    async def _handle_depth_diff(self, envelope: dict[str, Any]) -> None:
        row = parser.parse_depth_diff(envelope)
        # Persist the raw diff regardless of book-state continuity -- raw
        # diff history is always stored (docs §6.5), only in-memory book
        # reconstruction pauses on a break.
        await self._sink.add_orderbook_event(row)

        brk = self._book.apply_diff(row)
        if brk is not None and self._open_sequence_break is None:
            incident = incidents.open_sequence_break_incident(
                exchange=self._exchange,
                segment=self._segment,
                symbol=self._symbol,
                last_valid_update_id=brk.last_valid_update_id,
            )
            self._open_sequence_break = incident
            await self._sink.add_incident(incident)
            log.warning(
                "orderbook_sequence_break detected",
                extra={
                    "last_valid_update_id": brk.last_valid_update_id,
                    "received_U": brk.received_update_id_U,
                    "received_u": brk.received_update_id_u,
                },
            )

    async def _handle_snapshot(self, envelope: dict[str, Any]) -> None:
        row = parser.parse_snapshot(envelope)
        await self._sink.add_orderbook_snapshot(row)
        self._book.apply_snapshot(row)

        if self._open_sequence_break is not None:
            resolved = incidents.resolve_sequence_break_incident(
                self._open_sequence_break,
                resync_last_update_id=row["last_update_id"],
            )
            await self._sink.add_incident(resolved)
            self._open_sequence_break = None
            log.info("orderbook_sequence_break resolved via new snapshot", extra={"last_update_id": row["last_update_id"]})

    async def _handle_disconnect(self, envelope: dict[str, Any]) -> None:
        error = envelope.get("raw", {}).get("error")
        incident = incidents.open_disconnect_incident(
            exchange=self._exchange,
            segment=self._segment,
            symbol=self._symbol,
            error=error,
        )
        self._open_disconnect = incident
        await self._sink.add_incident(incident)
        log.warning("collector_disconnect detected", extra={"error": error})

    async def _handle_reconnect(self, envelope: dict[str, Any]) -> None:
        if self._open_disconnect is None:
            # Reconnect signal with no matching open disconnect (e.g.
            # normalizer restarted mid-incident) -- nothing to resolve.
            log.debug("reconnect signal with no tracked open disconnect incident")
            return
        resolved = incidents.resolve_disconnect_incident(
            self._open_disconnect,
            reconnect_attempts=1,  # see incidents.py docstring: collector emits exactly one signal per attempt
        )
        await self._sink.add_incident(resolved)
        self._open_disconnect = None
        log.info("collector_disconnect resolved", extra={"downtime_ms": resolved["details"]["downtime_ms"]})

    async def periodic_book_snapshot_loop(self, interval_s: float) -> None:
        """Every `interval_s`, if the book is ready, materialize its current
        state as an orderbook_snapshots row (docs §6.4 periodic snapshot)."""
        while True:
            await asyncio.sleep(interval_s)
            if self._book.ready and not self._book.awaiting_resync:
                row = self._book.to_snapshot_row()
                await self._sink.add_orderbook_snapshot(row)
                log.debug("periodic book snapshot emitted", extra={"last_update_id": row["last_update_id"]})
