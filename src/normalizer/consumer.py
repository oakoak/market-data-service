"""Redis Streams consumer-group logic for the normalizer.

Stream-consumption split: **one independent consumer task per Redis
Stream** (`raw:{exchange}:{segment}:{symbol}:trades`, `...:depth`, and, from Phase B
(docs/08-prototype-roadmap.md) onward, `...:mark_price`, `...:liquidation`,
`...:poll`), each running its own XREADGROUP loop against its own consumer
group, rather than a single XREAD across all of them. Rationale (unchanged
from the spot-only prototype, now applies to five streams instead of two):
  - Streams carry semantically unrelated payloads that feed different
    downstream state (trades/mark_price/liquidations/poll -> ClickHouse
    only; depth -> ClickHouse *and* in-memory order-book state +
    sequence-break/incident detection + disconnect/reconnect signal
    handling). Keeping them on separate asyncio tasks means a slow/bursty
    stream can't starve another behind a single blocking XREADGROUP call.
  - `redis.asyncio`'s consumer-group API operates per stream key with its
    own group; reading multiple streams in one XREADGROUP call is
    supported but then requires demuxing by stream name before XACK --
    independent loops are simpler and match "one task per concern".
  - All tasks share one ClickHouseSink instance (batched, thread/task-safe
    via per-table asyncio.Lock) and one NormalizerContext (order book +
    incident-tracking state) so every stream's detected incidents and rows
    share the same flush cadence.

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

**Phase B fix (multi-connection disconnect tracking):** the spot-only
prototype tracked exactly one open `collector_disconnect` incident at a
time (`self._open_disconnect`, a scalar). Per memory/collector.md /
memory/exchanges.md, the collector's runner now supports multiple
independently-managed named WS connections per adapter (e.g. USDT-M perp's
`public`/`market` split), each emitting its own disconnect/reconnect signal
tagged with `raw.connection` -- two connections can be down at once, or one
can drop while the other is fine. `NormalizerContext` now keys open
disconnect incidents by connection name (`self._open_disconnects: dict[str,
dict]`) instead of a single scalar, so `public` and `market` disconnecting
independently no longer clobber each other's incident bookkeeping.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

import redis.asyncio as redis

from common import get_logger
from normalizer import incidents, parser
from normalizer.clickhouse_sink import ClickHouseSink
from normalizer.orderbook_state import OrderBookState

log = get_logger("normalizer.consumer")

# Default connection name for adapters that don't tag their signals with a
# named connection (shouldn't happen post the collector's multi-connection
# generalization, but guards against an older/malformed signal payload
# rather than crashing on a missing key).
_UNKNOWN_CONNECTION = "unknown"


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
    """Shared state/handlers wired to all stream consumers: order-book
    tracking, incident open/resolve bookkeeping, and the ClickHouse sink.

    `continuity`: which depth-diff sequencing rule OrderBookState enforces
    for this deployment -- "spot" (U==prev.u+1) or "futures" (pu==prev.u,
    binance.md step 5). Threaded from config (see main.py): a normalizer
    process is scoped to one (exchange, segment, symbol), so this is a
    single fixed choice for the process's lifetime, not a per-message
    branch -- keeps OrderBookState itself free of Binance-specific segment
    string matching (see orderbook_state.py module docstring).
    """

    def __init__(
        self,
        *,
        exchange: str,
        segment: str,
        symbol: str,
        sink: ClickHouseSink,
        continuity: Literal["spot", "futures"] = "spot",
        freeze_unchanged_threshold: int = incidents.FREEZE_UNCHANGED_THRESHOLD_DEFAULT,
    ) -> None:
        self._exchange = exchange
        self._segment = segment
        self._symbol = symbol
        self._sink = sink
        self._book = OrderBookState(exchange=exchange, segment=segment, symbol=symbol, continuity=continuity)
        self._freeze_unchanged_threshold = freeze_unchanged_threshold

        # Open incidents kept in memory until resolved (single-process
        # prototype scope -- a restart loses track of a still-open
        # incident's in-memory handle, but the ClickHouse row remains with
        # status='open' for manual/API follow-up).
        self._open_sequence_break: dict[str, Any] | None = None
        # Keyed by named WS connection (e.g. "combined" for spot,
        # "public"/"market" for USDT-M perp) -- see module docstring
        # "Phase B fix" above. Two connections disconnecting independently
        # each get their own tracked incident.
        self._open_disconnects: dict[str, dict[str, Any]] = {}

        # reference_price_freeze detector state (XAUUSDT/XAGUSDT only, see
        # incidents.py module docstring for the heuristic's caveats). This
        # normalizer instance is scoped to a single symbol, so a single
        # scalar pair of (last_value, unchanged_count) is sufficient -- no
        # per-symbol dict needed.
        self._mark_price_last_value: float | None = None
        self._mark_price_unchanged_count: int = 0
        self._open_price_freeze: dict[str, Any] | None = None

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

    async def handle_mark_price(self, envelope: dict[str, Any]) -> None:
        if envelope.get("type") != "mark_price":
            log.debug("unexpected type on mark_price stream", extra={"type": envelope.get("type")})
            return
        row = parser.parse_mark_price(envelope)
        await self._sink.add_mark_price(row)
        await self._check_reference_price_freeze(row)

    async def handle_liquidation(self, envelope: dict[str, Any]) -> None:
        if envelope.get("type") != "liquidation":
            log.debug("unexpected type on liquidation stream", extra={"type": envelope.get("type")})
            return
        row = parser.parse_liquidation(envelope)
        await self._sink.add_liquidation(row)

    async def handle_poll(self, envelope: dict[str, Any]) -> None:
        """Handler for the `{stream_prefix}:poll` stream (collector's
        rest_pollers() results, tagged with `type` = poller name --
        src/collector/runner.py `_run_rest_poller`). Only "open_interest" is
        implemented today (the only declared poller, per
        src/exchanges/binance/usdtm.py); an unrecognized poller name is
        logged and skipped rather than crashing, so a future poller can be
        added collector-side without this handler needing to reject it
        first.
        """
        kind = envelope.get("type")
        if kind == "open_interest":
            row = parser.parse_open_interest(envelope)
            await self._sink.add_open_interest(row)
        else:
            log.debug("unrecognized poll stream type", extra={"type": kind})

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
                continuity_mode=brk.continuity_mode,
                received_prev_update_id=brk.received_prev_update_id,
            )
            self._open_sequence_break = incident
            await self._sink.add_incident(incident)
            log.warning(
                "orderbook_sequence_break detected",
                extra={
                    "continuity_mode": brk.continuity_mode,
                    "last_valid_update_id": brk.last_valid_update_id,
                    "received_U": brk.received_update_id_U,
                    "received_u": brk.received_update_id_u,
                    "received_pu": brk.received_prev_update_id,
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
        raw = envelope.get("raw", {})
        connection = raw.get("connection", _UNKNOWN_CONNECTION)
        error = raw.get("error")
        if connection in self._open_disconnects:
            # Shouldn't normally happen (the collector only emits one
            # `disconnect` per drop), but guard against a duplicate signal
            # clobbering the original incident's start_ts.
            log.debug("disconnect signal for already-open incident, ignoring", extra={"connection": connection})
            return
        incident = incidents.open_disconnect_incident(
            exchange=self._exchange,
            segment=self._segment,
            symbol=self._symbol,
            connection=connection,
            error=error,
        )
        self._open_disconnects[connection] = incident
        await self._sink.add_incident(incident)
        log.warning("collector_disconnect detected", extra={"connection": connection, "error": error})

    async def _handle_reconnect(self, envelope: dict[str, Any]) -> None:
        raw = envelope.get("raw", {})
        connection = raw.get("connection", _UNKNOWN_CONNECTION)
        open_incident = self._open_disconnects.get(connection)
        if open_incident is None:
            # Reconnect signal with no matching open disconnect on this
            # connection (e.g. normalizer restarted mid-incident) --
            # nothing to resolve.
            log.debug("reconnect signal with no tracked open disconnect incident", extra={"connection": connection})
            return
        resolved = incidents.resolve_disconnect_incident(
            open_incident,
            reconnect_attempts=1,  # see incidents.py docstring: collector emits exactly one signal per attempt
        )
        await self._sink.add_incident(resolved)
        del self._open_disconnects[connection]
        log.info(
            "collector_disconnect resolved",
            extra={"connection": connection, "downtime_ms": resolved["details"]["downtime_ms"]},
        )

    async def _check_reference_price_freeze(self, row: dict[str, Any]) -> None:
        """Best-effort `reference_price_freeze` detection -- see
        incidents.py module docstring for the heuristic's caveats. Only
        runs for XAUUSDT/XAGUSDT (binance.md's documented freeze
        candidates); every other symbol's markPrice is expected to move
        continuously and is never evaluated against this heuristic at all.
        """
        if self._symbol not in incidents.FREEZE_CANDIDATE_SYMBOLS:
            return

        price = row["mark_price"]
        if price == self._mark_price_last_value:
            self._mark_price_unchanged_count += 1
        else:
            if self._open_price_freeze is not None:
                resolved = incidents.resolve_reference_price_freeze_incident(self._open_price_freeze)
                await self._sink.add_incident(resolved)
                self._open_price_freeze = None
                log.info("reference_price_freeze resolved, markPrice changed", extra={"symbol": self._symbol})
            self._mark_price_last_value = price
            self._mark_price_unchanged_count = 1

        if self._mark_price_unchanged_count >= self._freeze_unchanged_threshold and self._open_price_freeze is None:
            incident = incidents.open_reference_price_freeze_incident(
                exchange=self._exchange,
                segment=self._segment,
                symbol=self._symbol,
                frozen_price=price,
                unchanged_count=self._mark_price_unchanged_count,
            )
            self._open_price_freeze = incident
            await self._sink.add_incident(incident)
            log.info(
                "reference_price_freeze detected",
                extra={"symbol": self._symbol, "unchanged_count": self._mark_price_unchanged_count},
            )

    async def periodic_book_snapshot_loop(self, interval_s: float) -> None:
        """Every `interval_s`, if the book is ready, materialize its current
        state as an orderbook_snapshots row (docs §6.4 periodic snapshot)."""
        while True:
            await asyncio.sleep(interval_s)
            if self._book.ready and not self._book.awaiting_resync:
                row = self._book.to_snapshot_row()
                await self._sink.add_orderbook_snapshot(row)
                log.debug("periodic book snapshot emitted", extra={"last_update_id": row["last_update_id"]})
