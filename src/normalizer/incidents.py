"""Incident creation/resolution helpers matching
docs/04-architecture/00-overview.md §6.3.1 exactly (field names/types) and
infra/clickhouse/migrations/004_incidents.sql.

Only the two incident types reachable from this cut-down single-instrument
spot prototype are implemented: `orderbook_sequence_break` and
`collector_disconnect` (per task scope -- `reference_price_freeze` and
`backfill_gap` need markPrice/funding streams, not applicable here).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

DETECTED_BY = "normalizer"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return str(uuid.uuid4())


def open_sequence_break_incident(
    *,
    exchange: str,
    segment: str,
    symbol: str,
    last_valid_update_id: int,
    start_ts: datetime | None = None,
) -> dict[str, Any]:
    """Open an `orderbook_sequence_break` incident. `resync_source` is left
    unset in `details` until resolution (we don't know it yet -- the
    resolving snapshot hasn't arrived), matching the overview.md example
    where `resync_source` describes the snapshot that *fixed* the break.
    """
    ts = start_ts or _now()
    details = {
        "source_channel": "depth",
        "last_valid_update_id": last_valid_update_id,
    }
    return {
        "id": _new_id(),
        "exchange": exchange,
        "type": "orderbook_sequence_break",
        "severity": "warning",
        "segment": segment,
        "symbol": symbol,
        "affected_channel": "orderbook",
        "start_ts": ts,
        "end_ts": None,
        "status": "open",
        "description": (
            "Break in U/u continuity in the order book diff stream "
            f"(spot: current.U != previous.u + 1, last valid update id "
            f"{last_valid_update_id}); resync pending."
        ),
        "detected_by": DETECTED_BY,
        "details": details,
    }


def resolve_sequence_break_incident(
    incident: dict[str, Any],
    *,
    resync_last_update_id: int,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    """Return a new row (same `id`) marking the incident resolved once a
    resync (fresh snapshot) has been applied. ClickHouse's MergeTree doesn't
    do in-place updates; the caller inserts this as a new row and relies on
    querying the latest row per incident id (or a ReplacingMergeTree-style
    dedup) -- see clickhouse_sink.py note on this trade-off.
    """
    ts = end_ts or _now()
    details = dict(incident["details"])
    details["resync_source"] = f"REST snapshot lastUpdateId={resync_last_update_id}"
    return {
        **incident,
        "end_ts": ts,
        "status": "resolved",
        "description": (
            incident["description"] + " Resync completed via REST snapshot "
            f"lastUpdateId={resync_last_update_id}."
        ),
        "details": details,
    }


def open_disconnect_incident(
    *,
    exchange: str,
    segment: str,
    symbol: str,
    error: str | None,
    start_ts: datetime | None = None,
) -> dict[str, Any]:
    """Open a `collector_disconnect` incident from the collector's raw
    `disconnect` signal event (`raw = {"error": "..."}`, pushed on the depth
    stream -- see runner.py `_emit_signal`).

    Note (flagged per task instructions): the collector's disconnect payload
    carries only a free-text `error` string and no reconnect-attempt
    counter, so `details.reconnect_attempts` cannot be filled precisely at
    open time -- it is filled in on resolution from the number of
    `reconnect` signals observed (best-effort, see consumer.py), not from
    any collector-provided counter (the collector doesn't send one).
    """
    ts = start_ts or _now()
    details: dict[str, Any] = {"source_channel": "depth"}
    if error:
        details["error"] = error
    return {
        "id": _new_id(),
        "exchange": exchange,
        "type": "collector_disconnect",
        "severity": "warning",
        "segment": segment,
        "symbol": symbol,
        "affected_channel": "collector",
        "start_ts": ts,
        "end_ts": None,
        "status": "open",
        "description": f"Collector WS disconnect ({error or 'unknown error'}).",
        "detected_by": DETECTED_BY,
        "details": details,
    }


def resolve_disconnect_incident(
    incident: dict[str, Any],
    *,
    reconnect_attempts: int,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    """Resolve on the matching `reconnect` signal. `downtime_ms` is
    computed from start_ts/end_ts (both normalizer-observed receive_ts, not
    exchange-side timestamps -- the collector's signal payloads carry no
    other timing info, see runner.py `_emit_signal`: `disconnect` raw is
    `{"error": str}}`, `reconnect` raw is `{"attempt": true}` with nothing
    else). `reconnect_attempts` counts collector-emitted `reconnect` signals
    since the matching `disconnect`, which for this collector's backoff
    loop is always 1 (it emits exactly one `reconnect` signal right before
    re-attempting the connection, not one per retry) -- documented here
    rather than silently presented as more precise than it is.
    """
    ts = end_ts or _now()
    downtime_ms = int((ts - incident["start_ts"]).total_seconds() * 1000)
    details = dict(incident["details"])
    details["downtime_ms"] = downtime_ms
    details["reconnect_attempts"] = reconnect_attempts
    return {
        **incident,
        "end_ts": ts,
        "status": "resolved",
        "description": incident["description"] + " Reconnected.",
        "details": details,
    }


def details_to_json(details: dict[str, Any]) -> str:
    """ClickHouse's native JSON column type accepts a JSON string on
    insert via clickhouse-connect; serialize explicitly rather than relying
    on driver auto-serialization so we control key order/typing."""
    return json.dumps(details, default=str)
