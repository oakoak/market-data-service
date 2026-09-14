"""Incident creation/resolution helpers matching
docs/04-architecture/00-overview.md §6.3.1 exactly (field names/types) and
infra/clickhouse/migrations/004_incidents.sql.

Phase B (docs/08-prototype-roadmap.md) adds USDT-M perp on top of the
spot-only prototype. Implemented incident types:
  - `orderbook_sequence_break` -- now exchange/segment-aware: spot's
    U==prev.u+1 break and futures' pu==prev.u break share this same
    incident type (per §6.3.1's table -- `segment` in the record already
    disambiguates 'spot' vs the futures segment, no new type needed), only
    `details` differs (see `open_sequence_break_incident`).
  - `collector_disconnect` -- unchanged shape; now opened/resolved per
    named WS connection by the caller (consumer.py), not per-symbol, since
    an adapter can have >1 independent connection (see consumer.py).
  - `reference_price_freeze` -- new. binance.md: markPrice/indexPrice
    freeze at the last value outside gold/silver trading hours for
    XAUUSDT/XAGUSDT specifically, and "the gap/incident detector must not
    confuse this with a feed outage". severity is `info` per
    00-overview.md §6.3.1 ("reference_price_freeze is deliberately kept
    dynamic... just with severity: info").
    IMPORTANT -- best-effort MVP heuristic, NOT empirically validated: per
    docs/06-next-steps.md's "Still open" item 1 (the equivalent Bybit
    question), the exact threshold and a real trading-hours calendar are
    an explicitly open question, not resolved here. This module's detector
    is "N consecutive markPrice values unchanged for a symbol in
    {XAUUSDT, XAGUSDT}" with no calendar awareness at all -- it will also
    fire on a XAUUSDT/XAGUSDT price that happens to be genuinely flat for
    a few ticks during open hours (false positive) and won't distinguish
    weekend/holiday freezes from a stalled upstream (both look the same
    from here). Accepted for MVP because severity is `info`, not
    `warning`/`critical` -- a false positive here is noise, not a
    misleading claim of certainty. Revisit once empirical weekend/holiday
    data is available (same open item as Bybit's).

`backfill_gap` and `clock_drift` are explicitly NOT implemented here --
separate roadmap phases (D/F per the task), out of this change's scope.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

DETECTED_BY = "normalizer"

# XAUUSDT/XAGUSDT are the only symbols binance.md documents as having a
# freeze-outside-trading-hours reference price; the detector is scoped to
# just these two rather than applied blindly to every perp symbol.
FREEZE_CANDIDATE_SYMBOLS = frozenset({"XAUUSDT", "XAGUSDT"})

# "N consecutive unchanged markPrice values" threshold for the best-effort
# freeze heuristic (see module docstring -- not empirically tuned). At the
# @markPrice@1s cadence, 60 consecutive unchanged values is ~60s of a
# perfectly flat mark price, chosen as "long enough that it's very unlikely
# during a genuine open-hours lull" without embedding any real
# trading-hours calendar. Callers may pass their own threshold instead of
# relying on this default.
FREEZE_UNCHANGED_THRESHOLD_DEFAULT = 60


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
    continuity_mode: Literal["spot", "futures"] = "spot",
    received_prev_update_id: int | None = None,
    start_ts: datetime | None = None,
) -> dict[str, Any]:
    """Open an `orderbook_sequence_break` incident. `resync_source` is left
    unset in `details` until resolution (we don't know it yet -- the
    resolving snapshot hasn't arrived), matching the overview.md example
    where `resync_source` describes the snapshot that *fixed* the break.

    `continuity_mode` distinguishes which of binance.md's two step-5 rules
    broke (spot's `U == previous.u + 1` vs futures' `pu == previous.u`) --
    same incident `type`/`affected_channel` either way (see module
    docstring), only `description` wording and `details` differ so a
    futures break isn't misleadingly described in spot-only terms.
    """
    ts = start_ts or _now()
    details: dict[str, Any] = {
        "source_channel": "depth",
        "last_valid_update_id": last_valid_update_id,
        "continuity_mode": continuity_mode,
    }
    if continuity_mode == "futures":
        details["received_prev_update_id"] = received_prev_update_id
        description = (
            "Break in pu/u continuity in the futures order book diff stream "
            f"(current.pu != previous.u; last valid update id "
            f"{last_valid_update_id}, received pu={received_prev_update_id}); "
            "resync pending."
        )
    else:
        description = (
            "Break in U/u continuity in the order book diff stream "
            f"(spot: current.U != previous.u + 1, last valid update id "
            f"{last_valid_update_id}); resync pending."
        )
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
        "description": description,
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
        "description": incident["description"] + " Resync completed via REST snapshot "
        f"lastUpdateId={resync_last_update_id}.",
        "details": details,
    }


def open_disconnect_incident(
    *,
    exchange: str,
    segment: str,
    symbol: str,
    connection: str,
    error: str | None,
    start_ts: datetime | None = None,
) -> dict[str, Any]:
    """Open a `collector_disconnect` incident from the collector's raw
    `disconnect` signal event (`raw = {"error": "...", "connection": "..."}`,
    pushed on the depth stream from every named WS connection -- see
    runner.py `_emit_signal`).

    `connection` (e.g. "public"/"market" for USDT-M perp, "combined" for
    spot) is stored in `details.source_channel` alongside the incident so
    it's clear which of an adapter's possibly-multiple WS connections
    dropped -- callers (consumer.py) also use `connection` as the in-memory
    tracking key so two connections disconnecting independently don't
    clobber each other's open-incident bookkeeping (see consumer.py
    `NormalizerContext`).

    Note (flagged, unchanged from the spot-only prototype): the collector's
    disconnect payload carries only a free-text `error` string and no
    reconnect-attempt counter, so `details.reconnect_attempts` cannot be
    filled precisely at open time -- it is filled in on resolution from the
    number of `reconnect` signals observed (best-effort, see consumer.py),
    not from any collector-provided counter (the collector doesn't send
    one).
    """
    ts = start_ts or _now()
    details: dict[str, Any] = {"source_channel": connection}
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
        "description": f"Collector WS disconnect on connection '{connection}' ({error or 'unknown error'}).",
        "detected_by": DETECTED_BY,
        "details": details,
    }


def resolve_disconnect_incident(
    incident: dict[str, Any],
    *,
    reconnect_attempts: int,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    """Resolve on the matching `reconnect` signal (same `connection` as the
    open incident -- caller matches these up, see consumer.py).
    `downtime_ms` is computed from start_ts/end_ts (both normalizer-observed
    receive_ts, not exchange-side timestamps -- the collector's signal
    payloads carry no other timing info, see runner.py `_emit_signal`:
    `disconnect` raw is `{"error": str, "connection": str}`, `reconnect` raw
    is `{"attempt": true, "connection": str}` with nothing else).
    `reconnect_attempts` counts collector-emitted `reconnect` signals since
    the matching `disconnect` on the SAME connection, which for this
    collector's backoff loop is always 1 (it emits exactly one `reconnect`
    signal right before re-attempting the connection, not one per retry) --
    documented here rather than silently presented as more precise than it
    is.
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


def open_reference_price_freeze_incident(
    *,
    exchange: str,
    segment: str,
    symbol: str,
    frozen_price: float,
    unchanged_count: int,
    start_ts: datetime | None = None,
) -> dict[str, Any]:
    """Open a `reference_price_freeze` incident (see module docstring for
    the detector's best-effort nature and the open-question caveat).
    severity is fixed at `info` per 00-overview.md §6.3.1 -- this is an
    expected characteristic of the XAUUSDT/XAGUSDT reference price outside
    trading hours, not a fault, and must not be confused with a feed
    outage (binance.md) -- keeping severity at `info` (vs `warning` for a
    real `collector_disconnect`/`orderbook_sequence_break`) is exactly how
    that distinction is expressed in this schema.
    """
    ts = start_ts or _now()
    details: dict[str, Any] = {
        "source_channel": "markPrice",
        "frozen_price": frozen_price,
        "unchanged_count": unchanged_count,
        "heuristic": "n_consecutive_unchanged_mark_price",
    }
    return {
        "id": _new_id(),
        "exchange": exchange,
        "type": "reference_price_freeze",
        "severity": "info",
        "segment": segment,
        "symbol": symbol,
        "affected_channel": "mark_price",
        "start_ts": ts,
        "end_ts": None,
        "status": "open",
        "description": (
            f"markPrice/indexPrice for {symbol} unchanged for "
            f"{unchanged_count} consecutive updates (best-effort freeze "
            "heuristic, no trading-hours calendar -- see incidents.py "
            "module docstring; not necessarily a feed outage)."
        ),
        "detected_by": DETECTED_BY,
        "details": details,
    }


def resolve_reference_price_freeze_incident(
    incident: dict[str, Any],
    *,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    """Resolve once markPrice starts changing again."""
    ts = end_ts or _now()
    return {
        **incident,
        "end_ts": ts,
        "status": "resolved",
        "description": incident["description"] + " markPrice resumed changing.",
    }


def details_to_json(details: dict[str, Any]) -> str:
    """ClickHouse's native JSON column type accepts a JSON string on
    insert via clickhouse-connect; serialize explicitly rather than relying
    on driver auto-serialization so we control key order/typing."""
    return json.dumps(details, default=str)
