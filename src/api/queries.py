"""All read-path query logic for the API service, in one place so
rest.py and mcp_tools.py never duplicate SQL between the two protocols --
each just calls the functions below.

Every ClickHouse query is parameterized via clickhouse-connect's
`parameters={...}` + `{name:Type}` placeholders (docs/04-architecture/
01-stack.md); values from external callers (symbol, timestamps, cursors)
are never string-interpolated into SQL.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from clickhouse_connect.driver.asyncclient import AsyncClient


class OrderBookNotFound(Exception):
    """Raised when no orderbook_snapshots row exists at/before the requested
    timestamp -- point-in-time reconstruction has nothing to start from."""


def _rows(result: Any) -> list[dict[str, Any]]:
    return list(result.named_results())


def parse_ts(value: str | None) -> datetime | None:
    """Parse a caller-supplied ISO-8601 timestamp (REST query param or MCP
    tool argument) into a UTC-aware datetime. Shared so both protocols apply
    the same rules (e.g. bare 'Z' suffix, naive timestamps assumed UTC)."""
    if value is None:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_jsonable(value: Any) -> Any:
    """Recursively convert query-result values (datetime, tuples) into
    plain JSON-serializable types, for the MCP tool responses (unlike REST,
    which gets this for free from Pydantic/FastAPI's response models)."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return value


async def get_trades(
    client: AsyncClient,
    *,
    exchange: str,
    segment: str,
    symbol: str,
    ts_from: datetime | None,
    ts_to: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    conditions = ["exchange = {exchange:String}", "segment = {segment:String}", "symbol = {symbol:String}"]
    params: dict[str, Any] = {"exchange": exchange, "segment": segment, "symbol": symbol, "limit": limit}
    if ts_from is not None:
        conditions.append("ts_exchange >= {ts_from:DateTime64(3)}")
        params["ts_from"] = ts_from
    if ts_to is not None:
        conditions.append("ts_exchange <= {ts_to:DateTime64(3)}")
        params["ts_to"] = ts_to

    query = f"""
        SELECT exchange, segment, symbol, trade_id, price, quantity, quote_qty,
               is_buyer_maker, buyer_order_id, seller_order_id, ts_exchange, ts_received
        FROM trades
        WHERE {' AND '.join(conditions)}
        ORDER BY ts_exchange ASC, trade_id ASC
        LIMIT {{limit:UInt64}}
    """
    result = await client.query(query, parameters=params)
    return _rows(result)


def _parse_cursor(cursor: str) -> tuple[datetime, int]:
    ts_ms_str, update_id_str = cursor.split(":", 1)
    ts = datetime.fromtimestamp(int(ts_ms_str) / 1000.0, tz=timezone.utc)
    return ts, int(update_id_str)


def _make_cursor(ts_exchange: datetime, final_update_id: int) -> str:
    ts_ms = int(ts_exchange.timestamp() * 1000)
    return f"{ts_ms}:{final_update_id}"


async def get_orderbook_events(
    client: AsyncClient,
    *,
    exchange: str,
    segment: str,
    symbol: str,
    ts_from: datetime | None,
    ts_to: datetime | None,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    conditions = ["exchange = {exchange:String}", "segment = {segment:String}", "symbol = {symbol:String}"]
    params: dict[str, Any] = {"exchange": exchange, "segment": segment, "symbol": symbol, "limit": limit}
    if ts_from is not None:
        conditions.append("ts_exchange >= {ts_from:DateTime64(3)}")
        params["ts_from"] = ts_from
    if ts_to is not None:
        conditions.append("ts_exchange <= {ts_to:DateTime64(3)}")
        params["ts_to"] = ts_to
    if cursor is not None:
        cursor_ts, cursor_update_id = _parse_cursor(cursor)
        # Tuple comparison (ClickHouse supports lexicographic tuple ordering
        # natively) keeps pagination stable even when several events share a
        # ts_exchange millisecond.
        conditions.append("(ts_exchange, final_update_id) > ({cursor_ts:DateTime64(3)}, {cursor_update_id:UInt64})")
        params["cursor_ts"] = cursor_ts
        params["cursor_update_id"] = cursor_update_id

    query = f"""
        SELECT exchange, segment, symbol, first_update_id, final_update_id, prev_update_id,
               bids, asks, ts_exchange, ts_received
        FROM orderbook_events
        WHERE {' AND '.join(conditions)}
        ORDER BY ts_exchange ASC, final_update_id ASC
        LIMIT {{limit:UInt64}}
    """
    result = await client.query(query, parameters=params)
    rows = _rows(result)
    next_cursor = _make_cursor(rows[-1]["ts_exchange"], rows[-1]["final_update_id"]) if rows else None
    return {"events": rows, "next_cursor": next_cursor}


@dataclass
class OrderBookAt:
    symbol: str
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    snapshot_ts: datetime
    events_applied: int
    last_update_id: int


def _apply_levels(book_side: dict[float, float], levels: list[tuple[float, float]]) -> None:
    # Same rule as normalizer/orderbook_state.py's _apply_levels: qty == 0
    # removes the price level. Reimplemented standalone here (rather than
    # importing OrderBookState) since that class also carries live-stream-only
    # concerns (sequence-break detection, resync waiting) that don't apply to
    # this historical read path.
    for price, qty in levels:
        if qty == 0:
            book_side.pop(price, None)
        else:
            book_side[price] = qty


async def get_orderbook_at(
    client: AsyncClient,
    *,
    exchange: str,
    segment: str,
    symbol: str,
    ts: datetime,
) -> OrderBookAt:
    snapshot_query = """
        SELECT last_update_id, bids, asks, ts_received
        FROM orderbook_snapshots
        WHERE exchange = {exchange:String} AND segment = {segment:String}
          AND symbol = {symbol:String} AND ts_received <= {ts:DateTime64(3)}
        ORDER BY ts_received DESC
        LIMIT 1
    """
    snapshot_result = await client.query(
        snapshot_query,
        parameters={"exchange": exchange, "segment": segment, "symbol": symbol, "ts": ts},
    )
    snapshot_rows = _rows(snapshot_result)
    if not snapshot_rows:
        raise OrderBookNotFound(
            f"no orderbook_snapshots row at/before {ts.isoformat()} for "
            f"{exchange}/{segment}/{symbol}"
        )
    snapshot = snapshot_rows[0]

    events_query = """
        SELECT bids, asks, final_update_id
        FROM orderbook_events
        WHERE exchange = {exchange:String} AND segment = {segment:String}
          AND symbol = {symbol:String} AND final_update_id > {last_update_id:UInt64}
          AND ts_exchange <= {ts:DateTime64(3)}
        ORDER BY final_update_id ASC
    """
    events_result = await client.query(
        events_query,
        parameters={
            "exchange": exchange,
            "segment": segment,
            "symbol": symbol,
            "last_update_id": snapshot["last_update_id"],
            "ts": ts,
        },
    )
    events = _rows(events_result)

    bids = {price: qty for price, qty in snapshot["bids"]}
    asks = {price: qty for price, qty in snapshot["asks"]}
    last_update_id = snapshot["last_update_id"]
    for event in events:
        _apply_levels(bids, event["bids"])
        _apply_levels(asks, event["asks"])
        last_update_id = event["final_update_id"]

    return OrderBookAt(
        symbol=symbol,
        bids=sorted(bids.items(), key=lambda kv: -kv[0]),
        asks=sorted(asks.items(), key=lambda kv: kv[0]),
        snapshot_ts=snapshot["ts_received"],
        events_applied=len(events),
        last_update_id=last_update_id,
    )


async def list_incidents(
    client: AsyncClient,
    *,
    exchange: str,
    segment: str,
    symbol: str | None,
    ts_from: datetime | None,
    ts_to: datetime | None,
) -> list[dict[str, Any]]:
    conditions = ["exchange = {exchange:String}", "segment = {segment:String}"]
    params: dict[str, Any] = {"exchange": exchange, "segment": segment}
    if symbol is not None:
        # incidents.symbol is Nullable(String); NULL means segment-wide, so
        # a symbol-scoped query includes both its own rows and segment-wide ones.
        conditions.append("(symbol = {symbol:String} OR symbol IS NULL)")
        params["symbol"] = symbol
    if ts_from is not None:
        conditions.append("start_ts >= {ts_from:DateTime64(3)}")
        params["ts_from"] = ts_from
    if ts_to is not None:
        conditions.append("start_ts <= {ts_to:DateTime64(3)}")
        params["ts_to"] = ts_to

    query = f"""
        SELECT id, exchange, type, severity, segment, symbol, affected_channel,
               start_ts, end_ts, status, description, detected_by, details
        FROM incidents
        WHERE {' AND '.join(conditions)}
        ORDER BY start_ts DESC
    """
    result = await client.query(query, parameters=params)
    rows = _rows(result)
    for row in rows:
        # `details` is a JSON-serialized String column (ClickHouse's native
        # JSON type is experimental/disabled on the pinned 24.8 server, see
        # 004_incidents.sql) -- parse it back to a dict for API consumers.
        try:
            row["details"] = json.loads(row["details"]) if row["details"] else {}
        except (TypeError, ValueError):
            row["details"] = {}
    return rows


async def get_derivatives_metrics(
    client: AsyncClient,
    *,
    exchange: str,
    segment: str,
    symbol: str,
    ts_from: datetime | None,
    ts_to: datetime | None,
) -> dict[str, Any]:
    """Combine funding/mark/index price, open interest, and liquidations for
    `symbol` over an optional [ts_from, ts_to] range (docs/03-mvp-scope.md:
    "funding + OI + liquidations together"). Each of the three source tables
    is queried independently and combined client-side -- no SQL join -- since
    they're genuinely independent cadences (see 005_derivatives.sql's header:
    markPrice ~1s, open interest ~45s REST poll, liquidations irregular
    forceOrder events). Callers should also surface the
    `liquidation_partial_coverage` entry from `get_known_limitations()`
    alongside this response's `liquidations` list -- forceOrder is not a
    complete tape (see known_limitations.py)."""
    conditions = ["exchange = {exchange:String}", "segment = {segment:String}", "symbol = {symbol:String}"]
    params: dict[str, Any] = {"exchange": exchange, "segment": segment, "symbol": symbol}
    if ts_from is not None:
        conditions.append("ts_exchange >= {ts_from:DateTime64(3)}")
        params["ts_from"] = ts_from
    if ts_to is not None:
        conditions.append("ts_exchange <= {ts_to:DateTime64(3)}")
        params["ts_to"] = ts_to
    where = " AND ".join(conditions)

    mark_price_query = f"""
        SELECT exchange, segment, symbol, mark_price, index_price, funding_rate,
               next_funding_time, ts_exchange, ts_received
        FROM mark_price
        WHERE {where}
        ORDER BY ts_exchange ASC
    """
    open_interest_query = f"""
        SELECT exchange, segment, symbol, open_interest, ts_exchange, ts_received
        FROM open_interest
        WHERE {where}
        ORDER BY ts_exchange ASC
    """
    liquidations_query = f"""
        SELECT exchange, segment, symbol, side, price, avg_price, quantity,
               filled_quantity, order_status, ts_exchange, ts_received
        FROM liquidations
        WHERE {where}
        ORDER BY ts_exchange ASC
    """

    mark_price_result = await client.query(mark_price_query, parameters=params)
    open_interest_result = await client.query(open_interest_query, parameters=params)
    liquidations_result = await client.query(liquidations_query, parameters=params)

    return {
        "exchange": exchange,
        "segment": segment,
        "symbol": symbol,
        "mark_price": _rows(mark_price_result),
        "open_interest": _rows(open_interest_result),
        "liquidations": _rows(liquidations_result),
    }


def get_known_limitations() -> list[dict[str, Any]]:
    from api.known_limitations import KNOWN_LIMITATIONS

    return KNOWN_LIMITATIONS
