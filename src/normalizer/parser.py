"""Parse raw collector payloads (as pushed to Redis Streams -- see
src/collector/runner.py and src/exchanges/binance/spot.py) into row dicts
matching the exact ClickHouse column shapes in
infra/clickhouse/migrations/001_trades.sql and 002_orderbook_events.sql.

Only Binance spot is implemented (task scope), but the module is kept small
and exchange-labelled so a second exchange's parser can live alongside this
one without a rewrite.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _levels(raw_levels: list[list[str]]) -> list[tuple[float, float]]:
    """Binance gives bids/asks as [["price", "qty"], ...] string pairs."""
    return [(float(p), float(q)) for p, q in raw_levels]


def parse_trade(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope` is the full collector payload:
    {"type": "trade", "receive_ts": ms, "exchange", "segment", "symbol",
     "raw": {"stream": ..., "data": {...Binance @trade fields...}}}.

    Returns a row dict matching market_data.trades columns.
    """
    data = envelope["raw"]["data"]
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "trade_id": int(data["t"]),
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "is_buyer_maker": bool(data["m"]),
        "buyer_order_id": int(data["b"]) if data.get("b") is not None else None,
        "seller_order_id": int(data["a"]) if data.get("a") is not None else None,
        "ts_exchange": _ms_to_dt(int(data["T"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_depth_diff(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]["data"]` carries Binance spot `@depth@100ms` fields:
    U, u, b, a, E, s. No `pu` on spot -- prev_update_id stays NULL, per
    binance.md and migration 002's comment.

    Returns a row dict matching market_data.orderbook_events columns (row
    still includes qty=0 "delete" levels untouched, per schema comment).
    """
    data = envelope["raw"]["data"]
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "first_update_id": int(data["U"]),
        "final_update_id": int(data["u"]),
        "prev_update_id": None,  # spot has no `pu` (futures-only field)
        "bids": _levels(data.get("b", [])),
        "asks": _levels(data.get("a", [])),
        "ts_exchange": _ms_to_dt(int(data["E"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_snapshot(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]` is the untouched REST `GET /api/v3/depth` body:
    {"lastUpdateId": ..., "bids": [...], "asks": [...]}.

    Returns a row dict matching market_data.orderbook_snapshots columns.
    There is no exchange-side snapshot-generation timestamp published by
    Binance's REST endpoint, so ts_exchange is set equal to ts_received per
    migration 003's comment ("uniform dual-timestamp convention").
    """
    raw = envelope["raw"]
    received = _ms_to_dt(int(envelope["receive_ts"]))
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "last_update_id": int(raw["lastUpdateId"]),
        "bids": _levels(raw.get("bids", [])),
        "asks": _levels(raw.get("asks", [])),
        "ts_exchange": received,
        "ts_received": received,
    }
