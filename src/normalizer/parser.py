"""Parse raw collector payloads (as pushed to Redis Streams -- see
src/collector/runner.py and src/exchanges/binance/{spot,usdtm}.py) into row
dicts matching the exact ClickHouse column shapes in
infra/clickhouse/migrations/001_trades.sql, 002_orderbook_events.sql, and
005_derivatives.sql (mark_price / open_interest / liquidations).

Binance spot and USDT-M perp are both implemented (Phase B,
docs/08-prototype-roadmap.md); the module is kept small and exchange-
labelled so a second exchange's parser can live alongside this one without a
rewrite.
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
     "raw": {"stream": ..., "data": {...}}}.

    Handles BOTH Binance spot `@trade` and USDT-M perp `@aggTrade` -- both
    land on the same `:trades` Redis stream (the collector's runner
    classifies both as `kind == "trade"`, see runner.py `_stream_for_kind`),
    and per 001_trades.sql's own header comment ("column is named
    generically (`trade_id`) so a later perp/aggTrade ingestion path can
    reuse this table without a rename") they share market_data.trades
    rather than getting a redundant second table.

    Discriminator: spot's `@trade` payload has a genuine per-execution `t`
    field; perp's `@aggTrade` payload has no `t` at all (only the aggregate
    id `a`). Branch on presence of `t`, not on `segment`, so this also works
    unmodified for a future COIN-M perp adapter (same `@aggTrade` shape,
    different segment string).

    IMPORTANT for perp rows: `trade_id` is Binance aggTrade's `a` -- an
    AGGREGATE id, not a 1:1 execution id (multiple same-price/same-ts fills
    within the same 100ms window share one `a`; memory/exchanges.md). The
    aggregate's constituent first/last trade ids (`f`/`l`) are not stored --
    001_trades.sql (frozen, never edited by this change) has no column for
    them, and the table's own header comment anticipated exactly this
    reduced-fidelity reuse for perp. `buyer_order_id`/`seller_order_id`
    (spot-only `b`/`a` order-id fields, not present on aggTrade) are left
    NULL for perp rows.
    """
    data = envelope["raw"]["data"]
    is_spot_trade = "t" in data
    if is_spot_trade:
        trade_id = int(data["t"])
        buyer_order_id = int(data["b"]) if data.get("b") is not None else None
        seller_order_id = int(data["a"]) if data.get("a") is not None else None
    else:
        # Perp aggTrade: `a` is the aggregate trade id (see docstring above).
        trade_id = int(data["a"])
        buyer_order_id = None
        seller_order_id = None
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "trade_id": trade_id,
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "is_buyer_maker": bool(data["m"]),
        "buyer_order_id": buyer_order_id,
        "seller_order_id": seller_order_id,
        "ts_exchange": _ms_to_dt(int(data["T"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_depth_diff(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]["data"]` carries Binance `@depth@100ms` fields:
    U, u, b, a, E, s, and (futures only) `pu`.

    `prev_update_id` is exchange-aware, not spot-hardcoded: Binance spot's
    depth-diff payload has no `pu` field at all, so `data.get("pu")` is
    naturally None there and the column stays NULL as before; USDT-M (and
    COIN-M later) perp payloads DO carry `pu` (binance.md step 5: futures
    continuity is `current.pu == previous.u`), and it is now extracted and
    passed through into the row so orderbook_state.OrderBookState can run
    the futures-specific continuity check (see orderbook_state.py).

    Returns a row dict matching market_data.orderbook_events columns (row
    still includes qty=0 "delete" levels untouched, per schema comment).
    """
    data = envelope["raw"]["data"]
    prev_update_id = int(data["pu"]) if data.get("pu") is not None else None
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "first_update_id": int(data["U"]),
        "final_update_id": int(data["u"]),
        "prev_update_id": prev_update_id,
        "bids": _levels(data.get("b", [])),
        "asks": _levels(data.get("a", [])),
        "ts_exchange": _ms_to_dt(int(data["E"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_snapshot(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]` is the untouched REST depth-snapshot body (spot
    `GET /api/v3/depth` or USDT-M `GET /fapi/v1/depth` -- same shape):
    {"lastUpdateId": ..., "bids": [...], "asks": [...]}.

    Returns a row dict matching market_data.orderbook_snapshots columns.
    There is no exchange-side snapshot-generation timestamp published by
    either REST endpoint, so ts_exchange is set equal to ts_received per
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


def parse_mark_price(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]["data"]` carries Binance USDT-M `@markPrice@1s`
    fields: `p` (mark price), `i` (index price), `r` (funding rate),
    `T` (next funding time, ms), `E` (event time, ms).

    No XAUUSDT/XAGUSDT special-casing here (binance.md / adapter docstring:
    the freeze-outside-trading-hours behavior is passed through untouched by
    design) -- detecting the freeze is `reference_price_freeze` incident
    logic in consumer.py, not a parsing concern; this function stores
    whatever the exchange sends, frozen or not, verbatim.

    Returns a row dict matching market_data.mark_price columns
    (005_derivatives.sql).
    """
    data = envelope["raw"]["data"]
    funding_rate = data.get("r")
    next_funding_time_ms = data.get("T")
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "mark_price": float(data["p"]),
        "index_price": float(data["i"]),
        "funding_rate": float(funding_rate) if funding_rate is not None else None,
        "next_funding_time": _ms_to_dt(int(next_funding_time_ms)) if next_funding_time_ms else None,
        "ts_exchange": _ms_to_dt(int(data["E"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_open_interest(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]` is the untouched REST `GET /fapi/v1/openInterest`
    body: {"symbol": ..., "openInterest": "...", "time": ms}. Published by
    the collector's REST-poller mechanism onto `{stream_prefix}:poll` with
    `type == "open_interest"` (src/collector/runner.py `_run_rest_poller`).

    Unlike parse_snapshot, Binance's openInterest response DOES carry its
    own exchange-side timestamp (`time`), so ts_exchange is taken from it
    rather than forced equal to ts_received.

    Returns a row dict matching market_data.open_interest columns
    (005_derivatives.sql).
    """
    raw = envelope["raw"]
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "open_interest": float(raw["openInterest"]),
        "ts_exchange": _ms_to_dt(int(raw["time"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }


def parse_liquidation(envelope: dict[str, Any]) -> dict[str, Any]:
    """`envelope["raw"]["data"]` carries Binance USDT-M `@forceOrder`'s
    top-level fields plus a nested `o` object: `s` (symbol), `S` (side),
    `p` (price), `ap` (avg price), `q` (orig qty), `z` (filled qty),
    `X` (order status), `T` (order trade time, ms).

    binance.md / overview.md §6.3.1: this stream is NOT a full liquidation
    tape (largest-per-1000ms-per-symbol only) -- that's recorded as a
    static `liquidation_partial_coverage` entry in `/known-limitations`,
    not something this parser needs to flag per-row.

    Returns a row dict matching market_data.liquidations columns
    (005_derivatives.sql).
    """
    o = envelope["raw"]["data"]["o"]
    return {
        "exchange": envelope["exchange"],
        "segment": envelope["segment"],
        "symbol": envelope["symbol"],
        "side": o["S"],
        "price": float(o["p"]),
        "avg_price": float(o["ap"]),
        "quantity": float(o["q"]),
        "filled_quantity": float(o["z"]),
        "order_status": o["X"],
        "ts_exchange": _ms_to_dt(int(o["T"])),
        "ts_received": _ms_to_dt(int(envelope["receive_ts"])),
    }
