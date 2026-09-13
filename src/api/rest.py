"""FastAPI REST router. All handlers are thin: parse/validate params, call
into queries.py, shape the response with a Pydantic model. No query logic
lives here -- see queries.py docstring.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from api import queries

router = APIRouter()


def _parse_ts(value: str | None) -> datetime | None:
    try:
        return queries.parse_ts(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid timestamp: {value!r}") from exc


def _defaults(request: Request, exchange: str | None, segment: str | None) -> tuple[str, str]:
    config = request.app.state.config
    return exchange or config.default_exchange, segment or config.default_segment


class TradeOut(BaseModel):
    exchange: str
    segment: str
    symbol: str
    trade_id: int
    price: float
    quantity: float
    quote_qty: float
    is_buyer_maker: bool
    buyer_order_id: int | None
    seller_order_id: int | None
    ts_exchange: datetime
    ts_received: datetime


@router.get("/trades", response_model=list[TradeOut])
async def trades(
    request: Request,
    symbol: str,
    ts_from: str | None = Query(default=None),
    ts_to: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10_000),
    exchange: str | None = Query(default=None),
    segment: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    exchange, segment = _defaults(request, exchange, segment)
    return await queries.get_trades(
        request.app.state.ch_client.client,
        exchange=exchange,
        segment=segment,
        symbol=symbol.upper(),
        ts_from=_parse_ts(ts_from),
        ts_to=_parse_ts(ts_to),
        limit=limit,
    )


class OrderBookAtResponse(BaseModel):
    symbol: str
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    snapshot_ts: datetime
    events_applied: int
    last_update_id: int


@router.get("/orderbook/at", response_model=OrderBookAtResponse)
async def orderbook_at(
    request: Request,
    symbol: str,
    ts: str,
    exchange: str | None = Query(default=None),
    segment: str | None = Query(default=None),
) -> OrderBookAtResponse:
    exchange, segment = _defaults(request, exchange, segment)
    parsed_ts = _parse_ts(ts)
    assert parsed_ts is not None
    try:
        result = await queries.get_orderbook_at(
            request.app.state.ch_client.client,
            exchange=exchange,
            segment=segment,
            symbol=symbol.upper(),
            ts=parsed_ts,
        )
    except queries.OrderBookNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return OrderBookAtResponse(
        symbol=result.symbol,
        bids=result.bids,
        asks=result.asks,
        snapshot_ts=result.snapshot_ts,
        events_applied=result.events_applied,
        last_update_id=result.last_update_id,
    )


class OrderBookEventOut(BaseModel):
    exchange: str
    segment: str
    symbol: str
    first_update_id: int
    final_update_id: int
    prev_update_id: int | None
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    ts_exchange: datetime
    ts_received: datetime


class OrderBookEventsResponse(BaseModel):
    events: list[OrderBookEventOut]
    next_cursor: str | None


@router.get("/orderbook/events", response_model=OrderBookEventsResponse)
async def orderbook_events(
    request: Request,
    symbol: str,
    ts_from: str | None = Query(default=None),
    ts_to: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10_000),
    cursor: str | None = Query(default=None),
    exchange: str | None = Query(default=None),
    segment: str | None = Query(default=None),
) -> dict[str, Any]:
    exchange, segment = _defaults(request, exchange, segment)
    return await queries.get_orderbook_events(
        request.app.state.ch_client.client,
        exchange=exchange,
        segment=segment,
        symbol=symbol.upper(),
        ts_from=_parse_ts(ts_from),
        ts_to=_parse_ts(ts_to),
        limit=limit,
        cursor=cursor,
    )


class IncidentOut(BaseModel):
    id: str
    exchange: str
    type: str
    severity: str
    segment: str
    symbol: str | None
    affected_channel: str
    start_ts: datetime
    end_ts: datetime | None
    status: str
    description: str
    detected_by: str
    details: dict[str, Any]


@router.get("/incidents", response_model=list[IncidentOut])
async def incidents(
    request: Request,
    symbol: str | None = Query(default=None),
    ts_from: str | None = Query(default=None),
    ts_to: str | None = Query(default=None),
    exchange: str | None = Query(default=None),
    segment: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    exchange, segment = _defaults(request, exchange, segment)
    return await queries.list_incidents(
        request.app.state.ch_client.client,
        exchange=exchange,
        segment=segment,
        symbol=symbol.upper() if symbol else None,
        ts_from=_parse_ts(ts_from),
        ts_to=_parse_ts(ts_to),
    )


@router.get("/known-limitations", response_model=list[dict[str, Any]])
async def known_limitations() -> list[dict[str, Any]]:
    return queries.get_known_limitations()
