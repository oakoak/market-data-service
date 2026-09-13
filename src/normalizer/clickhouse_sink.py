"""Batched ClickHouse insert sink using `clickhouse-connect`'s native asyncio
client (docs/04-architecture/01-stack.md: >=0.12.0 has native async I/O on
aiohttp, no `asyncio.to_thread` wrapping needed for batch inserts).

Batching policy (kept simple per task instructions): each of the four
tables (trades, orderbook_events, orderbook_snapshots, incidents) has its
own buffer, flushed when it reaches `batch_size` rows or every
`flush_interval_s` seconds, whichever comes first. A background flusher
task drives the timer; callers (consumer.py) call `add_*` and the sink
flushes synchronously inline once a buffer is full.
"""

from __future__ import annotations

import asyncio
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from common import get_logger

log = get_logger("normalizer.clickhouse_sink")

TRADES_COLUMNS = [
    "exchange",
    "segment",
    "symbol",
    "trade_id",
    "price",
    "quantity",
    "is_buyer_maker",
    "buyer_order_id",
    "seller_order_id",
    "ts_exchange",
    "ts_received",
]

ORDERBOOK_EVENTS_COLUMNS = [
    "exchange",
    "segment",
    "symbol",
    "first_update_id",
    "final_update_id",
    "prev_update_id",
    "bids",
    "asks",
    "ts_exchange",
    "ts_received",
]

ORDERBOOK_SNAPSHOTS_COLUMNS = [
    "exchange",
    "segment",
    "symbol",
    "last_update_id",
    "bids",
    "asks",
    "ts_exchange",
    "ts_received",
]

INCIDENTS_COLUMNS = [
    "id",
    "exchange",
    "type",
    "severity",
    "segment",
    "symbol",
    "affected_channel",
    "start_ts",
    "end_ts",
    "status",
    "description",
    "detected_by",
    "details",
]


class ClickHouseSink:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        batch_size: int,
        flush_interval_s: float,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s

        self._client: AsyncClient | None = None
        self._buffers: dict[str, list[list[Any]]] = {
            "trades": [],
            "orderbook_events": [],
            "orderbook_snapshots": [],
            "incidents": [],
        }
        self._locks: dict[str, asyncio.Lock] = {
            name: asyncio.Lock() for name in self._buffers
        }
        self._flusher_task: asyncio.Task[None] | None = None
        self._closing = False

    async def connect(self) -> None:
        self._client = await clickhouse_connect.get_async_client(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._user,
            password=self._password,
        )
        self._flusher_task = asyncio.create_task(self._periodic_flush())

    async def close(self) -> None:
        self._closing = True
        if self._flusher_task is not None:
            self._flusher_task.cancel()
            try:
                await self._flusher_task
            except asyncio.CancelledError:
                pass
        await self.flush_all()
        if self._client is not None:
            await self._client.close()

    async def _periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            try:
                await self.flush_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("periodic flush failed, will retry on next tick")

    async def flush_all(self) -> None:
        for table in list(self._buffers):
            await self._flush_table(table)

    async def add_trade(self, row: dict[str, Any]) -> None:
        await self._add("trades", TRADES_COLUMNS, row)

    async def add_orderbook_event(self, row: dict[str, Any]) -> None:
        await self._add("orderbook_events", ORDERBOOK_EVENTS_COLUMNS, row)

    async def add_orderbook_snapshot(self, row: dict[str, Any]) -> None:
        await self._add("orderbook_snapshots", ORDERBOOK_SNAPSHOTS_COLUMNS, row)

    async def add_incident(self, row: dict[str, Any]) -> None:
        import json as _json

        prepared = dict(row)
        details = prepared.get("details")
        if isinstance(details, dict):
            prepared["details"] = _json.dumps(details, default=str)
        await self._add("incidents", INCIDENTS_COLUMNS, prepared)

    async def _add(self, table: str, columns: list[str], row: dict[str, Any]) -> None:
        values = [row.get(col) for col in columns]
        async with self._locks[table]:
            self._buffers[table].append(values)
            should_flush = len(self._buffers[table]) >= self._batch_size
        if should_flush:
            await self._flush_table(table)

    async def _flush_table(self, table: str) -> None:
        columns = _COLUMNS_BY_TABLE[table]
        async with self._locks[table]:
            if not self._buffers[table]:
                return
            batch = self._buffers[table]
            self._buffers[table] = []
        assert self._client is not None, "call connect() first"
        try:
            await self._client.insert(table, batch, column_names=columns)
        except Exception:
            log.exception("clickhouse insert failed, dropping batch", extra={"table": table, "rows": len(batch)})
            raise


_COLUMNS_BY_TABLE = {
    "trades": TRADES_COLUMNS,
    "orderbook_events": ORDERBOOK_EVENTS_COLUMNS,
    "orderbook_snapshots": ORDERBOOK_SNAPSHOTS_COLUMNS,
    "incidents": INCIDENTS_COLUMNS,
}
