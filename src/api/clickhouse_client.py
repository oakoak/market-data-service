"""Read-only async ClickHouse client wrapper.

Separate from normalizer/clickhouse_sink.py's `ClickHouseSink` (write-only,
batched inserts) -- this side only ever runs `SELECT`s, so there's no
buffering/flushing to do, just the connect()/close() lifecycle around
`clickhouse_connect.get_async_client(...)` (same pattern as clickhouse_sink.py).
"""

from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from common import get_logger

log = get_logger("api.clickhouse_client")


class ClickHouseReadClient:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._client: AsyncClient | None = None

    async def connect(self) -> None:
        self._client = await clickhouse_connect.get_async_client(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._user,
            password=self._password,
        )
        log.info("clickhouse client connected", extra={"host": self._host, "port": self._port})

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> AsyncClient:
        assert self._client is not None, "call connect() first"
        return self._client
