"""MCP tool registrations. Tool names match docs/03-mvp-scope.md exactly:
`get_trades`, `get_orderbook_at`, `get_orderbook_events`, `get_derivatives_metrics`,
`list_data_incidents` -- all five MVP tools.

Each tool is a thin wrapper around queries.py -- exactly the same functions
rest.py's handlers call, so REST and MCP can never drift apart on query
logic.
"""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from api import queries
from api.clickhouse_client import ClickHouseReadClient
from api.config import Config


def build_mcp_server(ch_client: ClickHouseReadClient, config: Config) -> FastMCP:
    # streamable_http_path="/" because main.py mounts this app's ASGI callable
    # at "/mcp" itself; leaving the SDK's own default ("/mcp") would double up
    # to "/mcp/mcp".
    mcp = FastMCP("market-data-api", streamable_http_path="/")

    @mcp.tool()
    async def get_trades(
        symbol: str,
        ts_from: str | None = None,
        ts_to: str | None = None,
        limit: int = 100,
        exchange: str | None = None,
        segment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch raw trades for `symbol` in an optional [ts_from, ts_to] ISO-8601
        UTC time range, newest-ordered ascending, capped at `limit` rows."""
        rows = await queries.get_trades(
            ch_client.client,
            exchange=exchange or config.default_exchange,
            segment=segment or config.default_segment,
            symbol=symbol.upper(),
            ts_from=queries.parse_ts(ts_from),
            ts_to=queries.parse_ts(ts_to),
            limit=limit,
        )
        return queries.to_jsonable(rows)

    @mcp.tool()
    async def get_orderbook_at(
        symbol: str,
        ts: str,
        exchange: str | None = None,
        segment: str | None = None,
    ) -> dict[str, Any]:
        """Reconstruct the full order book for `symbol` at a point in time
        `ts` (ISO-8601) from the latest snapshot at/before `ts` plus replayed
        diff events. Raises if no snapshot exists yet before `ts`."""
        parsed_ts = queries.parse_ts(ts)
        assert parsed_ts is not None
        result = await queries.get_orderbook_at(
            ch_client.client,
            exchange=exchange or config.default_exchange,
            segment=segment or config.default_segment,
            symbol=symbol.upper(),
            ts=parsed_ts,
        )
        return queries.to_jsonable(
            {
                "symbol": result.symbol,
                "bids": result.bids,
                "asks": result.asks,
                "snapshot_ts": result.snapshot_ts,
                "events_applied": result.events_applied,
                "last_update_id": result.last_update_id,
            }
        )

    @mcp.tool()
    async def get_orderbook_events(
        symbol: str,
        ts_from: str | None = None,
        ts_to: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
        exchange: str | None = None,
        segment: str | None = None,
    ) -> dict[str, Any]:
        """Fetch raw order-book diff events for `symbol`, paginated via the
        opaque `cursor` returned in `next_cursor` (pass it back to continue)."""
        result = await queries.get_orderbook_events(
            ch_client.client,
            exchange=exchange or config.default_exchange,
            segment=segment or config.default_segment,
            symbol=symbol.upper(),
            ts_from=queries.parse_ts(ts_from),
            ts_to=queries.parse_ts(ts_to),
            limit=limit,
            cursor=cursor,
        )
        return queries.to_jsonable(result)

    @mcp.tool()
    async def get_derivatives_metrics(
        symbol: str,
        ts_from: str | None = None,
        ts_to: str | None = None,
        exchange: str | None = None,
        segment: str | None = None,
    ) -> dict[str, Any]:
        """Fetch derivatives metrics for `symbol` in an optional [ts_from, ts_to]
        ISO-8601 UTC time range: funding rate + mark/index price, open interest,
        and liquidations, combined (docs/03-mvp-scope.md: "funding + OI +
        liquidations together"). The `liquidations` list reflects Binance's
        `forceOrder` stream, which is a partial tape (only the largest
        liquidation per symbol per 1000ms window) -- see `GET /known-limitations`
        / `liquidation_partial_coverage` for the full caveat."""
        result = await queries.get_derivatives_metrics(
            ch_client.client,
            exchange=exchange or config.default_exchange,
            segment=segment or config.default_segment,
            symbol=symbol.upper(),
            ts_from=queries.parse_ts(ts_from),
            ts_to=queries.parse_ts(ts_to),
        )
        return queries.to_jsonable(result)

    @mcp.tool()
    async def list_data_incidents(
        symbol: str | None = None,
        ts_from: str | None = None,
        ts_to: str | None = None,
        exchange: str | None = None,
        segment: str | None = None,
    ) -> list[dict[str, Any]]:
        """List data-quality incidents (disconnects, order-book sequence
        breaks, etc.) optionally scoped to `symbol` and a time range."""
        rows = await queries.list_incidents(
            ch_client.client,
            exchange=exchange or config.default_exchange,
            segment=segment or config.default_segment,
            symbol=symbol.upper() if symbol else None,
            ts_from=queries.parse_ts(ts_from),
            ts_to=queries.parse_ts(ts_to),
        )
        return queries.to_jsonable(rows)

    return mcp
