"""Builds the FastAPI app: mounts the REST router and the MCP server as a
sub-app at `/mcp`, and wires startup/shutdown to the ClickHouse read client's
connect()/close().

Mounting note (mcp==1.29.1's FastMCP): `streamable_http_app()` returns a
Starlette app with its own lifespan that starts the MCP session manager --
but a plain `app.mount(...)` does NOT forward ASGI lifespan events into
mounted sub-apps, so that lifespan would never run. FastMCP works around this
by exposing `mcp_server.session_manager` (public once `streamable_http_app()`
has been called) specifically "to enable advanced use cases like mounting
[...] in a single FastAPI application" -- so the main app's own lifespan
enters `session_manager.run()` directly instead of relying on the sub-app's.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.clickhouse_client import ClickHouseReadClient
from api.config import load_config
from api.mcp_tools import build_mcp_server
from api.rest import router as rest_router
from common import get_logger

config = load_config()
log = get_logger("api.main", level=config.log_level)

ch_client = ClickHouseReadClient(
    host=config.clickhouse_host,
    port=config.clickhouse_port,
    database=config.clickhouse_database,
    user=config.clickhouse_user,
    password=config.clickhouse_password,
)

mcp_server = build_mcp_server(ch_client, config)
# Must be called here (not inside lifespan) so mcp_server.session_manager
# exists by the time the lifespan below references it.
mcp_asgi_app = mcp_server.streamable_http_app()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "starting api",
        extra={
            "clickhouse_host": config.clickhouse_host,
            "clickhouse_port": config.clickhouse_port,
            "default_exchange": config.default_exchange,
            "default_segment": config.default_segment,
        },
    )
    await ch_client.connect()
    async with mcp_server.session_manager.run():
        yield
    await ch_client.close()


app = FastAPI(title="market-data-api", lifespan=lifespan)
app.state.ch_client = ch_client
app.state.config = config
app.include_router(rest_router)
app.mount("/mcp", mcp_asgi_app)
