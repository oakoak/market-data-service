"""Entrypoint: wire config -> Redis client -> ClickHouse sink -> stream
consumer tasks (trades, depth, and, from Phase B (docs/08-prototype-
roadmap.md), mark_price/liquidation/poll) + the periodic book-snapshot
task, run forever.
"""

from __future__ import annotations

import asyncio
import sys

import redis.asyncio as redis

from common import get_logger
from normalizer.clickhouse_sink import ClickHouseSink
from normalizer.config import Config, load_config
from normalizer.consumer import NormalizerContext, StreamConsumer


async def _main() -> None:
    config = load_config()
    log = get_logger("normalizer.main", level=config.log_level)
    log.info(
        "starting normalizer",
        extra={
            "exchange": config.exchange,
            "segment": config.segment,
            "symbol": config.symbol,
            "redis_url": config.redis_url,
            "redis_stream_db": config.redis_stream_db,
            "clickhouse_host": config.clickhouse_host,
            "clickhouse_port": config.clickhouse_port,
            "trades_stream": config.trades_stream,
            "depth_stream": config.depth_stream,
            "mark_price_stream": config.mark_price_stream,
            "liquidation_stream": config.liquidation_stream,
            "poll_stream": config.poll_stream,
            "continuity_mode": config.continuity_mode,
            "consumer_group": config.consumer_group,
        },
    )

    redis_client = redis.from_url(config.redis_url, db=config.redis_stream_db, decode_responses=True)
    await redis_client.ping()

    sink = ClickHouseSink(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        database=config.clickhouse_database,
        user=config.clickhouse_user,
        password=config.clickhouse_password,
        batch_size=config.sink_batch_size,
        flush_interval_s=config.sink_flush_interval_s,
    )
    await sink.connect()

    ctx = NormalizerContext(
        exchange=config.exchange,
        segment=config.segment,
        symbol=config.symbol,
        sink=sink,
        continuity=config.continuity_mode,
    )

    trades_consumer = StreamConsumer(
        redis_client=redis_client,
        stream=config.trades_stream,
        group=config.consumer_group,
        consumer_name=f"{config.consumer_name}-trades",
        block_ms=config.read_block_ms,
        count=config.read_count,
        handler=ctx.handle_trade,
    )
    depth_consumer = StreamConsumer(
        redis_client=redis_client,
        stream=config.depth_stream,
        group=config.consumer_group,
        consumer_name=f"{config.consumer_name}-depth",
        block_ms=config.read_block_ms,
        count=config.read_count,
        handler=ctx.handle_depth,
    )
    # Phase B (docs/08-prototype-roadmap.md): USDT-M perp streams. These are
    # additive -- a spot-only deployment's collector never publishes to
    # them, so these consumers just sit blocked on an empty/nonexistent
    # stream (XREADGROUP with mkstream=True creates it if needed) rather
    # than erroring.
    mark_price_consumer = StreamConsumer(
        redis_client=redis_client,
        stream=config.mark_price_stream,
        group=config.consumer_group,
        consumer_name=f"{config.consumer_name}-mark-price",
        block_ms=config.read_block_ms,
        count=config.read_count,
        handler=ctx.handle_mark_price,
    )
    liquidation_consumer = StreamConsumer(
        redis_client=redis_client,
        stream=config.liquidation_stream,
        group=config.consumer_group,
        consumer_name=f"{config.consumer_name}-liquidation",
        block_ms=config.read_block_ms,
        count=config.read_count,
        handler=ctx.handle_liquidation,
    )
    poll_consumer = StreamConsumer(
        redis_client=redis_client,
        stream=config.poll_stream,
        group=config.consumer_group,
        consumer_name=f"{config.consumer_name}-poll",
        block_ms=config.read_block_ms,
        count=config.read_count,
        handler=ctx.handle_poll,
    )

    tasks = [
        asyncio.create_task(trades_consumer.run()),
        asyncio.create_task(depth_consumer.run()),
        asyncio.create_task(mark_price_consumer.run()),
        asyncio.create_task(liquidation_consumer.run()),
        asyncio.create_task(poll_consumer.run()),
        asyncio.create_task(ctx.periodic_book_snapshot_loop(config.book_snapshot_interval_s)),
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await sink.close()
        await redis_client.aclose()


def main() -> None:
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
