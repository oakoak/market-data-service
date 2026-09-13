"""Entrypoint: wire config -> Redis client -> ClickHouse sink -> two stream
consumer tasks (trades, depth) + the periodic book-snapshot task, run forever.
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

    tasks = [
        asyncio.create_task(trades_consumer.run()),
        asyncio.create_task(depth_consumer.run()),
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
