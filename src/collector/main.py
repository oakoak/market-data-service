"""Entrypoint: build the exchange adapter from env config and run the collector.

Config-driven exchange/instrument selection (EXCHANGE, SYMBOL, SEGMENT env
vars) without building a full multi-instrument orchestrator: this process
still runs exactly one adapter instance. Adding Bybit later means adding a
branch to `build_adapter`, not touching runner.py.
"""

from __future__ import annotations

import asyncio
import sys

from collector.adapter import ExchangeAdapter
from collector.config import Config, load_config
from collector.runner import CollectorRunner
from collector.sink import RedisStreamSink
from common import get_logger


def build_adapter(config: Config) -> ExchangeAdapter:
    if config.exchange == "binance" and config.segment == "spot":
        from exchanges.binance import BinanceSpotAdapter

        return BinanceSpotAdapter(symbol=config.symbol)

    if config.exchange == "binance" and config.segment == "usdtm":
        from exchanges.binance import BinanceUsdtmPerpAdapter

        return BinanceUsdtmPerpAdapter(symbol=config.symbol)

    raise NotImplementedError(
        f"No adapter registered for exchange={config.exchange!r} segment={config.segment!r}. "
        "Add one under src/exchanges/<exchange>/ and register it here."
    )


async def _main() -> None:
    config = load_config()
    log = get_logger("collector.main", level=config.log_level)
    log.info(
        "starting collector",
        extra={
            "exchange": config.exchange,
            "segment": config.segment,
            "symbol": config.symbol,
            "redis_url": config.redis_url,
            "redis_stream_db": config.redis_stream_db,
        },
    )

    adapter = build_adapter(config)
    sink = RedisStreamSink(
        redis_url=config.redis_url,
        db=config.redis_stream_db,
        stream_maxlen=config.stream_maxlen,
    )
    runner = CollectorRunner(config=config, adapter=adapter, sink=sink)
    await runner.run()


def main() -> None:
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
