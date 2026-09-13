# Binance BTCUSDT spot normalizer -- consumes raw Redis Streams messages,
# parses into the unified ClickHouse schema, tracks order-book state,
# detects incidents. See docs/04-architecture/00-overview.md, §6.2/§6.3.1.
FROM python:3.12.7-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/common /app/src/common
COPY src/normalizer /app/src/normalizer

# Runs as non-root by default in python:*-slim only if a user is created;
# add one explicitly for defense in depth.
RUN useradd --create-home --uid 1000 normalizer
USER normalizer

# Config is env-driven: REDIS_URL, REDIS_STREAM_DB, EXCHANGE, SEGMENT, SYMBOL,
# LOG_LEVEL, CONSUMER_GROUP, CONSUMER_NAME, REDIS_READ_BLOCK_MS,
# REDIS_READ_COUNT, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE,
# CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, SINK_BATCH_SIZE,
# SINK_FLUSH_INTERVAL_S, BOOK_SNAPSHOT_INTERVAL_S.
# Defaults (see src/normalizer/config.py) already target binance/spot/BTCUSDT.
CMD ["python", "-m", "normalizer.main"]
