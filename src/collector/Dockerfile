# Binance BTCUSDT spot collector -- dumb collector, writes raw WS/REST
# messages + receive_ts to Redis Streams. See docs/04-architecture/01-stack.md.
FROM python:3.12.7-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/common /app/src/common
COPY src/collector /app/src/collector
COPY src/exchanges /app/src/exchanges

# Runs as non-root by default in python:*-slim only if a user is created;
# add one explicitly for defense in depth.
RUN useradd --create-home --uid 1000 collector
USER collector

# Config is env-driven: REDIS_URL, REDIS_STREAM_DB, EXCHANGE, SEGMENT, SYMBOL,
# LOG_LEVEL, REDIS_STREAM_MAXLEN, BACKOFF_INITIAL_S, BACKOFF_MAX_S.
# Defaults (see src/collector/config.py) already target binance/spot/BTCUSDT.
CMD ["python", "-m", "collector.main"]
