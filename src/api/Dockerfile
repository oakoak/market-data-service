# API service -- read-only REST + MCP serving layer over ClickHouse.
# See docs/04-architecture/00-overview.md §6.3 ("Serving: REST/WS API + MCP tools").
FROM python:3.12.7-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/common /app/src/common
COPY src/api /app/src/api

# Runs as non-root by default in python:*-slim only if a user is created;
# add one explicitly for defense in depth.
RUN useradd --create-home --uid 1000 api
USER api

# Config is env-driven: API_HOST, API_PORT, LOG_LEVEL, EXCHANGE, SEGMENT,
# CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USER,
# CLICKHOUSE_PASSWORD. Defaults (see src/api/config.py) already target
# binance/spot as query defaults.
EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
