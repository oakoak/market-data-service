"""Redis Streams sink -- the collector's only side effect.

Dumb-collector contract (docs/04-architecture/00-overview.md §6.2,
docs/04-architecture/01-stack.md): every message pushed here is raw
bytes/JSON plus receive_ts, never parsed into the unified schema. Broker
streams live in a separate logical Redis DB from the (not-yet-implemented)
cache DB, per 01-stack.md's noeviction-vs-allkeys-lru split -- the actual
`maxmemory-policy noeviction` setting is configured at the Redis *service*
level in docker-compose (owned by another agent), this module just makes
sure we write into the DB reserved for the broker role (`SELECT`, default 1).
"""

from __future__ import annotations

import json
import time
from typing import Any

import redis.asyncio as redis


class RedisStreamSink:
    def __init__(self, redis_url: str, db: int, stream_maxlen: int) -> None:
        self._redis_url = redis_url
        self._db = db
        self._stream_maxlen = stream_maxlen
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        self._client = redis.from_url(self._redis_url, db=self._db, decode_responses=True)
        await self._client.ping()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def publish(self, stream: str, payload: dict[str, Any]) -> str:
        """XADD a single JSON-encoded message. Field name is `payload` -- the
        whole message shape is `{"type", "receive_ts", "raw"/"snapshot": ...}`
        JSON-encoded into one Redis stream field, rather than exploding raw
        exchange fields into individual Redis field/value pairs. This keeps
        the collector genuinely dumb (no schema knowledge of `raw`'s
        contents) and keeps the entry atomic to read back with XREAD/XRANGE.
        """
        assert self._client is not None, "call connect() first"
        return await self._client.xadd(
            stream,
            {"payload": json.dumps(payload, default=str)},
            maxlen=self._stream_maxlen,
            approximate=True,
        )


def now_ts_ms() -> int:
    """receive_ts in epoch milliseconds -- local receive time at the
    collector, i.e. queue-entry time (docs/04-architecture/00-overview.md,
    dual-timestamp requirement; ts_exchange is added later by the
    normalizer from the raw payload's own `T`/`E` fields)."""
    return time.time_ns() // 1_000_000
