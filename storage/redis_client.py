"""
storage/redis_client.py - Async Redis client with connection pooling.
"""
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)
_pool: Optional[ConnectionPool] = None

def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool.from_url(
            settings.redis_url, max_connections=settings.redis_max_connections,
            decode_responses=True,
        )
    return _pool

def get_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=get_pool())

async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None

class RedisStore:
    def __init__(self) -> None:
        self._client = get_client()

    async def frontier_push(self, url: str) -> None:
        await self._client.rpush("frontier:queue", url)

    async def frontier_pop(self) -> Optional[str]:
        return await self._client.lpop("frontier:queue")

    async def frontier_size(self) -> int:
        return await self._client.llen("frontier:queue")

    async def mark_seen(self, url_hash: str) -> bool:
        return bool(await self._client.sadd("seen:urls", url_hash))

    async def is_seen(self, url_hash: str) -> bool:
        return bool(await self._client.sismember("seen:urls", url_hash))

    async def seen_count(self) -> int:
        return await self._client.scard("seen:urls")

    async def set_postings(self, shard: int, term: str, postings: list[dict]) -> None:
        await self._client.set(f"index:shard{shard}:{term}", json.dumps(postings))

    async def get_postings(self, shard: int, term: str) -> list[dict]:
        raw = await self._client.get(f"index:shard{shard}:{term}")
        return json.loads(raw) if raw else []

    async def set_index_stats(self, stats: dict[str, Any]) -> None:
        await self._client.set("index:stats", json.dumps(stats))

    async def get_index_stats(self) -> dict[str, Any]:
        raw = await self._client.get("index:stats")
        return json.loads(raw) if raw else {}

    async def set_doc_meta(self, doc_id: str, meta: dict[str, Any]) -> None:
        await self._client.set(f"doc:{doc_id}", json.dumps(meta))

    async def get_doc_meta(self, doc_id: str) -> Optional[dict[str, Any]]:
        raw = await self._client.get(f"doc:{doc_id}")
        return json.loads(raw) if raw else None

    async def mget_doc_meta(self, doc_ids: list[str]) -> list[Optional[dict]]:
        if not doc_ids:
            return []
        keys = [f"doc:{doc_id}" for doc_id in doc_ids]
        raws = await self._client.mget(keys)
        return [json.loads(r) if r else None for r in raws]

    async def cache_set(self, key: str, value: Any, ttl: int) -> None:
        await self._client.setex(f"cache:{key}", ttl, json.dumps(value))

    async def cache_get(self, key: str) -> Optional[Any]:
        raw = await self._client.get(f"cache:{key}")
        return json.loads(raw) if raw else None

    async def ping(self) -> bool:
        try:
            return await self._client.ping()
        except Exception:
            return False
