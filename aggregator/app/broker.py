from __future__ import annotations
import json
import logging
import os
from typing import Any, Dict
import redis.asyncio as aioredis

logger = logging.getLogger("aggregator.broker")
_redis: aioredis.Redis | None = None
STREAM_NAME = "events_stream"
CONSUMER_GROUP = "log-workers"

def get_redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://redis:6379/0")

async def init_broker() -> aioredis.Redis:
    global _redis
    if _redis is not None:
        return _redis

    redis_url = get_redis_url()
    _redis = aioredis.from_url(redis_url, decode_responses=True)

    try:
        await _redis.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    return _redis

async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await init_broker()
    return _redis

async def close_broker() -> None:
    global _redis
    if _redis is not None:
        await _redis.close()
        _redis = None

async def publish_to_stream(event: Dict[str, Any]) -> str:
    r = await get_redis()
    event_data = json.dumps(event, default=str)
    return await r.xadd(STREAM_NAME, {"data": event_data})

async def check_broker_health() -> bool:
    try:
        r = await get_redis()
        return await r.ping() is True
    except Exception:
        return False
