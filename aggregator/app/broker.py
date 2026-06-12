"""
broker.py — Redis Stream helper untuk pub/sub internal.

Redis Stream digunakan sebagai message broker antara aggregator dan worker:
- aggregator: XADD event ke stream 'events_stream'
- worker: XREADGROUP dari consumer group 'log-workers'

Redis Stream dipilih karena:
1. Built-in consumer groups (scaling worker)
2. Message acknowledgment (XACK)
3. Persistence (AOF)
4. Ordering guarantee per-stream
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import redis.asyncio as aioredis

logger = logging.getLogger("aggregator.broker")

# Global Redis client
_redis: aioredis.Redis | None = None

# Constants
STREAM_NAME = "events_stream"
CONSUMER_GROUP = "log-workers"


def get_redis_url() -> str:
    """Ambil REDIS_URL dari environment variable."""
    return os.environ.get("REDIS_URL", "redis://redis:6379/0")


async def init_broker() -> aioredis.Redis:
    """
    Inisialisasi koneksi Redis dan buat consumer group jika belum ada.

    Consumer group 'log-workers' memungkinkan multiple worker membaca
    dari stream yang sama tanpa memproses event yang sama dua kali
    (at-most-once delivery per worker, combined with DB dedup for exactly-once).
    """
    global _redis

    if _redis is not None:
        return _redis

    redis_url = get_redis_url()
    _redis = aioredis.from_url(redis_url, decode_responses=True)

    # Buat consumer group jika belum ada
    try:
        await _redis.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(f"✅ Consumer group '{CONSUMER_GROUP}' created on stream '{STREAM_NAME}'")
    except aioredis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"ℹ️  Consumer group '{CONSUMER_GROUP}' already exists")
        else:
            raise

    logger.info("✅ Redis broker connection established")
    return _redis


async def get_redis() -> aioredis.Redis:
    """Dapatkan Redis client. Inisialisasi jika belum."""
    global _redis
    if _redis is None:
        _redis = await init_broker()
    return _redis


async def close_broker() -> None:
    """Tutup koneksi Redis saat shutdown."""
    global _redis
    if _redis is not None:
        await _redis.close()
        _redis = None
        logger.info("🔒 Redis broker connection closed")


async def publish_to_stream(event: Dict[str, Any]) -> str:
    """
    Publish satu event ke Redis Stream.

    Event di-serialize ke JSON string dan disimpan sebagai field 'data'
    di stream entry. Redis Stream akan memberikan ID unik untuk setiap entry.

    Args:
        event: Dict event yang sudah divalidasi

    Returns:
        Stream entry ID dari Redis
    """
    r = await get_redis()
    event_data = json.dumps(event, default=str)

    # XADD: menambahkan entry ke stream dengan auto-generated ID
    entry_id = await r.xadd(STREAM_NAME, {"data": event_data})
    logger.debug(
        f"📤 Event published to stream: topic={event.get('topic')} "
        f"event_id={event.get('event_id')} stream_id={entry_id}"
    )
    return entry_id


async def check_broker_health() -> bool:
    """Health check: test koneksi ke Redis."""
    try:
        r = await get_redis()
        result = await r.ping()
        return result is True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False
