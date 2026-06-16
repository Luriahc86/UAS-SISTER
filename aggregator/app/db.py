from __future__ import annotations
import asyncio
import logging
import os
import asyncpg

logger = logging.getLogger("aggregator.db")
_pool: asyncpg.Pool | None = None

def get_database_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://app:app_password@postgres:5432/pubsubdb")

async def init_db(max_retries: int = 15, retry_delay: float = 2.0) -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool

    database_url = get_database_url()
    for attempt in range(1, max_retries + 1):
        try:
            _pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10, command_timeout=30)
            return _pool
        except (ConnectionRefusedError, asyncpg.CannotConnectNowError, OSError):
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                raise
    raise RuntimeError("Failed to initialize database pool")

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await init_db()
    return _pool

async def close_db() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

async def check_db_health() -> bool:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT 1") == 1
    except Exception:
        return False
