from __future__ import annotations
import logging
import time
from typing import Dict
import asyncpg

logger = logging.getLogger("aggregator.stats")
_start_time: float = time.time()

def get_start_time() -> float:
    return _start_time

def reset_start_time() -> None:
    global _start_time
    _start_time = time.time()

async def increment_stat(pool: asyncpg.Pool, key: str, amount: int = 1) -> None:
    async with pool.acquire() as conn:
        await conn.execute("UPDATE stats SET value = value + $1 WHERE key = $2", amount, key)

async def increment_stat_conn(conn: asyncpg.Connection, key: str, amount: int = 1) -> None:
    await conn.execute("UPDATE stats SET value = value + $1 WHERE key = $2", amount, key)

async def get_all_stats(pool: asyncpg.Pool) -> Dict[str, object]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM stats")
        stats = {row["key"]: row["value"] for row in rows}
        topic_count = await conn.fetchval("SELECT COUNT(DISTINCT topic) FROM events")
        uptime = time.time() - _start_time

        received = stats.get("received_total", 0)
        unique = stats.get("unique_processed", 0)
        duplicates = stats.get("duplicates_dropped", 0)

        duplicate_rate = round(duplicates / received, 4) if received > 0 else 0.0
        throughput = round(unique / uptime, 2) if uptime > 0 else 0.0

    return {
        "received_total": stats.get("received_total", 0),
        "queued_total": stats.get("queued_total", 0),
        "unique_processed": stats.get("unique_processed", 0),
        "duplicates_dropped": stats.get("duplicates_dropped", 0),
        "failed_total": stats.get("failed_total", 0),
        "topic_count": topic_count or 0,
        "uptime_seconds": round(uptime, 2),
        "duplicate_rate": duplicate_rate,
        "throughput_events_per_second": throughput,
    }
