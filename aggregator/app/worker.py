from __future__ import annotations
import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

from app.broker import CONSUMER_GROUP, STREAM_NAME, get_redis, init_broker, close_broker
from app.db import get_pool, init_db, close_db
from app.migrations import run_migrations
from app.stats import increment_stat_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("worker")

WORKER_NAME = os.environ.get("WORKER_NAME", "worker-default")
_shutdown = False

def handle_shutdown(signum, frame):
    global _shutdown
    _shutdown = True

async def process_event(pool, event_data: dict) -> None:
    topic = event_data.get("topic")
    event_id = event_data.get("event_id")
    timestamp_str = event_data.get("timestamp")
    source = event_data.get("source")
    payload = event_data.get("payload", {})

    if not all([topic, event_id, timestamp_str, source]):
        async with pool.acquire() as conn:
            await increment_stat_conn(conn, "failed_total")
        return

    try:
        event_timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        async with pool.acquire() as conn:
            await increment_stat_conn(conn, "failed_total")
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.execute(
                "INSERT INTO processed_events (topic, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                topic, event_id
            )
            rows_inserted = int(result.split()[-1])

            if rows_inserted == 1:
                await conn.execute(
                    "INSERT INTO events (topic, event_id, event_timestamp, source, payload, processed_at) VALUES ($1, $2, $3, $4, $5::jsonb, NOW()) ON CONFLICT (topic, event_id) DO NOTHING",
                    topic, event_id, event_timestamp, source, json.dumps(payload)
                )
                await increment_stat_conn(conn, "unique_processed")
            else:
                await increment_stat_conn(conn, "duplicates_dropped")

async def consumer_loop() -> None:
    pool = await get_pool()
    redis = await get_redis()

    while not _shutdown:
        try:
            messages = await redis.xreadgroup(groupname=CONSUMER_GROUP, consumername=WORKER_NAME, streams={STREAM_NAME: ">"}, count=10, block=2000)
            if not messages:
                continue

            for stream_name, entries in messages:
                for entry_id, fields in entries:
                    try:
                        event_data = json.loads(fields.get("data", "{}"))
                        await process_event(pool, event_data)
                    except Exception:
                        async with pool.acquire() as conn:
                            await increment_stat_conn(conn, "failed_total")
                    finally:
                        await redis.xack(STREAM_NAME, CONSUMER_GROUP, entry_id)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1)

async def main() -> None:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    try:
        pool = await init_db()
        await run_migrations(pool)
        await init_broker()
        await consumer_loop()
    finally:
        await close_broker()
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
