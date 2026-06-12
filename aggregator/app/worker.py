"""
worker.py — Redis Stream consumer / Idempotent Worker.

Worker ini membaca event dari Redis Stream dan memprosesnya ke PostgreSQL
dengan jaminan idempotency dan deduplication.

=== STRATEGI DEDUPLICATION & TRANSAKSI ===

Alur pemrosesan setiap event:
1. XREADGROUP: baca event dari Redis Stream (consumer group)
2. BEGIN TRANSACTION
3. INSERT INTO processed_events(topic, event_id) ON CONFLICT DO NOTHING
4. Cek rowcount:
   - Jika rowcount = 1 → event BARU → INSERT ke tabel events + increment unique_processed
   - Jika rowcount = 0 → event DUPLIKAT → increment duplicates_dropped
5. COMMIT TRANSACTION
6. XACK: acknowledge event di Redis Stream

=== CONCURRENCY CONTROL ===

Mekanisme ini mencegah race condition ketika banyak worker memproses
event yang sama secara paralel:

- UNIQUE constraint (topic, event_id) pada tabel processed_events adalah
  mekanisme concurrency control utama.
- INSERT ... ON CONFLICT DO NOTHING bersifat atomic di PostgreSQL.
- Jika dua worker mencoba INSERT event yang sama secara bersamaan,
  hanya SATU yang berhasil (rowcount=1), yang lain mendapat conflict (rowcount=0).
- Isolation level READ COMMITTED sudah cukup karena constraint unik
  menjadi penjaga utama untuk konflik insert.
- Transaksi memastikan insert event + update stats dilakukan secara atomic.

Referensi: PostgreSQL Documentation - INSERT ON CONFLICT
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from app.broker import CONSUMER_GROUP, STREAM_NAME, get_redis, init_broker
from app.db import get_pool, init_db, close_db
from app.broker import close_broker
from app.migrations import run_migrations
from app.stats import increment_stat_conn

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")

# Worker identity
WORKER_NAME = os.environ.get("WORKER_NAME", "worker-default")

# Graceful shutdown flag
_shutdown = False


def handle_shutdown(signum, frame):
    """Signal handler untuk graceful shutdown."""
    global _shutdown
    logger.info(f"🛑 {WORKER_NAME} received shutdown signal ({signum}), stopping...")
    _shutdown = True


async def process_event(pool, event_data: dict) -> None:
    """
    Proses satu event dengan deduplication dan transaksi.

    === TRANSACTION BOUNDARY ===
    Semua operasi berikut dilakukan dalam SATU transaksi:
    1. Insert ke processed_events (dedup check)
    2. Insert ke events (jika baru)
    3. Update stats counter

    Ini memastikan konsistensi: jika salah satu step gagal,
    seluruh operasi di-rollback.

    === IDEMPOTENT WRITE PATTERN ===
    INSERT ... ON CONFLICT DO NOTHING menghasilkan:
    - rowcount = 1 jika event belum pernah diproses (baru)
    - rowcount = 0 jika event sudah pernah diproses (duplikat)

    Pattern ini idempotent: mengirim event yang sama berkali-kali
    selalu menghasilkan state database yang sama.
    """
    topic = event_data.get("topic")
    event_id = event_data.get("event_id")
    timestamp_str = event_data.get("timestamp")
    source = event_data.get("source")
    payload = event_data.get("payload", {})

    if not all([topic, event_id, timestamp_str, source]):
        logger.warning(f"⚠️  {WORKER_NAME} skipping malformed event: {event_data}")
        async with pool.acquire() as conn:
            await increment_stat_conn(conn, "failed_total")
        return

    # Parse timestamp
    try:
        event_timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(f"⚠️  {WORKER_NAME} invalid timestamp for event {event_id}: {timestamp_str}")
        async with pool.acquire() as conn:
            await increment_stat_conn(conn, "failed_total")
        return

    # =========================================================================
    # TRANSACTION: Deduplication + Insert + Stats Update
    # =========================================================================
    # Menggunakan transaksi database untuk memastikan atomicity.
    # Jika dua worker memproses event yang sama secara bersamaan:
    # - Worker pertama: INSERT berhasil (rowcount=1) → proses event
    # - Worker kedua: INSERT conflict (rowcount=0) → skip sebagai duplikat
    # PostgreSQL UNIQUE constraint menjamin hanya satu worker yang berhasil.
    # =========================================================================
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: Attempt dedup insert
            # ON CONFLICT DO NOTHING = idempotent write pattern
            result = await conn.execute(
                """
                INSERT INTO processed_events (topic, event_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                topic,
                event_id,
            )

            # Step 2: Check if insert succeeded
            # 'INSERT 0 1' = berhasil (event baru)
            # 'INSERT 0 0' = conflict (event duplikat)
            rows_inserted = int(result.split()[-1])

            if rows_inserted == 1:
                # ---- EVENT BARU: proses dan simpan ----
                await conn.execute(
                    """
                    INSERT INTO events (topic, event_id, event_timestamp, source, payload, processed_at)
                    VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    topic,
                    event_id,
                    event_timestamp,
                    source,
                    json.dumps(payload),
                )

                # Update stats: unique_processed
                await increment_stat_conn(conn, "unique_processed")

                logger.info(
                    f"✅ {WORKER_NAME} processed event "
                    f"topic={topic} event_id={event_id}"
                )
            else:
                # ---- EVENT DUPLIKAT: skip processing ----
                await increment_stat_conn(conn, "duplicates_dropped")

                logger.info(
                    f"🔄 {WORKER_NAME} duplicate dropped "
                    f"topic={topic} event_id={event_id}"
                )


async def consumer_loop() -> None:
    """
    Main consumer loop: membaca event dari Redis Stream menggunakan consumer group.

    Consumer group 'log-workers' memungkinkan:
    - Multiple worker membaca dari stream yang sama
    - Redis mendistribusikan event ke worker secara round-robin
    - Event yang sudah di-ACK tidak akan diberikan lagi
    - Jika worker crash sebelum ACK, event bisa di-claim oleh worker lain

    XREADGROUP:
    - GROUP: nama consumer group
    - CONSUMER: nama worker (unik per worker instance)
    - COUNT: jumlah event dibaca per batch
    - BLOCK: timeout menunggu event baru (ms)
    - ID '>': hanya baca event baru yang belum di-assign
    """
    pool = await get_pool()
    redis = await get_redis()

    logger.info(f"🚀 {WORKER_NAME} starting consumer loop on stream '{STREAM_NAME}'")

    while not _shutdown:
        try:
            # XREADGROUP: baca event baru dari stream
            messages = await redis.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=WORKER_NAME,
                streams={STREAM_NAME: ">"},
                count=10,
                block=2000,  # Block 2 detik menunggu event baru
            )

            if not messages:
                continue

            for stream_name, entries in messages:
                for entry_id, fields in entries:
                    try:
                        # Parse event data dari Redis Stream entry
                        event_data = json.loads(fields.get("data", "{}"))

                        # Proses event dengan dedup + transaksi
                        await process_event(pool, event_data)

                    except json.JSONDecodeError as e:
                        logger.error(f"❌ {WORKER_NAME} failed to parse event: {e}")
                        async with pool.acquire() as conn:
                            await increment_stat_conn(conn, "failed_total")

                    except Exception as e:
                        logger.error(
                            f"❌ {WORKER_NAME} error processing event "
                            f"stream_id={entry_id}: {e}"
                        )
                        async with pool.acquire() as conn:
                            await increment_stat_conn(conn, "failed_total")

                    finally:
                        # XACK: acknowledge event di Redis Stream
                        # Event yang sudah di-ACK tidak akan di-deliver lagi
                        await redis.xack(STREAM_NAME, CONSUMER_GROUP, entry_id)

        except asyncio.CancelledError:
            logger.info(f"🛑 {WORKER_NAME} consumer loop cancelled")
            break
        except Exception as e:
            logger.error(f"❌ {WORKER_NAME} consumer loop error: {e}")
            await asyncio.sleep(1)  # Backoff sebelum retry

    logger.info(f"👋 {WORKER_NAME} consumer loop stopped")


async def main() -> None:
    """Entry point untuk worker."""
    logger.info(f"🏁 Starting worker: {WORKER_NAME}")

    # Setup signal handlers untuk graceful shutdown
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        # Inisialisasi database dan broker
        pool = await init_db()
        await run_migrations(pool)
        await init_broker()

        logger.info(f"✅ {WORKER_NAME} initialized successfully")

        # Jalankan consumer loop
        await consumer_loop()

    except Exception as e:
        logger.error(f"❌ {WORKER_NAME} fatal error: {e}")
        raise
    finally:
        await close_broker()
        await close_db()
        logger.info(f"👋 {WORKER_NAME} shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
