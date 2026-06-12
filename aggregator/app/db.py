"""
db.py — Koneksi database PostgreSQL menggunakan asyncpg.

Modul ini menyediakan connection pool yang digunakan oleh:
- main.py (API endpoints untuk query events & stats)
- worker.py (consumer untuk insert events & dedup)

Connection pool di-inisialisasi saat startup dan ditutup saat shutdown.
Retry dengan backoff diterapkan untuk menunggu PostgreSQL siap.
"""

from __future__ import annotations

import asyncio
import logging
import os

import asyncpg

logger = logging.getLogger("aggregator.db")

# Global connection pool
_pool: asyncpg.Pool | None = None


def get_database_url() -> str:
    """Ambil DATABASE_URL dari environment variable."""
    url = os.environ.get("DATABASE_URL", "postgresql://app:app_password@postgres:5432/pubsubdb")
    return url


async def init_db(max_retries: int = 15, retry_delay: float = 2.0) -> asyncpg.Pool:
    """
    Inisialisasi connection pool ke PostgreSQL.

    Retry dengan backoff untuk menunggu database siap saat container startup.
    Ini penting karena PostgreSQL mungkin belum sepenuhnya ready saat
    aggregator/worker mulai berjalan.
    """
    global _pool

    if _pool is not None:
        return _pool

    database_url = get_database_url()

    for attempt in range(1, max_retries + 1):
        try:
            _pool = await asyncpg.create_pool(
                database_url,
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            logger.info("✅ Database connection pool created successfully")
            return _pool
        except (ConnectionRefusedError, asyncpg.CannotConnectNowError, OSError) as e:
            if attempt < max_retries:
                logger.warning(
                    f"⏳ Database not ready (attempt {attempt}/{max_retries}): {e}. "
                    f"Retrying in {retry_delay}s..."
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"❌ Failed to connect to database after {max_retries} attempts")
                raise

    raise RuntimeError("Failed to initialize database pool")


async def get_pool() -> asyncpg.Pool:
    """Dapatkan connection pool. Raise error jika belum diinisialisasi."""
    global _pool
    if _pool is None:
        _pool = await init_db()
    return _pool


async def close_db() -> None:
    """Tutup connection pool saat shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("🔒 Database connection pool closed")


async def check_db_health() -> bool:
    """Health check: test koneksi ke database."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            return result == 1
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
