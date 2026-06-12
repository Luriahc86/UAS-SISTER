"""
stats.py — Modul statistik sistem.

Menyediakan fungsi untuk:
- Increment counter statistik secara atomic
- Query semua metrik sistem
- Menghitung derived metrics (duplicate_rate, throughput, topic_count)

Semua update stats menggunakan atomic SQL UPDATE untuk mencegah
race condition saat multiple worker mengupdate counter secara bersamaan.
"""

from __future__ import annotations

import logging
import time
from typing import Dict

import asyncpg

logger = logging.getLogger("aggregator.stats")

# Waktu startup untuk menghitung uptime
_start_time: float = time.time()


def get_start_time() -> float:
    """Dapatkan waktu startup."""
    return _start_time


def reset_start_time() -> None:
    """Reset waktu startup (untuk testing)."""
    global _start_time
    _start_time = time.time()


async def increment_stat(pool: asyncpg.Pool, key: str, amount: int = 1) -> None:
    """
    Increment counter statistik secara atomic.

    Menggunakan atomic UPDATE ... SET value = value + $amount
    untuk mencegah race condition saat beberapa worker
    mengupdate counter yang sama secara bersamaan.

    Args:
        pool: asyncpg connection pool
        key: nama counter (e.g. 'received_total', 'duplicates_dropped')
        amount: jumlah increment (default 1)
    """
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE stats SET value = value + $1 WHERE key = $2",
            amount,
            key,
        )


async def increment_stat_conn(conn: asyncpg.Connection, key: str, amount: int = 1) -> None:
    """
    Increment counter statistik dalam koneksi/transaksi yang sudah ada.

    Versi ini digunakan oleh worker agar update stats
    bisa dilakukan dalam transaksi yang sama dengan insert event.

    Args:
        conn: asyncpg connection (bisa dalam transaksi)
        key: nama counter
        amount: jumlah increment
    """
    await conn.execute(
        "UPDATE stats SET value = value + $1 WHERE key = $2",
        amount,
        key,
    )


async def get_all_stats(pool: asyncpg.Pool) -> Dict[str, object]:
    """
    Query semua metrik sistem termasuk derived metrics.

    Menggabungkan:
    - Counter dari tabel stats
    - topic_count dari tabel events (COUNT DISTINCT)
    - uptime_seconds (computed dari startup time)
    - duplicate_rate (computed: duplicates / received)
    - throughput_events_per_second (computed: unique / uptime)
    """
    async with pool.acquire() as conn:
        # Ambil semua counter dari tabel stats
        rows = await conn.fetch("SELECT key, value FROM stats")
        stats = {row["key"]: row["value"] for row in rows}

        # Hitung topic_count dari tabel events
        topic_count = await conn.fetchval(
            "SELECT COUNT(DISTINCT topic) FROM events"
        )

        # Hitung uptime
        uptime = time.time() - _start_time

        # Ambil nilai counter
        received = stats.get("received_total", 0)
        unique = stats.get("unique_processed", 0)
        duplicates = stats.get("duplicates_dropped", 0)

        # Hitung duplicate_rate
        duplicate_rate = 0.0
        if received > 0:
            duplicate_rate = round(duplicates / received, 4)

        # Hitung throughput
        throughput = 0.0
        if uptime > 0:
            throughput = round(unique / uptime, 2)

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
