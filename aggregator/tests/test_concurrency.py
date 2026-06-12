"""
test_concurrency.py — Test concurrency dan race condition.

Test Cases:
13. Concurrent duplicate insert tetap menghasilkan satu event unik (TC-13)
14. 100 event dengan 30% duplikat → jumlah unik sesuai ekspektasi (TC-14)
"""

import pytest
import asyncio

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-13: Concurrent duplicate insert = satu event unik
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_duplicate_insert(db_pool):
    """
    Simulasi race condition: 10 task asyncio memproses event yang SAMA
    secara bersamaan. Hasil akhir harus tetap hanya 1 event di database.

    Ini menguji bahwa UNIQUE constraint + ON CONFLICT DO NOTHING
    berfungsi sebagai concurrency control:
    - Hanya satu INSERT yang berhasil (rowcount=1)
    - Sisanya mendapat conflict (rowcount=0)
    - Tidak ada duplicate row di database
    """
    event = make_event(
        topic="concurrency.test",
        event_id="race-condition-001",
        payload={"test": "concurrent"},
    )

    # Jalankan 10 worker secara bersamaan memproses event yang sama
    tasks = [process_event(db_pool, event) for _ in range(10)]
    await asyncio.gather(*tasks)

    # Verifikasi: hanya 1 event di database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE topic=$1 AND event_id=$2",
            "concurrency.test",
            "race-condition-001",
        )

    assert count == 1, (
        f"Expected exactly 1 event in database, found {count}. "
        "Race condition detected — concurrent inserts created duplicates."
    )

    # Verifikasi stats: unique_processed=1, duplicates_dropped=9
    async with db_pool.acquire() as conn:
        unique = await conn.fetchval(
            "SELECT value FROM stats WHERE key='unique_processed'"
        )
        dupes = await conn.fetchval(
            "SELECT value FROM stats WHERE key='duplicates_dropped'"
        )

    assert unique == 1, f"Expected unique_processed=1, got {unique}"
    assert dupes == 9, f"Expected duplicates_dropped=9, got {dupes}"


# ---------------------------------------------------------------------------
# TC-14: 100 event dengan 30% duplikat → jumlah unik sesuai ekspektasi
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_100_events_with_30_percent_duplicates(client, db_pool):
    """
    Simulasi 100 event total:
    - 70 event unik
    - 30 event duplikat (diambil random dari 70 event unik)

    Setelah semua diproses, jumlah event unik di database harus = 70.
    """
    import random

    # Generate 70 event unik
    unique_events = [
        make_event(
            topic="bulk.test",
            event_id=f"bulk-{i:04d}",
            payload={"index": i},
        )
        for i in range(70)
    ]

    # Generate 30 duplikat dari event unik
    duplicate_events = [random.choice(unique_events) for _ in range(30)]

    # Gabung dan shuffle
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)

    assert len(all_events) == 100

    # Proses semua event
    for event in all_events:
        await process_event(db_pool, event)

    # Verifikasi jumlah event unik di database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE topic='bulk.test'"
        )

    assert count == 70, (
        f"Expected 70 unique events, found {count}. "
        "Deduplication with 30% duplicate rate failed."
    )

    # Verifikasi stats
    resp = await client.get("/stats")
    data = resp.json()

    assert data["unique_processed"] == 70
    assert data["duplicates_dropped"] == 30
