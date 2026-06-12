"""
test_stats.py — Test endpoint GET /stats.

Test Cases:
11. /stats menghitung unique_processed dengan benar (TC-11)
12. /stats menghitung duplicates_dropped dengan benar (TC-12)
"""

import pytest

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-11: /stats menghitung unique_processed dengan benar
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stats_unique_processed_count(client, db_pool):
    """
    Setelah memproses 3 event unik, stats.unique_processed harus = 3.
    """
    for i in range(3):
        event = make_event(
            topic="payment.created",
            event_id=f"stats-unique-{i:03d}",
            payload={"index": i},
        )
        await process_event(db_pool, event)

    resp = await client.get("/stats")
    data = resp.json()

    assert data["unique_processed"] == 3, (
        f"Expected unique_processed=3, got {data['unique_processed']}"
    )


# ---------------------------------------------------------------------------
# TC-12: /stats menghitung duplicates_dropped dengan benar
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stats_duplicates_dropped_count(client, db_pool):
    """
    Proses 2 event unik + 3 duplikat.
    Expected: unique_processed=2, duplicates_dropped=3.
    """
    # 2 event unik
    event_a = make_event(topic="auth.login", event_id="stats-dup-a")
    event_b = make_event(topic="auth.login", event_id="stats-dup-b")

    await process_event(db_pool, event_a)
    await process_event(db_pool, event_b)

    # 3 duplikat (event_a dikirim 2x lagi, event_b 1x lagi)
    await process_event(db_pool, event_a)
    await process_event(db_pool, event_a)
    await process_event(db_pool, event_b)

    resp = await client.get("/stats")
    data = resp.json()

    assert data["unique_processed"] == 2, (
        f"Expected unique_processed=2, got {data['unique_processed']}"
    )
    assert data["duplicates_dropped"] == 3, (
        f"Expected duplicates_dropped=3, got {data['duplicates_dropped']}"
    )
