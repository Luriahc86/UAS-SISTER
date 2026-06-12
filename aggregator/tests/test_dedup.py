"""
test_dedup.py — Test deduplication.

Test Cases:
9.  Event duplikat (same topic+event_id) hanya muncul sekali (TC-09)
10. Duplicate counter bertambah (TC-10)
"""

import pytest

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-09: Event duplikat hanya muncul sekali
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_event_only_appears_once(client, db_pool):
    """
    Mengirim event dengan (topic, event_id) yang sama 5 kali.
    Event hanya boleh muncul sekali di GET /events.

    Ini menguji mekanisme deduplication menggunakan:
    - UNIQUE constraint (topic, event_id) pada tabel events
    - INSERT ... ON CONFLICT DO NOTHING pada tabel processed_events
    """
    event = make_event(
        topic="payment.created",
        event_id="dedup-test-001",
        payload={"amount": 150000},
    )

    # Proses event yang sama 5 kali
    for _ in range(5):
        await process_event(db_pool, event)

    # Cek di /events: hanya boleh ada 1
    resp = await client.get("/events", params={"topic": "payment.created"})
    data = resp.json()

    # Hitung event dengan event_id ini
    matching = [e for e in data["events"] if e["event_id"] == "dedup-test-001"]
    assert len(matching) == 1, (
        f"Expected 1 event, found {len(matching)}. "
        "Deduplication failed — duplicate events were inserted."
    )


# ---------------------------------------------------------------------------
# TC-10: Duplicate counter bertambah
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_counter_increments(client, db_pool):
    """
    Setelah mengirim event yang sama beberapa kali,
    counter duplicates_dropped harus bertambah sesuai jumlah duplikat.

    Jika event dikirim 5 kali:
    - 1x diproses (unique)
    - 4x ditolak (duplicate)
    → duplicates_dropped harus >= 4
    """
    event = make_event(
        topic="payment.created",
        event_id="dedup-counter-001",
        payload={"amount": 200000},
    )

    # Proses event yang sama 5 kali
    for _ in range(5):
        await process_event(db_pool, event)

    # Cek stats
    resp = await client.get("/stats")
    data = resp.json()

    assert data["unique_processed"] >= 1
    assert data["duplicates_dropped"] >= 4, (
        f"Expected duplicates_dropped >= 4, got {data['duplicates_dropped']}. "
        "Duplicate counter is not incrementing correctly."
    )
