"""
test_events.py — Test endpoint GET /events.

Test Cases:
7. Event unik diproses dan muncul di GET /events (TC-07)
8. Event dengan event_id sama tapi topic berbeda = event berbeda (TC-08)
"""

import pytest
import asyncio
import json

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-07: Event unik diproses dan muncul di GET /events
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unique_event_appears_in_events(client, db_pool):
    """
    Setelah worker memproses event, event tersebut harus muncul
    di response GET /events?topic=...
    """
    event = make_event(
        topic="payment.created",
        event_id="events-test-001",
        payload={"amount": 150000},
    )

    # Proses event langsung via worker (bypass Redis Stream)
    await process_event(db_pool, event)

    # Cek di endpoint /events
    resp = await client.get("/events", params={"topic": "payment.created"})
    assert resp.status_code == 200

    data = resp.json()
    assert data["topic"] == "payment.created"
    assert data["count"] >= 1

    # Cari event yang baru diproses
    event_ids = [e["event_id"] for e in data["events"]]
    assert "events-test-001" in event_ids


# ---------------------------------------------------------------------------
# TC-08: event_id sama tapi topic berbeda = event berbeda
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_same_event_id_different_topic_not_duplicate(client, db_pool):
    """
    Event dengan event_id yang sama tetapi topic berbeda
    TIDAK dianggap duplikat. Dedup key adalah (topic, event_id).
    """
    # Event 1: topic = payment.created
    event1 = make_event(
        topic="payment.created",
        event_id="shared-id-001",
        payload={"type": "payment"},
    )

    # Event 2: topic = order.created, event_id SAMA
    event2 = make_event(
        topic="order.created",
        event_id="shared-id-001",
        payload={"type": "order"},
    )

    # Proses kedua event
    await process_event(db_pool, event1)
    await process_event(db_pool, event2)

    # Cek: payment.created harus ada 1 event
    resp1 = await client.get("/events", params={"topic": "payment.created"})
    data1 = resp1.json()
    assert data1["count"] >= 1

    # Cek: order.created harus ada 1 event
    resp2 = await client.get("/events", params={"topic": "order.created"})
    data2 = resp2.json()
    assert data2["count"] >= 1

    # Kedua event harus ada (bukan duplikat)
    payment_ids = [e["event_id"] for e in data1["events"]]
    order_ids = [e["event_id"] for e in data2["events"]]
    assert "shared-id-001" in payment_ids
    assert "shared-id-001" in order_ids
