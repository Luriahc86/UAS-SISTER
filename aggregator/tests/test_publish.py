"""
test_publish.py — Test endpoint POST /publish.

Test Cases:
5. POST /publish single event mengembalikan accepted (TC-05)
6. POST /publish batch event mengembalikan jumlah accepted benar (TC-06)
"""

import pytest

from tests.conftest import make_event


# ---------------------------------------------------------------------------
# TC-05: POST /publish single event mengembalikan accepted
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_single_event(client):
    """Single event publish harus mengembalikan accepted=1, queued=1."""
    event = make_event(
        topic="payment.created",
        event_id="publish-single-001",
    )

    resp = await client.post("/publish", json=event)
    assert resp.status_code == 200

    data = resp.json()
    assert data["accepted"] == 1
    assert data["queued"] == 1
    assert data["message"] == "events accepted"


# ---------------------------------------------------------------------------
# TC-06: POST /publish batch event mengembalikan jumlah accepted benar
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_batch_events(client):
    """Batch publish 5 event harus mengembalikan accepted=5, queued=5."""
    events = [
        make_event(topic="payment.created", event_id=f"batch-{i:03d}")
        for i in range(5)
    ]

    resp = await client.post("/publish", json=events)
    assert resp.status_code == 200

    data = resp.json()
    assert data["accepted"] == 5
    assert data["queued"] == 5
    assert data["message"] == "events accepted"
