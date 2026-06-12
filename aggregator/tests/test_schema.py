"""
test_schema.py — Test validasi schema event.

Test Cases:
1. Valid event schema diterima (TC-01)
2. Event tanpa topic ditolak (TC-02)
3. Event tanpa event_id ditolak (TC-03)
4. Event dengan timestamp invalid ditolak (TC-04)
"""

import pytest

from tests.conftest import make_event


# ---------------------------------------------------------------------------
# TC-01: Valid event schema diterima
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_valid_event_accepted(client):
    """Event dengan semua field valid harus diterima (HTTP 200)."""
    event = make_event(
        topic="payment.created",
        event_id="schema-test-001",
        timestamp="2026-06-12T10:00:00Z",
        source="test",
        payload={"amount": 150000},
    )

    resp = await client.post("/publish", json=event)
    assert resp.status_code == 200

    data = resp.json()
    assert data["accepted"] == 1
    assert data["queued"] == 1
    assert data["message"] == "events accepted"


# ---------------------------------------------------------------------------
# TC-02: Event tanpa topic ditolak
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_event_without_topic_rejected(client):
    """Event tanpa field 'topic' harus ditolak (HTTP 422)."""
    event = {
        "event_id": "no-topic-001",
        "timestamp": "2026-06-12T10:00:00Z",
        "source": "test",
        "payload": {"test": True},
    }

    resp = await client.post("/publish", json=event)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TC-03: Event tanpa event_id ditolak
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_event_without_event_id_rejected(client):
    """Event tanpa field 'event_id' harus ditolak (HTTP 422)."""
    event = {
        "topic": "payment.created",
        "timestamp": "2026-06-12T10:00:00Z",
        "source": "test",
        "payload": {"test": True},
    }

    resp = await client.post("/publish", json=event)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TC-04: Event dengan timestamp invalid ditolak
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_event_with_invalid_timestamp_rejected(client):
    """Event dengan timestamp yang bukan ISO8601 harus ditolak (HTTP 422)."""
    event = {
        "topic": "payment.created",
        "event_id": "bad-ts-001",
        "timestamp": "not-a-valid-timestamp",
        "source": "test",
        "payload": {"test": True},
    }

    resp = await client.post("/publish", json=event)
    assert resp.status_code == 422
