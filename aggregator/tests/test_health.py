"""
test_health.py — Test endpoint GET /health dan logging.

Test Cases:
15. Health check mengembalikan status database dan broker (TC-15)
16. Logging duplicate event muncul saat event duplikat dikirim (TC-16)
"""

import pytest
import logging

from tests.conftest import make_event
from app.worker import process_event


# ---------------------------------------------------------------------------
# TC-15: Health check mengembalikan status database dan broker
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_health_check(client):
    """
    GET /health harus mengembalikan status database dan broker.
    Kedua komponen harus 'ok' saat tests berjalan.
    """
    resp = await client.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "ok"
    assert data["database"] == "ok"
    assert data["broker"] == "ok"


# ---------------------------------------------------------------------------
# TC-16: Logging duplicate event muncul saat event duplikat dikirim
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_logging(client, db_pool, caplog):
    """
    Saat event duplikat dikirim, worker harus mencatat log
    'duplicate dropped' dengan informasi topic dan event_id.
    """
    event = make_event(
        topic="logging.test",
        event_id="log-dup-001",
    )

    # Proses event pertama (baru)
    with caplog.at_level(logging.INFO, logger="worker"):
        await process_event(db_pool, event)

    # Proses event kedua (duplikat)
    with caplog.at_level(logging.INFO, logger="worker"):
        caplog.clear()
        await process_event(db_pool, event)

    # Cek bahwa log duplicate muncul
    log_messages = [record.message for record in caplog.records]
    has_duplicate_log = any("duplicate dropped" in msg.lower() for msg in log_messages)

    assert has_duplicate_log, (
        "Expected 'duplicate dropped' log message when processing duplicate event. "
        f"Actual logs: {log_messages}"
    )
