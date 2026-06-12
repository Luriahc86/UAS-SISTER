"""
conftest.py — Pytest fixtures untuk testing.

Menyediakan:
- FastAPI test client (httpx AsyncClient)
- Database pool fixture (real PostgreSQL)
- Redis fixture (real Redis)
- Cleanup fixtures

Tests membutuhkan Redis dan PostgreSQL running (via Docker Compose).
Jalankan tests dengan:
    docker compose run --rm aggregator pytest -v
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import httpx
from httpx import ASGITransport

# Set test environment variables sebelum import app
os.environ.setdefault("DATABASE_URL", "postgresql://app:app_password@postgres:5432/pubsubdb")
os.environ.setdefault("REDIS_URL", "redis://redis:6379/0")

from app.main import app
from app.db import init_db, close_db, get_pool
from app.broker import init_broker, close_broker, get_redis, STREAM_NAME
from app.migrations import run_migrations


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def setup_services():
    """Initialize database and broker for the test session."""
    pool = await init_db()
    await run_migrations(pool)
    await init_broker()
    yield pool
    await close_broker()
    await close_db()


@pytest_asyncio.fixture
async def client(setup_services) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an async HTTP client for testing FastAPI endpoints."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db_pool(setup_services):
    """Provide database pool for direct DB operations in tests."""
    pool = await get_pool()
    yield pool


@pytest_asyncio.fixture
async def redis_client(setup_services):
    """Provide Redis client for direct broker operations in tests."""
    redis = await get_redis()
    yield redis


@pytest_asyncio.fixture(autouse=True)
async def cleanup_db(db_pool):
    """
    Clean up database tables before each test.

    Ini memastikan setiap test berjalan dengan state database bersih.
    """
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM processed_events")
        await conn.execute("UPDATE stats SET value = 0")
    yield


@pytest_asyncio.fixture(autouse=True)
async def cleanup_redis(redis_client):
    """Clean up Redis stream before each test."""
    try:
        await redis_client.delete(STREAM_NAME)
    except Exception:
        pass
    # Recreate consumer group
    try:
        from app.broker import CONSUMER_GROUP
        await redis_client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass
    yield


def make_event(
    topic: str = "test.event",
    event_id: str = "test-001",
    timestamp: str = "2026-06-12T10:00:00Z",
    source: str = "test-publisher",
    payload: dict | None = None,
) -> dict:
    """Helper untuk membuat event dict untuk testing."""
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": timestamp,
        "source": source,
        "payload": payload or {"test": True},
    }
