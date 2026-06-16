from __future__ import annotations
import asyncpg

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    topic           TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    payload         JSONB NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);
"""

CREATE_PROCESSED_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS processed_events (
    topic        TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, event_id)
);
"""

CREATE_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS stats (
    key   TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
"""

CREATE_EVENTS_TOPIC_INDEX = "CREATE INDEX IF NOT EXISTS idx_events_topic ON events (topic);"

SEED_STATS = """
INSERT INTO stats(key, value) VALUES
    ('received_total', 0),
    ('queued_total', 0),
    ('unique_processed', 0),
    ('duplicates_dropped', 0),
    ('failed_total', 0)
ON CONFLICT (key) DO NOTHING;
"""

async def run_migrations(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_PROCESSED_EVENTS_TABLE)
        await conn.execute(CREATE_EVENTS_TABLE)
        await conn.execute(CREATE_STATS_TABLE)
        await conn.execute(CREATE_EVENTS_TOPIC_INDEX)
        await conn.execute(SEED_STATS)
