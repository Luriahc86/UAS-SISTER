"""
migrations.py — Database schema initialization.

Membuat tabel-tabel yang diperlukan saat startup:
1. events          : menyimpan event unik yang berhasil diproses
2. processed_events: tabel deduplication (primary key = topic + event_id)
3. stats           : statistik global sistem

Migrasi bersifat idempotent — aman dijalankan berulang kali (IF NOT EXISTS).
"""

from __future__ import annotations

import logging

import asyncpg

logger = logging.getLogger("aggregator.migrations")

# ---------------------------------------------------------------------------
# DDL Statements
# ---------------------------------------------------------------------------

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

# Index untuk query events berdasarkan topic
CREATE_EVENTS_TOPIC_INDEX = """
CREATE INDEX IF NOT EXISTS idx_events_topic ON events (topic);
"""

# Seed data untuk tabel stats
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
    """
    Jalankan semua migrasi database.

    Fungsi ini idempotent — aman dipanggil berulang kali.
    Menggunakan IF NOT EXISTS dan ON CONFLICT DO NOTHING.
    """
    async with pool.acquire() as conn:
        # Buat tabel-tabel
        await conn.execute(CREATE_PROCESSED_EVENTS_TABLE)
        logger.info("✅ Table 'processed_events' ready")

        await conn.execute(CREATE_EVENTS_TABLE)
        logger.info("✅ Table 'events' ready")

        await conn.execute(CREATE_STATS_TABLE)
        logger.info("✅ Table 'stats' ready")

        # Buat index
        await conn.execute(CREATE_EVENTS_TOPIC_INDEX)
        logger.info("✅ Index 'idx_events_topic' ready")

        # Seed stats
        await conn.execute(SEED_STATS)
        logger.info("✅ Stats counters seeded")

    logger.info("🎉 All migrations completed successfully")
