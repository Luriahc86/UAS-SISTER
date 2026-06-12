"""
main.py — FastAPI Aggregator API Service.

Endpoint utama:
1. POST /publish  : menerima single event atau batch event
2. GET /events    : query event unik yang sudah diproses
3. GET /stats     : metrik sistem
4. GET /health    : health check database & broker

Service ini bertindak sebagai gateway:
- Menerima event dari publisher
- Validasi schema event
- Push event ke Redis Stream (broker internal)
- Query data dari PostgreSQL

Service ini TIDAK memproses event secara langsung.
Pemrosesan dilakukan oleh worker (consumer) yang membaca dari Redis Stream.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Union

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.broker import (
    check_broker_health,
    close_broker,
    init_broker,
    publish_to_stream,
)
from app.db import check_db_health, close_db, get_pool, init_db
from app.migrations import run_migrations
from app.models import (
    EventSchema,
    EventsResponse,
    HealthResponse,
    PublishResponse,
    StatsResponse,
)
from app.stats import get_all_stats, increment_stat

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("aggregator")


# ---------------------------------------------------------------------------
# Lifespan: startup & shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Mengelola lifecycle aplikasi:
    - Startup: inisialisasi DB pool, Redis, dan jalankan migrasi
    - Shutdown: tutup koneksi DB dan Redis
    """
    logger.info("🚀 Aggregator starting up...")

    # Inisialisasi database
    pool = await init_db()
    await run_migrations(pool)
    logger.info("✅ Database initialized")

    # Inisialisasi broker
    await init_broker()
    logger.info("✅ Broker initialized")

    logger.info("🎉 Aggregator ready to accept requests")

    yield

    # Shutdown
    logger.info("🛑 Aggregator shutting down...")
    await close_broker()
    await close_db()
    logger.info("👋 Aggregator shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Pub-Sub Log Aggregator Terdistribusi",
    description=(
        "Sistem Pub-Sub log aggregator multi-service dengan "
        "idempotent consumer, deduplication, dan transaction/concurrency control."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# POST /publish — Menerima event single atau batch
# ---------------------------------------------------------------------------
@app.post("/publish", response_model=PublishResponse)
async def publish_event(event_input: Union[EventSchema, List[EventSchema]]):
    """
    Menerima satu atau beberapa event log.

    - Single event: kirim object JSON
    - Batch event: kirim array JSON

    Event akan divalidasi schema-nya, lalu dimasukkan ke Redis Stream
    untuk diproses oleh worker.

    Catatan: deduplication belum dilakukan di endpoint ini.
    Semua event yang valid akan diterima ke queue.
    Deduplication dilakukan saat worker memproses event.
    """
    # Normalize input: pastikan selalu list
    if isinstance(event_input, EventSchema):
        events = [event_input]
    else:
        events = event_input

    if len(events) == 0:
        raise HTTPException(status_code=400, detail="No events provided")

    pool = await get_pool()
    queued_count = 0

    for event in events:
        try:
            # Serialize event ke dict untuk Redis Stream
            event_dict = event.model_dump()

            # Push ke Redis Stream
            await publish_to_stream(event_dict)
            queued_count += 1

        except Exception as e:
            logger.error(f"❌ Failed to queue event {event.event_id}: {e}")

    # Update statistik: received_total dan queued_total
    await increment_stat(pool, "received_total", len(events))
    await increment_stat(pool, "queued_total", queued_count)

    logger.info(
        f"📥 Received {len(events)} event(s), queued {queued_count}"
    )

    return PublishResponse(
        accepted=len(events),
        queued=queued_count,
        message="events accepted",
    )


# ---------------------------------------------------------------------------
# GET /events — Query event unik yang sudah diproses
# ---------------------------------------------------------------------------
@app.get("/events", response_model=EventsResponse)
async def get_events(
    topic: Optional[str] = Query(None, description="Filter berdasarkan topic"),
    limit: int = Query(100, ge=1, le=1000, description="Batas jumlah event"),
    offset: int = Query(0, ge=0, description="Offset untuk pagination"),
):
    """
    Mengembalikan daftar event unik yang sudah diproses.

    Bisa difilter berdasarkan topic.
    Mendukung pagination dengan limit dan offset.
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, event_timestamp, source, payload, processed_at
                FROM events
                WHERE topic = $1
                ORDER BY processed_at DESC
                LIMIT $2 OFFSET $3
                """,
                topic,
                limit,
                offset,
            )
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM events WHERE topic = $1",
                topic,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, event_timestamp, source, payload, processed_at
                FROM events
                ORDER BY processed_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit,
                offset,
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM events")

    events_list = []
    for row in rows:
        events_list.append(
            {
                "topic": row["topic"],
                "event_id": row["event_id"],
                "timestamp": row["event_timestamp"].isoformat(),
                "source": row["source"],
                "payload": json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
                "processed_at": row["processed_at"].isoformat(),
            }
        )

    return EventsResponse(
        topic=topic or "all",
        count=count,
        events=events_list,
    )


# ---------------------------------------------------------------------------
# GET /stats — Metrik sistem
# ---------------------------------------------------------------------------
@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Menampilkan metrik sistem:
    - received_total: total event yang diterima
    - queued_total: total event yang masuk queue
    - unique_processed: event unik yang diproses
    - duplicates_dropped: event duplikat yang ditolak
    - failed_total: event yang gagal diproses
    - topic_count: jumlah topic unik
    - uptime_seconds: uptime service
    - duplicate_rate: rasio duplikasi
    - throughput_events_per_second: throughput pemrosesan
    """
    pool = await get_pool()
    stats = await get_all_stats(pool)
    return StatsResponse(**stats)


# ---------------------------------------------------------------------------
# GET /health — Health check
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.

    Mengecek koneksi ke:
    - PostgreSQL database
    - Redis broker
    """
    db_ok = await check_db_health()
    broker_ok = await check_broker_health()

    status = "ok" if (db_ok and broker_ok) else "degraded"

    response = HealthResponse(
        status=status,
        database="ok" if db_ok else "error",
        broker="ok" if broker_ok else "error",
    )

    if status != "ok":
        return JSONResponse(status_code=503, content=response.model_dump())

    return response
