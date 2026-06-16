from __future__ import annotations
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional, Union
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from app.broker import check_broker_health, close_broker, init_broker, publish_to_stream
from app.db import check_db_health, close_db, get_pool, init_db
from app.migrations import run_migrations
from app.models import EventSchema, EventsResponse, HealthResponse, PublishResponse, StatsResponse
from app.stats import get_all_stats, increment_stat

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("aggregator")

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await init_db()
    await run_migrations(pool)
    await init_broker()
    yield
    await close_broker()
    await close_db()

app = FastAPI(title="Pub-Sub Log Aggregator Terdistribusi", version="1.0.0", lifespan=lifespan)

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", include_in_schema=False)
async def serve_dashboard():
    index_path = _STATIC_DIR / "index.html"
    if index_path.is_file():
        return FileResponse(str(index_path), media_type="text/html")
    return JSONResponse(status_code=404, content={"detail": "Dashboard files not found."})

@app.post("/publish", response_model=PublishResponse)
async def publish_event(event_input: Union[EventSchema, List[EventSchema]]):
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
            event_dict = event.model_dump()
            await publish_to_stream(event_dict)
            queued_count += 1
        except Exception as e:
            logger.error(f"Failed to queue event {event.event_id}: {e}")

    await increment_stat(pool, "received_total", len(events))
    await increment_stat(pool, "queued_total", queued_count)

    return PublishResponse(accepted=len(events), queued=queued_count, message="events accepted")

@app.get("/events", response_model=EventsResponse)
async def get_events(topic: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch("SELECT topic, event_id, event_timestamp, source, payload, processed_at FROM events WHERE topic = $1 ORDER BY processed_at DESC LIMIT $2 OFFSET $3", topic, limit, offset)
            count = await conn.fetchval("SELECT COUNT(*) FROM events WHERE topic = $1", topic)
        else:
            rows = await conn.fetch("SELECT topic, event_id, event_timestamp, source, payload, processed_at FROM events ORDER BY processed_at DESC LIMIT $1 OFFSET $2", limit, offset)
            count = await conn.fetchval("SELECT COUNT(*) FROM events")

    events_list = []
    for row in rows:
        events_list.append({
            "topic": row["topic"],
            "event_id": row["event_id"],
            "timestamp": row["event_timestamp"].isoformat(),
            "source": row["source"],
            "payload": json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
            "processed_at": row["processed_at"].isoformat(),
        })

    return EventsResponse(topic=topic or "all", count=count, events=events_list)

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    pool = await get_pool()
    stats = await get_all_stats(pool)
    return StatsResponse(**stats)

@app.get("/health", response_model=HealthResponse)
async def health_check():
    db_ok = await check_db_health()
    broker_ok = await check_broker_health()
    status = "ok" if (db_ok and broker_ok) else "degraded"
    response = HealthResponse(status=status, database="ok" if db_ok else "error", broker="ok" if broker_ok else "error")
    if status != "ok":
        return JSONResponse(status_code=503, content=response.model_dump())
    return response
