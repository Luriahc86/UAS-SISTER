from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Union
from pydantic import BaseModel, Field, field_validator

class EventSchema(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: str = Field(...)
    source: str = Field(..., min_length=1, max_length=255)
    payload: Dict[str, Any] = Field(...)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            raise ValueError("Invalid ISO8601 timestamp")
        return v

class PublishResponse(BaseModel):
    accepted: int
    queued: int
    message: str = "events accepted"

class EventOut(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]
    processed_at: str

class EventsResponse(BaseModel):
    topic: str
    count: int
    events: List[EventOut]

class StatsResponse(BaseModel):
    received_total: int = 0
    queued_total: int = 0
    unique_processed: int = 0
    duplicates_dropped: int = 0
    failed_total: int = 0
    topic_count: int = 0
    uptime_seconds: float = 0.0
    duplicate_rate: float = 0.0
    throughput_events_per_second: float = 0.0

class HealthResponse(BaseModel):
    status: str = "ok"
    database: str = "ok"
    broker: str = "ok"

PublishInput = Union[EventSchema, List[EventSchema]]
