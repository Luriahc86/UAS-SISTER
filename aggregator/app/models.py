"""
models.py — Pydantic schemas untuk event validation.

Setiap event WAJIB memiliki:
- topic   : kategori event (e.g. "payment.created")
- event_id: UUID unik per event
- timestamp: ISO8601 datetime string
- source  : asal event
- payload : data bebas (dict)

Dedup key: (topic, event_id)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, field_validator


class EventSchema(BaseModel):
    """Schema validasi untuk satu event log."""

    topic: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Kategori event, e.g. 'payment.created', 'auth.login'",
        examples=["payment.created"],
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Identitas unik event (UUID atau string unik)",
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    timestamp: str = Field(
        ...,
        description="Waktu event dibuat (ISO8601 format)",
        examples=["2026-06-12T10:00:00Z"],
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Sumber/origin event",
        examples=["publisher-1"],
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="Data event dalam format object/dict",
        examples=[{"user_id": "U001", "amount": 150000, "currency": "IDR"}],
    )

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Validasi bahwa timestamp adalah ISO8601 yang valid."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            raise ValueError(
                f"Invalid ISO8601 timestamp: '{v}'. "
                "Expected format: '2026-06-12T10:00:00Z' or '2026-06-12T10:00:00+00:00'"
            )
        return v


class PublishResponse(BaseModel):
    """Response schema untuk POST /publish."""

    accepted: int = Field(description="Jumlah event yang diterima")
    queued: int = Field(description="Jumlah event yang masuk ke queue")
    message: str = Field(default="events accepted")


class EventOut(BaseModel):
    """Schema output untuk satu event yang sudah diproses."""

    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]
    processed_at: str


class EventsResponse(BaseModel):
    """Response schema untuk GET /events."""

    topic: str
    count: int
    events: List[EventOut]


class StatsResponse(BaseModel):
    """Response schema untuk GET /stats."""

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
    """Response schema untuk GET /health."""

    status: str = "ok"
    database: str = "ok"
    broker: str = "ok"


# Type alias: POST /publish bisa menerima single event atau list of events
PublishInput = Union[EventSchema, List[EventSchema]]
