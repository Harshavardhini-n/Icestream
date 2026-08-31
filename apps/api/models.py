from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class EventResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str | None = None
    event_type: str | None = None
    event_timestamp: str | None = None
    customer_id: str | None = None
    session_id: str | None = None
    product_id: str | None = None
    quantity: int | None = None
    unit_price: float | None = None
    subtotal: float | None = None
    discount_amount: float | None = None
    shipping_amount: float | None = None
    tax_amount: float | None = None
    total_amount: float | None = None
    currency: str | None = None
    payment_method: str | None = None


class HealthResponse(BaseModel):
    status: str
    kafka_connected: bool = False
    details: str | None = None


class StatisticsResponse(BaseModel):
    total_events: int
    valid_events: int
    malformed_events: int
    consumer_errors: int
    events_in_memory: int
