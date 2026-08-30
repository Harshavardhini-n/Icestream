from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass(slots=True)
class CheckoutEvent:
    event_id: str
    event_timestamp: str
    customer_id: str
    session_id: str
    product_id: str
    quantity: int
    unit_price: float
    subtotal: float
    discount_amount: float
    shipping_amount: float
    tax_amount: float | None
    total_amount: float
    currency: str
    payment_method: str
    event_type: str = "checkout"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CheckoutEvent":
        return cls(**payload)
