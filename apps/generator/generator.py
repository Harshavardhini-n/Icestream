from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _choose_product(rng: random.Random) -> str:
    products = [
        "prod-1001",
        "prod-1045",
        "prod-1189",
        "prod-1217",
        "prod-1342",
        "prod-1410",
        "prod-1526",
        "prod-1715",
        "prod-1888",
        "prod-2007",
    ]
    return rng.choice(products)


def _choose_payment_method(rng: random.Random) -> str:
    return rng.choice(["card", "wallet", "bank_transfer", "gift_card"])


def generate_checkout_event(
    rng: random.Random | None = None,
    *,
    null_tax: bool = False,
    schema_drift: bool = False,
) -> dict[str, Any]:
    rng = rng or random.Random()

    quantity = rng.randint(1, 4)
    unit_price = round(rng.uniform(12.50, 250.00), 2)
    subtotal = round(quantity * unit_price, 2)
    discount_fraction = rng.uniform(0.0, 0.18)
    discount_amount = round(min(subtotal * discount_fraction, subtotal * 0.35), 2)
    shipping_amount = round(rng.choice([0.00, 4.99, 6.99, 9.99]), 2)
    tax_rate = rng.choice([0.065, 0.0725, 0.0825])
    tax_amount = round((subtotal - discount_amount) * tax_rate, 2)
    total_amount = round(subtotal - discount_amount + shipping_amount + tax_amount, 2)

    event: dict[str, Any] = {
        "event_id": f"evt-{uuid.uuid4().hex[:12]}",
        "event_timestamp": _utc_now_iso(),
        "customer_id": f"cust-{rng.randint(1000, 9999)}",
        "session_id": f"sess-{uuid.uuid4().hex[:10]}",
        "product_id": _choose_product(rng),
        "quantity": quantity,
        "unit_price": unit_price,
        "subtotal": subtotal,
        "discount_amount": discount_amount,
        "shipping_amount": shipping_amount,
        "tax_amount": tax_amount,
        "total_amount": total_amount,
        "currency": "USD",
        "payment_method": _choose_payment_method(rng),
        "event_type": "checkout",
    }

    if null_tax:
        event["tax_amount"] = None

    if schema_drift:
        tax_value = event.pop("tax_amount", None)
        event["taxAmount"] = tax_value

    return event


def render_event_payload(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def generate_malformed_event(rng: random.Random | None = None) -> str:
    rng = rng or random.Random()
    templates: Iterable[str] = (
        '{"event_id":"bad-1","event_type":"checkout","subtotal":45.10,"tax_amount":,"currency":"USD"}',
        '{"event_id":"bad-2","event_type":"checkout","subtotal":45.10,"tax_amount":12.99,"currency":"USD"',
        '{"event_id":"bad-3","event_type":"checkout","subtotal":45.10,"tax_amount":12.99,"currency":}',
        '{"event_id":"bad-4","event_type":"checkout","subtotal":45.10,"tax_amount":, "currency":"USD"}',
    )
    return rng.choice(list(templates))


def apply_anomalies(
    event: dict[str, Any],
    *,
    null_tax_rate: float,
    schema_drift_rate: float,
    malformed_event_rate: float,
    rng: random.Random,
) -> dict[str, Any] | str:
    if rng.random() < malformed_event_rate:
        return generate_malformed_event(rng)

    if rng.random() < null_tax_rate:
        event["tax_amount"] = None

    if rng.random() < schema_drift_rate:
        tax_value = event.pop("tax_amount", None)
        event["taxAmount"] = tax_value

    return event
