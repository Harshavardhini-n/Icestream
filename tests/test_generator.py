import json
import random

import pytest

from apps.generator.config import Settings
from apps.generator.generator import (
    generate_checkout_event,
    generate_malformed_event,
    render_event_payload,
)


def test_normal_event_generation():
    event = generate_checkout_event(random.Random(42))

    assert event["event_type"] == "checkout"
    assert "event_id" in event
    assert "event_timestamp" in event
    assert "customer_id" in event
    assert "session_id" in event
    assert "product_id" in event
    assert isinstance(event["quantity"], int)
    assert event["quantity"] > 0
    assert event["unit_price"] > 0
    assert event["currency"] == "USD"


def test_financial_calculation_consistency():
    event = generate_checkout_event(random.Random(99))

    subtotal = event["quantity"] * event["unit_price"]
    assert abs(event["subtotal"] - subtotal) < 0.01

    total = (
        event["subtotal"]
        - event["discount_amount"]
        + event["shipping_amount"]
        + event["tax_amount"]
    )
    assert abs(event["total_amount"] - total) < 0.01


def test_null_tax_injection():
    event = generate_checkout_event(random.Random(7), null_tax=True)

    assert event["tax_amount"] is None


def test_schema_drift_injection():
    event = generate_checkout_event(random.Random(11), schema_drift=True)

    assert "tax_amount" not in event
    assert "taxAmount" in event
    assert event["event_type"] == "checkout"


def test_malformed_event_generation():
    payload = generate_malformed_event(random.Random(5))

    assert isinstance(payload, str)
    with pytest.raises(json.JSONDecodeError):
        json.loads(payload)


def test_configuration_parsing():
    settings = Settings.from_mapping(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_CHECKOUT_TOPIC": "custom-topic",
            "EVENTS_PER_SECOND": "250",
            "MAX_EVENTS": "50",
            "NULL_TAX_RATE": "0.25",
            "SCHEMA_DRIFT_RATE": "0.1",
            "MALFORMED_EVENT_RATE": "0.05",
            "RANDOM_SEED": "42",
        }
    )

    assert settings.kafka_bootstrap_servers == "broker:9092"
    assert settings.kafka_checkout_topic == "custom-topic"
    assert settings.events_per_second == 250
    assert settings.max_events == 50
    assert settings.null_tax_rate == 0.25
    assert settings.schema_drift_rate == 0.1
    assert settings.malformed_event_rate == 0.05
    assert settings.random_seed == 42


def test_render_event_payload_serializes_normal_event():
    event = generate_checkout_event(random.Random(13))
    payload = render_event_payload(event)

    assert isinstance(payload, bytes)
    assert json.loads(payload.decode("utf-8"))["event_id"] == event["event_id"]
