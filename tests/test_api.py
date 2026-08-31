import json

import pytest
from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.kafka_consumer import KafkaCheckoutConsumer


class FakeService:
    def __init__(self):
        self._events = [
            {
                "event_id": "evt-123",
                "event_type": "checkout",
                "customer_id": "cust-1001",
                "total_amount": 82.44,
                "currency": "USD",
            },
            {
                "event_id": "evt-456",
                "event_type": "checkout",
                "customer_id": "cust-1002",
                "total_amount": 64.10,
                "currency": "USD",
            },
        ]

    def get_recent_events(self, limit=10):
        return list(self._events[:limit])

    def get_event(self, event_id):
        for event in self._events:
            if event["event_id"] == event_id:
                return event
        return None

    def get_statistics(self):
        return {
            "total_events": 2,
            "valid_events": 2,
            "malformed_events": 0,
            "consumer_errors": 0,
            "events_in_memory": 2,
        }

    def get_health(self):
        return {"status": "healthy", "kafka_connected": True}


@pytest.fixture
def api_client():
    app.state.service = FakeService()
    return TestClient(app)


def test_root(api_client):
    response = api_client.get("/")
    assert response.status_code == 200
    assert response.json()["name"] == "IceStream API"
    assert response.json()["version"] == "0.1.0"


def test_health(api_client):
    response = api_client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"healthy", "degraded"}
    assert "kafka_connected" in payload


def test_list_events(api_client):
    response = api_client.get("/api/events?limit=10")
    assert response.status_code == 200
    assert len(response.json()) == 2


def test_get_event_by_id(api_client):
    response = api_client.get("/api/events/evt-123")
    assert response.status_code == 200
    assert response.json()["event_id"] == "evt-123"


def test_unknown_event_returns_404(api_client):
    response = api_client.get("/api/events/evt-missing")
    assert response.status_code == 404


def test_statistics(api_client):
    response = api_client.get("/api/statistics")
    assert response.status_code == 200
    body = response.json()
    assert body["total_events"] == 2
    assert body["valid_events"] == 2
    assert body["malformed_events"] == 0
    assert body["consumer_errors"] == 0


def test_limit_validation(api_client):
    response = api_client.get("/api/events?limit=99999")
    assert response.status_code == 422


def test_malformed_event_handling_at_consumer_level():
    consumer = KafkaCheckoutConsumer("localhost:9092", "checkout-events", max_events_in_memory=10)
    consumer._process_message(b'{"event_id":"evt-1", "event_type":"checkout"}')
    consumer._process_message(b'{bad-json')

    stats = consumer.get_statistics()
    assert stats["total_events"] == 2
    assert stats["valid_events"] == 1
    assert stats["malformed_events"] == 1

    events = consumer.get_recent_events(limit=10)
    assert any(event["event_id"] == "evt-1" for event in events)
