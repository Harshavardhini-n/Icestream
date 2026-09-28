import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from apps.api.main import app, websocket_events
from apps.api.kafka_consumer import KafkaCheckoutConsumer
from apps.api.service import ApiService
from apps.api.websocket_manager import WebSocketConnectionManager


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
    app.state.websocket_manager = WebSocketConnectionManager()
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


def test_websocket_sends_initial_snapshot(api_client):
    with api_client.websocket_connect("/ws/events") as websocket:
        message = websocket.receive_json()

    assert message["type"] == "snapshot"
    assert [event["event_id"] for event in message["events"]] == ["evt-123", "evt-456"]
    assert message["statistics"]["total_events"] == 2
    assert message["health"]["kafka_connected"] is True


def test_websocket_receives_snapshot_after_consumer_event(api_client):
    consumer = KafkaCheckoutConsumer("localhost:9092", "checkout-events")
    manager = app.state.websocket_manager
    app.state.consumer = consumer
    app.state.service = ApiService(consumer=consumer)
    consumer.add_event_listener(manager.notify_clients)

    with api_client.websocket_connect("/ws/events") as websocket:
        initial_message = websocket.receive_json()
        assert initial_message["events"] == []

        consumer._process_message(
            b'{"event_id":"evt-live","event_type":"checkout"}'
        )
        update = websocket.receive_json()

    assert update["type"] == "snapshot"
    assert update["events"] == [
        {"event_id": "evt-live", "event_type": "checkout"}
    ]
    assert update["statistics"]["valid_events"] == 1


def test_websocket_disconnect_does_not_interrupt_other_clients(api_client):
    consumer = KafkaCheckoutConsumer("localhost:9092", "checkout-events")
    manager = app.state.websocket_manager
    app.state.consumer = consumer
    app.state.service = ApiService(consumer=consumer)
    consumer.add_event_listener(manager.notify_clients)

    with api_client.websocket_connect("/ws/events") as first:
        first.receive_json()
        with api_client.websocket_connect("/ws/events") as second:
            second.receive_json()
            first.close()

            consumer._process_message(
                b'{"event_id":"evt-live","event_type":"checkout"}'
            )
            update = second.receive_json()

    assert update["events"][0]["event_id"] == "evt-live"


def test_failed_websocket_send_does_not_interrupt_other_clients(api_client):
    class FakeSocket:
        def __init__(self, *, fail_send=False):
            self.fail_send = fail_send
            self.sent = asyncio.Queue()
            self.disconnected = asyncio.Event()

        async def accept(self):
            pass

        async def send_json(self, payload):
            if self.fail_send:
                raise RuntimeError("simulated send failure")
            await self.sent.put(payload)

        async def receive(self):
            await self.disconnected.wait()
            return {"type": "websocket.disconnect"}

        async def close(self, code=1000):
            self.disconnected.set()

    async def exercise_clients():
        manager = app.state.websocket_manager
        failing_socket = FakeSocket(fail_send=True)
        healthy_socket = FakeSocket()
        failing_task = asyncio.create_task(websocket_events(failing_socket))
        healthy_task = asyncio.create_task(websocket_events(healthy_socket))

        await healthy_socket.sent.get()
        await failing_task
        manager.notify_clients()
        update = await asyncio.wait_for(healthy_socket.sent.get(), timeout=1)
        await manager.close()
        await healthy_task
        return update

    update = asyncio.run(exercise_clients())

    assert update["type"] == "snapshot"


def test_consumer_listener_failure_does_not_interrupt_other_listeners():
    consumer = KafkaCheckoutConsumer("localhost:9092", "checkout-events")
    notified = []

    def failing_listener():
        raise RuntimeError("listener failed")

    consumer.add_event_listener(failing_listener)
    consumer.add_event_listener(lambda: notified.append(True))

    consumer._process_message(b'{"event_id":"evt-1"}')

    assert notified == [True]


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
