from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from apps.api.config import get_settings
from apps.api.kafka_consumer import KafkaCheckoutConsumer
from apps.api.models import EventResponse, HealthResponse, StatisticsResponse
from apps.api.service import ApiService
from apps.api.websocket_manager import WebSocketConnectionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("apps.api.main")

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting IceStream API")
    consumer = KafkaCheckoutConsumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_checkout_topic,
        max_events_in_memory=settings.max_events_in_memory,
    )
    websocket_manager = WebSocketConnectionManager()
    app.state.consumer = consumer
    app.state.service = ApiService(consumer=consumer)
    app.state.websocket_manager = websocket_manager
    consumer.add_event_listener(websocket_manager.notify_clients)
    consumer.start()
    logger.info("Kafka consumer startup initiated")
    try:
        yield
    finally:
        logger.info("Shutting down IceStream API")
        consumer.remove_event_listener(websocket_manager.notify_clients)
        consumer.stop()
        await websocket_manager.close()
        logger.info("Kafka consumer shutdown complete")


app = FastAPI(
    title="IceStream API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {"name": "IceStream API", "version": "0.1.0", "status": "running"}


@app.get("/health", response_model=HealthResponse, tags=["health"])
def health() -> HealthResponse:
    service = app.state.service
    payload = service.get_health()
    return HealthResponse(**payload)


@app.get("/api/events", response_model=list[EventResponse], tags=["events"])
def list_events(limit: int = Query(default=10, ge=1, le=settings.max_events_in_memory)) -> list[dict[str, str | int | float | None]]:
    service = app.state.service
    return [event for event in service.get_recent_events(limit)]


@app.get("/api/events/{event_id}", response_model=EventResponse, tags=["events"])
def get_event(event_id: str) -> EventResponse:
    service = app.state.service
    event = service.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return EventResponse(**event)


@app.get("/api/statistics", response_model=StatisticsResponse, tags=["statistics"])
def statistics() -> StatisticsResponse:
    service = app.state.service
    content = service.get_statistics()
    return StatisticsResponse(**content)


@app.websocket("/ws/events")
async def websocket_events(socket: WebSocket) -> None:
    manager: WebSocketConnectionManager = app.state.websocket_manager
    service = app.state.service
    client = await manager.connect(socket)

    async def send_snapshot() -> None:
        await socket.send_json(
            {
                "type": "snapshot",
                "events": service.get_recent_events(limit=20),
                "statistics": service.get_statistics(),
                "health": service.get_health(),
            }
        )

    async def send_updates() -> None:
        while True:
            await client.changed.wait()
            client.changed.clear()
            await send_snapshot()

    async def monitor_disconnect(task_group: anyio.abc.TaskGroup) -> None:
        while True:
            message = await socket.receive()
            if message["type"] == "websocket.disconnect":
                task_group.cancel_scope.cancel()
                return

    try:
        await send_snapshot()
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(send_updates)
            task_group.start_soon(monitor_disconnect, task_group)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.warning("WebSocket client disconnected after an update failure", exc_info=True)
    finally:
        manager.disconnect(client)
