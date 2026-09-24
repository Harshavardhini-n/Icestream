from __future__ import annotations
import asyncio, time, uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka_service import KafkaService
from validator import Validator
from websocket import ConnectionManager

validator, manager = Validator(), ConnectionManager()
state: dict[str, Any] = {"running": True, "circuit_open": False, "events": deque(maxlen=40), "dlq": deque(maxlen=30), "clean": [], "recent": deque(maxlen=20), "alert": None}
kafka: KafkaService

def status() -> dict:
    clean = state["clean"]
    total = len(clean) + len(state["dlq"])
    revenue = sum(float(x.get("amount", 0)) for x in clean)
    return {"running": state["running"], "circuit_open": state["circuit_open"], "kafka_connected": kafka.kafka_connected, "total_orders": len(clean), "total_revenue": round(revenue, 2), "avg_order_value": round(revenue / len(clean), 2) if clean else 0, "error_rate": round((len(state["dlq"])/total*100), 1) if total else 0, "dlq_count": len(state["dlq"]), "alert": state["alert"], "clean": clean[-100:], "dlq": list(state["dlq"]), "events": list(state["events"])}

async def process(event: dict) -> None:
    event = {**event, "_id": str(uuid.uuid4())[:8], "_received": time.time()}
    if not state["running"] or state["circuit_open"]:
        valid, error = False, "CIRCUIT_BREAKER"
    else:
        valid, error = validator.validate(event)
    state["recent"].append(valid)
    item = {"type": "VALID" if valid else "INVALID", "data": event, "error": error, "timestamp": time.time()}
    state["events"].append(item)
    if valid:
        state["clean"].append(event)  # simulated Iceberg clean-data table
    else:
        state["dlq"].appendleft(item) # simulated orders-dlq topic
    if len(state["recent"]) >= 20 and sum(not x for x in state["recent"]) / 20 > .30:
        state["circuit_open"] = True
        state["running"] = False
        state["alert"] = "Circuit breaker open: >30% of the last 20 events were invalid."
    await manager.broadcast({"kind": "update", "status": status(), "event": item})

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka
    kafka = KafkaService(process)
    await kafka.start()
    yield
    await kafka.stop()

app = FastAPI(title="IceStream API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])

class Event(BaseModel):
    data: dict[str, Any]
class ExternalRequest(BaseModel):
    url: str | None = None
    data: dict[str, Any] | list[dict[str, Any]] | None = None

@app.get("/status")
async def get_status(): return status()


@app.get("/")
async def root():
    return RedirectResponse(url="/docs")

@app.post("/produce")
async def produce(payload: Event):
    await kafka.produce(payload.data)
    return {"accepted": True, "topic": "orders"}

async def poll_url(url: str):
    async with httpx.AsyncClient(timeout=10) as client:
        while state["running"]:
            try:
                response = await client.get(url); response.raise_for_status(); body = response.json()
                for record in body if isinstance(body, list) else [body]: await kafka.produce(record)
            except Exception as exc:
                state["alert"] = f"External ingest error: {exc}"
                await manager.broadcast({"kind":"update", "status":status()})
            await asyncio.sleep(5)

@app.post("/external-ingest")
async def external_ingest(payload: ExternalRequest):
    if payload.data is not None:
        records = payload.data if isinstance(payload.data, list) else [payload.data]
        for record in records: await kafka.produce(record)
        return {"accepted": len(records), "source": "manual"}
    if payload.url:
        asyncio.create_task(poll_url(payload.url))
        return {"accepted": True, "source": "url", "interval_seconds": 5}
    raise HTTPException(400, "Provide url or data")

@app.post("/control/{action}")
async def control(action: str):
    if action == "start": state["running"] = True; state["circuit_open"] = False; state["alert"] = None
    elif action == "stop": state["running"] = False
    elif action == "reset":
        validator.reset(); state.update({"running": True, "circuit_open": False, "events": deque(maxlen=40), "dlq": deque(maxlen=30), "clean": [], "recent": deque(maxlen=20), "alert": None})
    elif action == "inject-bad": await kafka.produce({"order_id": "broken-" + str(uuid.uuid4())[:5], "user_id": "", "amount": -20, "tax": None})
    else: raise HTTPException(404, "Unknown action")
    await manager.broadcast({"kind":"update", "status":status()})
    return status()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws); await ws.send_json({"kind":"update", "status":status()})
    try:
        while True: await ws.receive_text()
    except Exception: manager.disconnect(ws)
