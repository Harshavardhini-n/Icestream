# IceStream API

## What the API does

This API is the first backend service for IceStream. It connects to Kafka, consumes messages from the real `checkout-events` topic, keeps a lightweight in-memory view of the latest events, and exposes REST endpoints for future frontend integration.

## Architecture

Kafka
↓
Kafka Consumer
↓
FastAPI Backend
↓
REST API

The API intentionally does not implement React, Flink, Iceberg, MinIO, DLQ handling, or remediation in this commit.

## Required environment variables

The API reads these environment variables:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_CHECKOUT_TOPIC` (default: `checkout-events`)
- `API_HOST` (default: `0.0.0.0`)
- `API_PORT` (default: `8000`)
- `MAX_EVENTS_IN_MEMORY` (default: `100`)

These values are defined in the repository root `.env.example` and are compatible with the existing generator setup.

## How to install dependencies

From the repository root:

```powershell
python -m pip install -r apps/api/requirements.txt
```

## How to start Kafka

Use the existing Kafka stack in this repo:

```powershell
docker compose -f .\infrastructure\kafka\docker-compose.kafka.yml up -d
powershell -ExecutionPolicy Bypass -File .\infrastructure\kafka\init-topics.ps1
```

## How to start the API

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
$env:KAFKA_CHECKOUT_TOPIC='checkout-events'
python -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload
```

## Swagger URL

Open:

- http://localhost:8000/docs

## REST endpoints

- `GET /` — API metadata
- `GET /health` — API and Kafka health
- `GET /api/events?limit=20` — recent events
- `GET /api/events/{event_id}` — specific event lookup
- `GET /api/statistics` — stream statistics

## How to verify that Kafka events are reaching the API

1. Start Kafka and confirm the topic exists.
2. Start the existing Commit 3 generator with a small run:

```powershell
$env:EVENTS_PER_SECOND='2'
$env:MAX_EVENTS='10'
python -m apps.generator.main
```

3. Query the API:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/api/statistics
Invoke-RestMethod http://localhost:8000/api/events
```

The API should reflect actual Kafka messages flowing through the `checkout-events` topic.

## What is intentionally NOT implemented yet

- React is not part of Commit 4.
- Flink is not part of Commit 4.
- Iceberg is not part of Commit 4.
- DLQ/remediation are not part of Commit 4.
- MinIO is not part of Commit 4.
- This commit does not add frontend or streaming processing beyond the API consumer.
