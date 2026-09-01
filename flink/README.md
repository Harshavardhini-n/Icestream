# Flink Stream Processing

Apache Flink foundation for real-time event stream processing.

## Overview

This Flink implementation provides a stream-processing layer that:

1. **Consumes** raw checkout events from Kafka (`checkout-events` topic)
2. **Deserializes** JSON payloads with error handling
3. **Validates** event structure against the expected schema
4. **Filters** malformed events without crashing the job
5. **Enriches** valid events with processing metadata
6. **Produces** processed events to Kafka (`processed-checkout-events` topic)

## Architecture

```
Python Generator
        ↓
    Kafka (checkout-events)
        ↓
    Flink Job (checkout_processor)
    - Deserialize
    - Validate
    - Enrich
    - Serialize
        ↓
    Kafka (processed-checkout-events)
```

## Components

### Configuration (`flink/config.py`)

`FlinkConfig` class manages all configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address. Use `kafka:9092` for Docker. |
| `KAFKA_INPUT_TOPIC` | `checkout-events` | Input topic with raw events |
| `KAFKA_OUTPUT_TOPIC` | `processed-checkout-events` | Output topic for processed events |
| `KAFKA_CONSUMER_GROUP` | `icestream-flink-processor` | Kafka consumer group name |
| `FLINK_JOB_NAME` | `icestream-checkout-processor` | Flink job name |
| `FLINK_PARALLELISM` | `2` | Parallelism level |

### Job Implementation (`flink/jobs/checkout_processor.py`)

The main Flink job uses the following processing functions:

#### EventDeserializer (MapFunction)
- Parses JSON strings into Python dictionaries
- Returns `None` for malformed JSON (logged, not crashed)
- Tracks malformed event count

#### EventValidator (MapFunction)
- Validates event has all required fields
- Checks basic numeric field types
- Tolerant of extra fields and null values in nullable fields
- Returns `None` for invalid events
- Tracks validation success and error counts

#### MalformedEventFilter (FilterFunction)
- Filters out `None` values (invalid events)
- Passes through valid events

#### EventEnricher (MapFunction)
- Adds `"processed": true` flag to valid events
- Preserves all original fields

#### EventSerializer (MapFunction)
- Serializes event dict back to compact JSON
- Returns minimal error record if serialization fails
- Uses compact JSON format (no whitespace)

## Docker Setup

### Local Development (Docker Compose)

```bash
# Start Kafka (if not already running)
docker compose -f infrastructure/kafka/docker-compose.kafka.yml up -d

# Start Flink
docker compose -f infrastructure/flink/docker-compose.flink.yml up -d

# View Flink UI
open http://localhost:8081
```

The Flink Docker Compose provides:
- **JobManager**: Orchestrates the job, exposes REST API and Web UI on port 8081
- **TaskManager**: Executes parallel processing tasks
- Both connected to `icestream-network` for internal communication

**Important**: Inside Docker containers, use `kafka:9092` (internal Docker hostname) instead of `localhost:9092`.

### Docker Compose Structure

```yaml
services:
  flink-jobmanager:
    # Runs Flink JobManager
    # Exposes port 8081 (Web UI)
    # Exposes port 6123 (RPC)
    
  flink-taskmanager:
    # Runs Flink TaskManager
    # Depends on JobManager
```

## Running the Job

### Via Python (Local Development)

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r flink/requirements.txt

# Run the job
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_INPUT_TOPIC="checkout-events"
$env:KAFKA_OUTPUT_TOPIC="processed-checkout-events"
$env:FLINK_PARALLELISM="2"

python -m flink.jobs.checkout_processor
```

### Via Flink Cluster (Docker)

```powershell
# Set variables
$env:KAFKA_BOOTSTRAP_SERVERS="kafka:9092"

# Use Flink CLI to submit the job
# (Requires Flink client tools installed)

flink run -m localhost:8081 -py flink/jobs/checkout_processor.py
```

**Note**: PyFlink requires Java Runtime Environment (JRE). Ensure `JAVA_HOME` is set.

## Event Flow

### Valid Event Example

```json
{
  "event_id": "evt-abc123",
  "event_timestamp": "2026-01-01T12:00:00.000Z",
  "customer_id": "cust-5678",
  "session_id": "sess-xyz",
  "product_id": "prod-1001",
  "quantity": 2,
  "unit_price": 49.99,
  "subtotal": 99.98,
  "discount_amount": 10.00,
  "shipping_amount": 5.99,
  "tax_amount": 7.50,
  "total_amount": 103.47,
  "currency": "USD",
  "payment_method": "card",
  "event_type": "checkout",
  "processed": true
}
```

### Anomalies Handled

1. **Malformed JSON**: Logged and filtered out
2. **Missing Required Fields**: Logged and filtered out
3. **Invalid Numeric Types**: Logged and filtered out
4. **Schema Drift** (e.g., camelCase field names): Currently flagged, but tolerant parsing can be extended
5. **Null Values**: Allowed in nullable fields (e.g., `tax_amount`)

## Testing

### Run Unit Tests

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest tests/test_flink.py -v
```

Tests cover:
- JSON deserialization (valid and malformed)
- Event validation (required fields, numeric types)
- Filter behavior (valid/malformed distinction)
- Event enrichment (metadata addition)
- JSON serialization (round-trip)
- Configuration loading
- Full pipeline integration (mixed batches)

### Run All Tests (Including Existing)

```powershell
python -m pytest tests -v
```

## Monitoring

### Flink Web UI

Access at `http://localhost:8081` to view:
- Job status (running, failed, etc.)
- Parallelism and task slot usage
- Task manager details
- Job statistics and metrics

### Logs

```bash
# View JobManager logs
docker logs -f icestream-flink-jobmanager

# View TaskManager logs
docker logs -f icestream-flink-taskmanager
```

### Kafka Topics

Inspect processed events:

```bash
# From Kafka container
docker exec icestream-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic processed-checkout-events \
  --from-beginning
```

## Future Extensions

Commit 6 establishes the foundation. Later commits can:

1. **Commit 7**: Add more sophisticated stream processing operators
2. **Commit 8+**: Integrate Iceberg for lakehouse storage
3. **Future**: Add MinIO storage backend
4. **Future**: Implement data quality rules engine
5. **Future**: Add circuit breaker for fault handling
6. **Future**: Implement DLQ for dead-letter queuing

## Troubleshooting

### Flink Job Won't Start

**Error**: `Connection refused`

**Solution**: Ensure Kafka is running and accessible at configured `KAFKA_BOOTSTRAP_SERVERS`.

### Kafka Topics Missing

**Error**: `Topic 'checkout-events' does not exist`

**Solution**: Run the Kafka topic initialization script:

```bash
docker exec icestream-kafka /opt/kafka/init-topics.ps1
```

### PyFlink Import Errors

**Error**: `No module named 'pyflink'`

**Solution**: Install Flink dependencies:

```powershell
pip install -r flink/requirements.txt
```

### Java Not Found

**Error**: `java: command not found` or `JAVA_HOME not set`

**Solution**: 
- Install Java Runtime Environment (JRE)
- Set `JAVA_HOME` environment variable to JRE installation directory

### Docker Network Issues

**Error**: `Flink cannot reach Kafka` (from Docker)

**Solution**: Ensure `KAFKA_BOOTSTRAP_SERVERS` is set to `kafka:9092` (internal Docker hostname) when running Flink in containers, not `localhost:9092`.

## Related Files

- Configuration: [flink/config.py](../flink/config.py)
- Main Job: [flink/jobs/checkout_processor.py](../flink/jobs/checkout_processor.py)
- Tests: [tests/test_flink.py](../tests/test_flink.py)
- Docker: [infrastructure/flink/docker-compose.flink.yml](../infrastructure/flink/docker-compose.flink.yml)
- Kafka Setup: [infrastructure/kafka/docker-compose.kafka.yml](../infrastructure/kafka/docker-compose.kafka.yml)
