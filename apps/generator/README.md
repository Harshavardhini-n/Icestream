# IceStream Transaction Generator

## Why this generator exists

The generator is the first data-producing component in the IceStream platform. It simulates realistic e-commerce checkout activity and emits that telemetry to Kafka so later pipeline stages can consume, validate, and monitor the stream.

This commit intentionally focuses on the transaction generator only. It does not implement Flink, Iceberg, or the downstream observability and remediation layers.

## Role in IceStream

The generator represents the upstream edge of the architecture:

Python transaction generator → Kafka → future Flink → future Iceberg

Its responsibility is to generate realistic checkout events at a configurable rate, publish them to the `checkout-events` Kafka topic, and occasionally inject controlled anomalies so future downstream observability components can detect schema drift, null data, and malformed payloads.

## Event schema

Normal events include the following fields:

- `event_id`
- `event_timestamp`
- `customer_id`
- `session_id`
- `product_id`
- `quantity`
- `unit_price`
- `subtotal`
- `discount_amount`
- `shipping_amount`
- `tax_amount`
- `total_amount`
- `currency`
- `payment_method`
- `event_type`

Example payload:

```json
{
  "event_id": "evt-9c2df1a8c1d2",
  "event_timestamp": "2026-08-30T12:00:00.000+00:00",
  "customer_id": "cust-4123",
  "session_id": "sess-ffb6d9c4f8",
  "product_id": "prod-1342",
  "quantity": 2,
  "unit_price": 49.99,
  "subtotal": 99.98,
  "discount_amount": 5.0,
  "shipping_amount": 4.99,
  "tax_amount": 8.55,
  "total_amount": 108.52,
  "currency": "USD",
  "payment_method": "card",
  "event_type": "checkout"
}
```

The generator ensures the totals are mathematically consistent before sending the event.

## Configuration variables

The generator uses environment variables. Safe defaults are provided for local development.

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CHECKOUT_TOPIC=checkout-events
EVENTS_PER_SECOND=100
MAX_EVENTS=0
NULL_TAX_RATE=0.0
SCHEMA_DRIFT_RATE=0.0
MALFORMED_EVENT_RATE=0.0
RANDOM_SEED=
```

`MAX_EVENTS=0` means continuous generation until interrupted.

## Anomaly injection

The generator intentionally creates bad data at a controllable rate so future Flink observability logic has realistic anomalies to detect.

### Null tax injection

When `NULL_TAX_RATE` is greater than zero, the event's `tax_amount` field may be set to JSON `null`.

- Variable: `NULL_TAX_RATE`
- Example: `NULL_TAX_RATE=0.5`
- Effect: roughly half of generated events have `"tax_amount": null`

### Schema drift injection

When `SCHEMA_DRIFT_RATE` is greater than zero, the event may switch from:

```json
"tax_amount": 8.55
```

to:

```json
"taxAmount": 8.55
```

This is a controlled schema drift example intended to be detected by future processing layers.

### Malformed event injection

When `MALFORMED_EVENT_RATE` is greater than zero, the generator emits intentionally invalid JSON or structurally broken payloads. These are not random event deletions; they are specific, controlled invalid records used for future bad-data detection tests.

Exact behavior:

- invalid JSON syntax is created with missing values or broken object structure
- the event is emitted as a raw string payload intended to fail JSON parsing
- this supports testing for malformed-message detection in later pipeline stages

## Install dependencies

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r .\apps\generator\requirements.txt
```

## Run the generator

Start Kafka first, then run the generator:

```powershell
docker compose -f .\infrastructure\kafka\docker-compose.kafka.yml up -d
powershell -ExecutionPolicy Bypass -File .\infrastructure\kafka\init-topics.ps1

.\.venv\Scripts\Activate.ps1
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
$env:KAFKA_CHECKOUT_TOPIC='checkout-events'
$env:EVENTS_PER_SECOND='100'
$env:MAX_EVENTS='0'
$env:NULL_TAX_RATE='0.0'
$env:SCHEMA_DRIFT_RATE='0.0'
$env:MALFORMED_EVENT_RATE='0.0'
python -m apps.generator.main
```

## Change the event rate

Adjust the generator rate with the environment variable:

```powershell
$env:EVENTS_PER_SECOND='10'
$env:EVENTS_PER_SECOND='100'
$env:EVENTS_PER_SECOND='1000'
```

This rate is enforced through a batching-oriented loop rather than sleeping every event individually.

## Intentionally create bad data

Set the anomaly variables before running the generator:

```powershell
$env:NULL_TAX_RATE='0.5'
$env:SCHEMA_DRIFT_RATE='0.2'
$env:MALFORMED_EVENT_RATE='0.1'
```

You can mix and match these values without changing source code. The generator reads them from the environment each startup.

## Verify events in Kafka

Consume the event stream from a new PowerShell session:

```powershell
docker compose -f .\infrastructure\kafka\docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic checkout-events --from-beginning --max-messages 5
```

Look for JSON lines that match the checkout event schema. If malformed payloads are enabled, the consumer may show invalid JSON, which is expected for that test mode.

## Example smoke test

```powershell
$env:EVENTS_PER_SECOND='5'
$env:MAX_EVENTS='10'
$env:NULL_TAX_RATE='0'
$env:SCHEMA_DRIFT_RATE='0'
$env:MALFORMED_EVENT_RATE='0'
python -m apps.generator.main
```

This should publish exactly 10 events and then exit cleanly.
