# IceStream Kafka Infrastructure

This directory contains the local Kafka development configuration for IceStream. It is intentionally scoped to infrastructure only and supports local development of the streaming backbone without introducing Flink, Iceberg, or application services.

## Why IceStream Uses Kafka

Kafka is the event backbone for IceStream because it provides durable, decoupled, append-only event streaming between producers and downstream consumers.

In this project, Kafka allows checkout events to be produced independently of later processing stages. It also provides dedicated topics for dead-letter events and operational control, which will be used by later remediation and orchestration components.

## Kafka's Role in the Architecture

The Kafka broker sits between the event producer and the downstream processing stack.

The local development topology is intentionally simple:

* `checkout-events`: primary stream for checkout telemetry events
* `checkout-dlq`: dead-letter topic reserved for future bad-data handling and replay
* `icestream-control`: control topic for operational coordination and future orchestration

This matches the current architecture without introducing processing components that are not part of this commit.

## Why KRaft Is Used

IceStream uses Kafka in KRaft mode instead of the ZooKeeper-based deployment model.

KRaft keeps the local development environment simpler by removing the additional ZooKeeper dependency. It reduces operational overhead and is suitable for a single-node development broker.

## Broker Configuration

The broker is configured as a single-node, single-broker development instance using the `apache/kafka:3.8.0` image.

It runs in KRaft mode, uses a fixed broker ID, persists Kafka data through a Docker volume, and exposes the local Kafka port required for development.

Key broker settings:

* Broker ID: `1`
* Process roles: `broker,controller`
* Controller quorum: `1@kafka:9093`
* Advertised listener: `localhost:9092`
* Single-broker replication factor: `1`
* Persistent local data volume: `kafka-data`
* Docker network: `icestream-network`
* Kafka host port: `9092`

## Topic Names

The project creates the following Kafka topics:

1. `checkout-events` - primary checkout data stream
2. `checkout-dlq` - dead-letter topic for future bad-data handling
3. `icestream-control` - control and coordination stream

## Partition Counts

For this local development configuration:

* `checkout-events`: 3 partitions
* `checkout-dlq`: 3 partitions
* `icestream-control`: 1 partition

These counts are intentionally lightweight for a one-node development environment and are sufficient for local testing.

Production deployment would use multiple Kafka brokers and appropriate replication factors.

## Local Ports

* Kafka broker: `localhost:9092`
* Kafka controller: `9093` inside the Docker network

### Client Connection Rules

Windows host applications should connect using:

```text
localhost:9092
```

Applications running inside containers connected to `icestream-network` should connect using:

```text
kafka:9092
```

This distinction is important because `localhost` inside a container refers to that container itself, not the Kafka broker.

## How to Start Kafka

From the repository root:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml up -d
```

Check the broker status:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml ps
```

The Kafka service should eventually report a healthy status.

Then initialize the required topics:

```powershell
powershell -ExecutionPolicy Bypass -File .\infrastructure\kafka\init-topics.ps1
```

## How to Stop Kafka

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml down
```

If you also want to remove the persistent Kafka volume:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml down -v
```

Removing the volume deletes the local Kafka data and topic contents.

## How to Inspect Topics

List all topics:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe the primary checkout topic:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic checkout-events
```

Describe the dead-letter topic:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic checkout-dlq
```

Describe the control topic:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic icestream-control
```

## How to Produce a Test Event

A test checkout event can be produced from inside the Kafka container:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka bash -lc "printf '%s\n' '{\"event_id\":\"test-001\",\"event_type\":\"checkout\",\"test\":true}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic checkout-events"
```

## How to Consume a Test Event

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic checkout-events --from-beginning --max-messages 1
```

A successfully produced event should be returned by the consumer.

## Troubleshooting Common Startup Problems

### The Broker Never Reports Healthy

Check the container status:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml ps
```

Check the Kafka logs:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml logs kafka
```

### Docker Cannot Connect to the Kafka Container

Check Docker:

```powershell
docker info
```

Then check the Kafka container:

```powershell
docker compose -f infrastructure/kafka/docker-compose.kafka.yml ps
```

If Docker itself is unavailable, start Docker Desktop before starting Kafka.

### The Advertised Listener Does Not Match the Client

This project uses:

```text
PLAINTEXT://localhost:9092
```

for Windows host clients.

Host applications should use:

```text
localhost:9092
```

Applications running inside a container on `icestream-network` should use:

```text
kafka:9092
```

### Topics Are Not Created

Run the initialization script again:

```powershell
powershell -ExecutionPolicy Bypass -File .\infrastructure\kafka\init-topics.ps1
```

The script uses `--if-not-exists`, so running it multiple times is safe.

### The Port Is Already in Use

Check whether another application is using port `9092`.

The IceStream Kafka broker requires:

```text
localhost:9092
```

If another Kafka installation is running, stop it before starting the IceStream broker.

## Local Development Note

This is a local development configuration only.

It is intentionally single-broker and non-production scaled. Its purpose is to provide a stable Kafka foundation for the remaining IceStream components.

Future commits will add the transaction generator, Apache Flink processing, Apache Iceberg storage, data-quality monitoring, circuit-breaker logic, DLQ processing, remediation, and the observability dashboard.

## Commit Scope

This Kafka infrastructure commit contains only:

* Local Kafka broker configuration
* KRaft-based single-node deployment
* Docker network and persistent volume
* Kafka topic definitions
* Idempotent topic initialization
* Kafka infrastructure documentation

Flink, Iceberg, data-quality monitoring, remediation, and frontend functionality are intentionally excluded from this commit.
