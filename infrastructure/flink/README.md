# Flink Infrastructure

Docker Compose setup for Apache Flink development and testing.

## Overview

This directory contains the Docker Compose configuration for running Apache Flink as a stream processing layer for IceStream.

## Components

### JobManager (`flink-jobmanager`)
- Runs the Flink JobManager daemon
- Orchestrates job execution
- Exposes Web UI on port 8081
- Exposes RPC interface on port 6123

### TaskManager (`flink-taskmanager`)
- Runs the Flink TaskManager daemon
- Executes parallel processing tasks
- Configured with 2 task slots for development
- Allocates 1GB memory for processing

## Network

Both JobManager and TaskManager are connected to the `icestream-network` Docker network, allowing communication with:
- Kafka broker (hostname: `kafka`)
- Other IceStream services

**Important**: When Flink containers connect to Kafka, use the internal Docker hostname `kafka:9092`, not `localhost:9092`.

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Kafka running (see `../kafka/`)
- Network created: `icestream-network` (created by Kafka setup)

### Start Flink Cluster

```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml up -d
```

### Verify Flink Is Running

```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml ps
```

Expected output:
```
NAME                              STATUS
icestream-flink-jobmanager        running
icestream-flink-taskmanager       running
```

### Access Web UI

Open browser to: `http://localhost:8081`

You should see the Flink dashboard with JobManager status and available task slots.

### View Logs

```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml logs -f
```

### Stop Flink Cluster

```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml down
```

## Full Stack Startup Sequence

```bash
# 1. Start Kafka
docker compose -f infrastructure/kafka/docker-compose.kafka.yml up -d

# 2. Initialize Kafka topics (creates checkout-events, processed-checkout-events, etc.)
# Run from repository root or adjust path:
# infrastructure/kafka/init-topics.ps1

# 3. Start Flink
docker compose -f infrastructure/flink/docker-compose.flink.yml up -d

# 4. Verify all services running
docker compose -f infrastructure/kafka/docker-compose.kafka.yml ps
docker compose -f infrastructure/flink/docker-compose.flink.yml ps
```

## Configuration

Flink configuration is set via environment variables in the compose file:

```yaml
FLINK_PROPERTIES: |
  jobmanager.rpc.address: flink-jobmanager
  jobmanager.rpc.port: 6123
  taskmanager.numberOfTaskSlots: 2
  parallelism.default: 2
  state.backend: hashmap
  taskmanager.memory.process.size: 1g
  jobmanager.memory.process.size: 1g
```

### Key Settings

| Setting | Value | Purpose |
|---------|-------|---------|
| `jobmanager.rpc.address` | `flink-jobmanager` | JobManager hostname |
| `jobmanager.rpc.port` | `6123` | RPC port for TaskManager communication |
| `taskmanager.numberOfTaskSlots` | `2` | Parallel task capacity per TaskManager |
| `parallelism.default` | `2` | Default parallelism for jobs |
| `state.backend` | `hashmap` | In-memory state for dev (not production-ready) |
| `taskmanager.memory.process.size` | `1g` | Memory per TaskManager |
| `jobmanager.memory.process.size` | `1g` | Memory per JobManager |

## Monitoring

### Health Checks

Both containers include health checks:

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -s http://localhost:8081/overview | grep -q 'Running' || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 10
  start_period: 30s
```

Monitor health status:

```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml ps
```

### Metrics

Access Flink metrics via REST API:

```bash
# JobManager status
curl http://localhost:8081/v1/overview

# Job details (after job submission)
curl http://localhost:8081/v1/jobs
```

### Logs

```bash
# All logs
docker compose -f infrastructure/flink/docker-compose.flink.yml logs

# Follow logs
docker compose -f infrastructure/flink/docker-compose.flink.yml logs -f

# JobManager only
docker compose -f infrastructure/flink/docker-compose.flink.yml logs flink-jobmanager

# TaskManager only
docker compose -f infrastructure/flink/docker-compose.flink.yml logs flink-taskmanager
```

## Troubleshooting

### Flink Won't Start

**Issue**: `flink-jobmanager` or `flink-taskmanager` container exits immediately

**Solution**:
1. Check logs: `docker compose -f infrastructure/flink/docker-compose.flink.yml logs`
2. Ensure Docker has sufficient resources (CPU, memory)
3. Check if ports 8081, 6123 are available
4. Verify network exists: `docker network ls | grep icestream-network`

### Can't Connect to Kafka

**Issue**: Flink job fails to connect to Kafka

**Solution**:
1. Verify Kafka is running: `docker compose -f infrastructure/kafka/docker-compose.kafka.yml ps`
2. Verify correct hostname is used in Flink config: `kafka:9092` (not `localhost:9092`)
3. Test Kafka connectivity from Flink container:
   ```bash
   docker compose -f infrastructure/flink/docker-compose.flink.yml exec flink-taskmanager \
     bash -c "apt-get update && apt-get install -y netcat && nc -zv kafka 9092"
   ```

### Out of Memory

**Issue**: Flink tasks fail with `OutOfMemoryError`

**Solution**: Increase memory in compose file:
```yaml
taskmanager.memory.process.size: 2g  # Increase from 1g
jobmanager.memory.process.size: 2g   # Increase from 1g
```

Then restart:
```bash
docker compose -f infrastructure/flink/docker-compose.flink.yml down
docker compose -f infrastructure/flink/docker-compose.flink.yml up -d
```

### Ports Already in Use

**Issue**: `Error: Port 8081 is already in use`

**Solution**:
1. Stop other Flink instances: `docker ps | grep flink`
2. Or use different ports in compose file:
   ```yaml
   ports:
     - "8082:8081"  # Map to 8082 instead
   ```

## Security Notes

This setup is for **local development only**. For production deployments:

- Use authenticated Kafka connections
- Restrict network access
- Enable TLS for inter-component communication
- Set proper resource limits and QoS
- Use persistent volumes for state
- Implement proper monitoring and alerting
- Use a production-grade state backend (RocksDB, S3, etc.)

## Related Documentation

- [Flink Jobs](../../flink/README.md)
- [Kafka Infrastructure](../kafka/README.md)
- [Main Architecture](../../docs/architecture.md)
