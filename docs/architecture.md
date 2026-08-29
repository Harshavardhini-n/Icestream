# IceStream Architecture

## Purpose

IceStream is a lakehouse observability platform for real-time transactional data. The system is intended to ingest, validate, process, and monitor event streams while preserving operational resilience when data quality or downstream system issues occur.

This document describes the intended architecture and explains why each component is included in the overall design. It is a planning document and does not claim that any of these components are already implemented.

## System overview

The intended data flow is:

Python Generator
→ Kafka
→ Flink
→ Iceberg
→ Observability
→ Circuit Breaker
→ DLQ / Quarantine
→ Recovery
→ React Flow Dashboard

## Components and responsibilities

### Python

Python provides the transactional data generation layer and the majority of the operational logic for local development and data engineering workflows.

Why IceStream uses Python:
- strong ecosystem for data processing and automation
- easy integration with Kafka clients and stream-processing tooling
- straightforward implementation of quality checks and validation logic
- fast prototyping for data engineering prototypes and operational tooling

### Kafka

Kafka provides the durable, high-throughput messaging layer between generators and downstream processing systems.

Why IceStream uses Kafka:
- decouples producers from consumers
- supports event-driven architecture at high scale
- provides buffering and replay-friendly event distribution
- enables real-time ingestion patterns for observability and analytics pipelines

### Flink

Flink provides stream processing for transformations, enrichment, computing validation metrics, and identifying data anomalies in motion.

Why IceStream uses Flink:
- optimized for continuous event processing
- supports stateful streaming logic and time-window operations
- handles real-time quality checks and operational alerts effectively
- integrates naturally with message bus and lakehouse storage patterns

### Iceberg

Iceberg gives the platform a table format designed for lakehouse analytics, lineage, schema evolution, and reliable data storage.

Why IceStream uses Iceberg:
- supports table evolution without brittle migrations
- works well with analytical lakehouse architectures
- improves reliability for large-scale historical and operational datasets
- provides better compatibility for downstream analytics and governance workflows

### MinIO

MinIO is the local object storage layer used to emulate S3-compatible storage in development and testing.

Why IceStream uses MinIO:
- mirrors common cloud object storage patterns without requiring a cloud dependency
- supports lakehouse-style data storage during local development
- simplifies reproducible testing for Iceberg and schema-driven ingestion
- fits the Docker-based local environment model

### Python data-quality engine

The Python data-quality layer is intended to validate schema integrity, nullability, anomaly rates, frequency patterns, and suspicious value distributions.

Why IceStream uses it:
- makes data quality measurable and transparent
- provides explicit rules for what constitutes healthy output
- helps identify drift or malformed records before they reach downstream consumers
- supports operational dashboards and automated remediation decisions

### FastAPI

FastAPI provides the API surface for health checks, operational controls, and integration endpoints.

Why IceStream uses FastAPI:
- modern, fast Python web framework
- strong async support for real-time monitoring endpoints
- clean API definitions for lifecycle and health operations
- easy integration with observability and remediation tools

### React

React provides the interactive frontend foundation for the monitoring experience.

Why IceStream uses React:
- mature ecosystem for dashboards and live UI workflows
- good fit for dynamic operational data visualizations
- simplifies building complex interfaces with reusable components
- supports modern frontend tooling and deployment patterns

### React Flow

React Flow is intended for visualizing the event pipeline and operational topology as interactive nodes and edges.

Why IceStream uses React Flow:
- naturally represents streaming architecture as a graph
- helps users see the relationship between producers, topics, jobs, and storage layers
- supports live status displays and operational topology monitoring
- makes the platform easier to reason about during incident response

### WebSockets

WebSockets enable near real-time communication between backend systems and the dashboard.

Why IceStream uses WebSockets:
- allows live updates without polling overhead
- supports immediate feedback for health, drift, and remediation events
- keeps the dashboard aligned with operational realities as data changes
- improves response time for incident and quality monitoring workflows

## Future operational flow

The eventual system is intended to combine these components into a resilient pipeline:

1. Python generates transaction events.
2. Events are published to Kafka topics.
3. Flink consumes and transforms the stream.
4. Validated data is written to Iceberg tables in object storage.
5. Data quality checks detect anomalies and drift.
6. The observability engine surfaces reliability metrics.
7. Circuit breakers isolate failing downstream flow.
8. Dead-letter queues quarantine invalid payloads.
9. Recovery logic reprocesses or repairs the pipeline automatically.
10. The React dashboard visualizes health and recovery status in near real time.

## Important note

All of the technologies above are planned architecture decisions for the future platform. The current commit establishes the repository foundation only and does not claim those systems are yet implemented.
