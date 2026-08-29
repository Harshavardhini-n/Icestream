# IceStream

## Problem statement

Modern data platforms often receive high-volume transactional events that need to be validated, streamed, stored, and monitored in near real time. In many organizations, reliability issues are discovered only after data quality or freshness problems have already impacted downstream systems.

IceStream is designed to address that challenge by creating a lakehouse observability pipeline that turns event streams into a traceable, reliable, and recoverable data platform foundation.

## Project objective

IceStream aims to build a real-time lakehouse observability platform that ingests transaction events, validates them, stores them in a lakehouse-compatible format, and exposes operational health through a live dashboard.

The system is designed to emphasize:
- reliability
- data quality
- operational visibility
- automated recovery
- observability for streaming data workflows

## Planned architecture

Python Generator
→ Kafka
→ Flink
→ Iceberg
→ Observability
→ Circuit Breaker
→ DLQ / Quarantine
→ Recovery
→ React Flow Dashboard

## Core technologies

- Python for transaction generation and validation logic
- Apache Kafka for event streaming and decoupled communication
- Apache Flink for stream processing and transformations
- Apache Iceberg for lakehouse table management and schema evolution
- MinIO for object storage compatibility in local development
- FastAPI for operational API access
- React and React Flow for live pipeline visualization and monitoring
- WebSockets for real-time dashboard updates
- Docker and Docker Compose for local environment orchestration

## Repository structure

- `apps/` - application modules for generator, observability, remediation, API, and frontend
- `flink/` - Flink job definitions and SQL assets
- `infrastructure/` - Docker, Kafka, Flink, and Iceberg-related deployment assets
- `data/` - schemas and sample datasets
- `tests/` - future test suite structure
- `docs/` - architecture and development documentation

## Current implementation status

This repository has completed Commit 1 and Commit 2 of the planned implementation roadmap.

Commit 1 — Project foundation — COMPLETE
- repository structure
- environment template
- project README
- architecture documentation
- minimal Python package skeleton
- test documentation

Commit 2 — Kafka streaming infrastructure — COMPLETE
- local Kafka broker through Docker Compose
- KRaft-based single-node deployment
- Kafka topics for checkout and control streams
- topic initialization script for idempotent local setup
- local documentation for broker startup and verification

The following components are not yet implemented and remain future work:
- Python transaction generator
- Apache Flink
- Apache Iceberg
- MinIO
- Great Expectations
- custom data-quality engine
- circuit breaker
- DLQ processing logic
- FastAPI
- React
- React Flow
- WebSockets
- automatic recovery

## Planned 20-commit development roadmap

1. Project foundation and repository structure
2. Local environment orchestration
3. Transaction generator skeleton
4. Event schema definitions
5. Kafka producer setup
6. Kafka topics and configuration
7. Flink job skeleton
8. Stream processing operators
9. Iceberg table setup
10. Storage integration with MinIO
11. Data quality rules
12. Observability engine baseline
13. Circuit breaker logic
14. DLQ and quarantine flow
15. Recovery automation
16. FastAPI service foundation
17. Dashboard shell
18. React Flow monitoring view
19. Real-time WebSocket updates
20. End-to-end validation and hardening

## Local development prerequisites

Before future implementation milestones, ensure the following are available:
- Python 3.11+
- Docker Desktop or Docker Engine
- Docker Compose
- Git
- Node.js and npm for frontend work
- WSL2 if running Docker in a Windows environment

## Notes

This repository currently includes the local Kafka infrastructure foundation for Commit 2. Future commits will progressively add each remaining architectural component without skipping the planned sequence.
