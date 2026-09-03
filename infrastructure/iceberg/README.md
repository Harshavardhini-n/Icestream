# Iceberg Infrastructure

Apache Iceberg is the table format used by IceStream for the lakehouse storage layer.

## Purpose

Iceberg provides a reliable table abstraction for processed checkout events.

The target architecture is:

Kafka
  ↓
Flink
  ↓
Iceberg
  ↓
Observability / Analytics

## Current Commit

This commit establishes the Iceberg table schema and storage-layer structure.

The actual Kafka → Flink → Iceberg write pipeline is implemented in a later commit.

## Table

The primary table is:

`checkout_events`

It contains:

- Original checkout event fields
- Normalized numeric values
- Derived financial metrics
- Processing metadata
- Data-quality indicators

## Design

The table is intentionally designed around the processed event produced by Flink.

Important derived fields include:

- `calculated_total_amount`
- `amount_difference`
- `amount_consistent`
- `tax_was_null`
- `has_discount`

These fields allow later observability components to detect financial inconsistencies and data-quality problems.

## Storage

The physical object storage layer is introduced separately through MinIO.

Iceberg and MinIO are intentionally kept as separate infrastructure concerns:

- Iceberg → table format and metadata
- MinIO → object storage

The integration between them is added in a later commit.