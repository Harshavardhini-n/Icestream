# MinIO Infrastructure

MinIO provides S3-compatible object storage for IceStream's local lakehouse environment.

## Purpose

MinIO acts as the object storage layer used by Apache Iceberg.

The intended architecture is:

Kafka
  ↓
Flink
  ↓
Iceberg
  ↓
MinIO

## Services

| Service | Purpose |
|---|---|
| MinIO API | S3-compatible object storage |
| MinIO Console | Storage administration |

## Ports

- `9000` - S3 API
- `9001` - MinIO web console

## Development Credentials

The development environment uses:

- Access key: `icestream`
- Secret key: `icestream-minio`

These credentials are for local development only.

## Bucket

The planned IceStream bucket is:

`icestream`

The bucket and Iceberg warehouse integration will be configured in a later commit.

## Scope

This commit only establishes MinIO infrastructure.

The Iceberg → MinIO integration is implemented separately.