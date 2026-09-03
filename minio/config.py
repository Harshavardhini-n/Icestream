import os


class MinIOConfig:
    """Configuration for IceStream's S3-compatible object storage."""

    ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "icestream")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "icestream-minio")

    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "icestream")

    SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"