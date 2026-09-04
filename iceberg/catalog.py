import os


class IcebergCatalogConfig:
    """Configuration for the Iceberg REST catalog."""

    CATALOG_URI = os.getenv(
        "ICEBERG_CATALOG_URI",
        "http://iceberg-rest:8181",
    )

    WAREHOUSE = os.getenv(
        "ICEBERG_WAREHOUSE",
        "s3://icestream/",
    )

    S3_ENDPOINT = os.getenv(
        "S3_ENDPOINT",
        "http://minio:9000",
    )

    S3_ACCESS_KEY = os.getenv(
        "AWS_ACCESS_KEY_ID",
        "icestream",
    )

    S3_SECRET_KEY = os.getenv(
        "AWS_SECRET_ACCESS_KEY",
        "icestream-minio",
    )