import os


class IcebergConfig:
    """Configuration for the IceStream Iceberg storage layer."""

    CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "icestream")
    NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "checkout")
    TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "checkout_events")

    @classmethod
    def table_identifier(cls) -> str:
        return f"{cls.CATALOG_NAME}.{cls.NAMESPACE}.{cls.TABLE_NAME}"