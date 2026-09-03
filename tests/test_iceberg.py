from pathlib import Path

from iceberg.config import IcebergConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = PROJECT_ROOT / "iceberg" / "schema" / "checkout_events.sql"


def test_iceberg_table_identifier():
    assert IcebergConfig.table_identifier() == (
        "icestream.checkout.checkout_events"
    )


def test_iceberg_schema_file_exists():
    assert SCHEMA_FILE.exists()


def test_iceberg_schema_contains_required_fields():
    schema = SCHEMA_FILE.read_text(encoding="utf-8")

    required_fields = [
        "event_id STRING",
        "event_type STRING",
        "event_timestamp TIMESTAMP",
        "user_id STRING",
        "product_id STRING",
        "quantity INT",
        "unit_price DOUBLE",
        "subtotal DOUBLE",
        "discount_amount DOUBLE",
        "shipping_amount DOUBLE",
        "tax_amount DOUBLE",
        "total_amount DOUBLE",
        "tax_was_null BOOLEAN",
        "calculated_total_amount DOUBLE",
        "amount_difference DOUBLE",
        "amount_consistent BOOLEAN",
        "has_discount BOOLEAN",
        "processed BOOLEAN",
        "processing_stage STRING",
        "processed_at TIMESTAMP",
    ]

    for field in required_fields:
        assert field in schema