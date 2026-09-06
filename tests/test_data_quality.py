"""Tests for Commit 11 data quality rules."""

from __future__ import annotations

from flink.jobs.checkout_processor import (
    DataQualityChecker,
    EventMetricsCalculator,
    EventNormalizer,
)


def make_valid_event() -> dict:
    """Return a valid checkout event."""
    return {
        "event_id": "evt-dq-001",
        "event_timestamp": "2026-09-01T12:00:00.000Z",
        "customer_id": "cust-001",
        "session_id": "sess-001",
        "product_id": "prod-001",
        "quantity": 2,
        "unit_price": 49.99,
        "subtotal": 99.98,
        "discount_amount": 10.00,
        "shipping_amount": 5.99,
        "tax_amount": 7.50,
        "total_amount": 103.47,
        "currency": "USD",
        "payment_method": "card",
        "event_type": "checkout",
    }


def prepare_event(event: dict) -> dict:
    """Run normalization and metric calculation before DQ checks."""
    normalized = EventNormalizer().normalize(event)
    return EventMetricsCalculator().calculate(
        normalized
    )


class TestDataQualityChecker:
    """Tests for Commit 11 data quality rules."""

    def test_valid_event_passes(self):
        event = prepare_event(
            make_valid_event()
        )

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_checked"] is True
        assert result["data_quality_status"] == "passed"
        assert result["data_quality_errors"] == []

        assert checker.checked_count == 1
        assert checker.passed_count == 1
        assert checker.failed_count == 0

    def test_missing_identity_field_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["customer_id"] = ""

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "customer_id_missing" in (
            result["data_quality_errors"]
        )

    def test_zero_quantity_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["quantity"] = 0

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "quantity_must_be_positive" in (
            result["data_quality_errors"]
        )

    def test_fractional_quantity_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["quantity"] = 1.5

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "quantity_must_be_integer" in (
            result["data_quality_errors"]
        )

    def test_negative_monetary_value_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["shipping_amount"] = -5.00

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "shipping_amount_must_be_non_negative" in (
            result["data_quality_errors"]
        )

    def test_discount_cannot_exceed_subtotal(self):
        event = prepare_event(
            make_valid_event()
        )
        event["discount_amount"] = 150.00

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "discount_exceeds_subtotal" in (
            result["data_quality_errors"]
        )

    def test_inconsistent_subtotal_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["subtotal"] = 80.00

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "subtotal_inconsistent" in (
            result["data_quality_errors"]
        )

    def test_inconsistent_total_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["total_amount"] = 999.99

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "total_amount_inconsistent" in (
            result["data_quality_errors"]
        )

    def test_null_tax_is_allowed_after_normalization(self):
        event = make_valid_event()
        event["tax_amount"] = None

        prepared = prepare_event(event)

        checker = DataQualityChecker()
        result = checker.check(prepared)

        assert result["tax_amount"] == 0.0
        assert result["tax_was_null"] is True
        assert result["data_quality_status"] == "passed"

    def test_invalid_currency_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["currency"] = "US"

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "currency_invalid" in (
            result["data_quality_errors"]
        )

    def test_empty_payment_method_fails(self):
        event = prepare_event(
            make_valid_event()
        )
        event["payment_method"] = " "

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"
        assert "payment_method_invalid" in (
            result["data_quality_errors"]
        )

    def test_multiple_quality_errors_are_recorded(self):
        event = prepare_event(
            make_valid_event()
        )

        event["customer_id"] = ""
        event["quantity"] = 0
        event["discount_amount"] = 150.00
        event["currency"] = "X"

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["data_quality_status"] == "failed"

        errors = result["data_quality_errors"]

        assert "customer_id_missing" in errors
        assert "quantity_must_be_positive" in errors
        assert "discount_exceeds_subtotal" in errors
        assert "currency_invalid" in errors

    def test_failed_event_is_not_removed(self):
        event = prepare_event(
            make_valid_event()
        )
        event["quantity"] = -1

        checker = DataQualityChecker()
        result = checker.check(event)

        assert result["event_id"] == "evt-dq-001"
        assert result["data_quality_checked"] is True
        assert result["data_quality_status"] == "failed"

    def test_checker_counts_passed_and_failed_events(self):
        checker = DataQualityChecker()

        valid = prepare_event(
            make_valid_event()
        )

        invalid = prepare_event(
            make_valid_event()
        )
        invalid["quantity"] = -2

        checker.check(valid)
        checker.check(invalid)

        assert checker.checked_count == 2
        assert checker.passed_count == 1
        assert checker.failed_count == 1