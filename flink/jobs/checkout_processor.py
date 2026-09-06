"""
Flink job for processing checkout events from Kafka.

Commit 11 responsibilities:
1. Consume raw checkout events from Kafka.
2. Deserialize JSON payloads.
3. Validate event structure.
4. Filter malformed and invalid events.
5. Normalize transaction values.
6. Calculate derived transaction metrics.
7. Apply data quality rules.
8. Add processing metadata.
9. Serialize processed events.
10. Publish processed events to Kafka.

The business transformation and data quality logic are kept
framework-independent so they can be unit tested without requiring
a running Flink cluster.

Commit 11 only detects and annotates data quality failures.
Quarantine, DLQ handling, circuit breaking, and recovery are
implemented in later commits.
"""

from __future__ import annotations

import json
import logging
import math
import sys
from datetime import datetime, timezone
from typing import Any

from flink.config import FlinkConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================================
# Constants
# ============================================================================

REQUIRED_FIELDS = {
    "event_id",
    "event_timestamp",
    "customer_id",
    "session_id",
    "product_id",
    "quantity",
    "unit_price",
    "subtotal",
    "discount_amount",
    "shipping_amount",
    "tax_amount",
    "total_amount",
    "currency",
    "payment_method",
    "event_type",
}

AMOUNT_TOLERANCE = 0.01

NUMERIC_FIELDS = {
    "quantity",
    "unit_price",
    "subtotal",
    "discount_amount",
    "shipping_amount",
    "tax_amount",
    "total_amount",
}

MONETARY_FIELDS = {
    "unit_price",
    "subtotal",
    "discount_amount",
    "shipping_amount",
    "tax_amount",
    "total_amount",
}


# ============================================================================
# Event Deserialization
# ============================================================================


class EventDeserializer:
    """Deserialize JSON strings into dictionaries."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.malformed_count = 0

    def deserialize(self, message: str) -> dict[str, Any] | None:
        """Deserialize a JSON message."""
        try:
            event = json.loads(message)

            if not isinstance(event, dict):
                self.malformed_count += 1

                self.logger.warning(
                    "JSON root is not an object | message=%s",
                    message[:100],
                )

                return None

            return event

        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            self.malformed_count += 1

            self.logger.warning(
                "Malformed JSON (count=%s): %s | message=%s",
                self.malformed_count,
                str(exc)[:100],
                message[:100],
            )

            return None


# ============================================================================
# Event Validation
# ============================================================================


class EventValidator:
    """Validate the basic structure and numeric fields of an event."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_error_count = 0
        self.validation_success_count = 0

    def validate(
        self,
        event: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Validate event structure.

        Extra fields are allowed.
        tax_amount is allowed to be None.
        """
        if event is None:
            return None

        try:
            missing_fields = REQUIRED_FIELDS - set(event.keys())

            if missing_fields:
                self.validation_error_count += 1

                self.logger.warning(
                    "Missing required fields: %s | event_id=%s",
                    sorted(missing_fields),
                    event.get("event_id", "UNKNOWN"),
                )

                return None

            for field in NUMERIC_FIELDS:
                value = event.get(field)

                if field == "tax_amount" and value is None:
                    continue

                float(value)

            self.validation_success_count += 1
            return event

        except (TypeError, ValueError):
            self.validation_error_count += 1

            self.logger.warning(
                "Invalid numeric field type | event_id=%s",
                event.get("event_id", "UNKNOWN"),
            )

            return None

        except Exception as exc:
            self.validation_error_count += 1

            self.logger.error(
                "Validation exception: %s",
                exc,
                exc_info=True,
            )

            return None


# ============================================================================
# Commit 7 — Stream Processing Operators
# ============================================================================


class EventNormalizer:
    """Normalize numeric transaction fields."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def normalize(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Normalize numeric values without removing original fields."""
        normalized = dict(event)

        tax_was_null = normalized.get("tax_amount") is None

        for field in NUMERIC_FIELDS:
            if field == "tax_amount":
                normalized[field] = (
                    0.0
                    if normalized.get(field) is None
                    else float(normalized[field])
                )
            else:
                normalized[field] = float(normalized[field])

        normalized["tax_was_null"] = tax_was_null

        return normalized


class EventMetricsCalculator:
    """Calculate derived transaction metrics."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def calculate(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Calculate transaction-level metrics."""
        result = dict(event)

        subtotal = float(result["subtotal"])
        discount = float(result["discount_amount"])
        shipping = float(result["shipping_amount"])
        tax = float(result["tax_amount"])
        supplied_total = float(result["total_amount"])

        calculated_total = (
            subtotal
            - discount
            + shipping
            + tax
        )

        amount_difference = (
            supplied_total - calculated_total
        )

        result["calculated_total_amount"] = round(
            calculated_total,
            2,
        )

        result["amount_difference"] = round(
            amount_difference,
            2,
        )

        result["amount_consistent"] = (
            abs(amount_difference) <= AMOUNT_TOLERANCE
        )

        result["has_discount"] = discount > 0

        return result


class BusinessEventFilter:
    """Filter events that should not enter the processed stream."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.filtered_count = 0

    def keep(
        self,
        event: dict[str, Any] | None,
    ) -> bool:
        """Return True for usable checkout events."""
        if event is None:
            self.filtered_count += 1
            return False

        event_type = event.get("event_type")

        if event_type != "checkout":
            self.filtered_count += 1

            self.logger.warning(
                "Filtering unsupported event type=%s | event_id=%s",
                event_type,
                event.get("event_id", "UNKNOWN"),
            )

            return False

        return True


# ============================================================================
# Commit 11 — Data Quality Rules
# ============================================================================


class DataQualityChecker:
    """Apply business-level data quality rules.

    The checker does not remove, quarantine, or reject the event.

    Instead, it annotates the event with:

        data_quality_checked
        data_quality_status
        data_quality_errors

    Failed events remain in the stream so later commits can implement
    quarantine, DLQ, circuit-breaker, and recovery behavior.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.checked_count = 0
        self.passed_count = 0
        self.failed_count = 0

    def check(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Run all data quality rules against an event."""
        result = dict(event)

        errors: list[str] = []

        self._check_identity_fields(result, errors)
        self._check_quantity(result, errors)
        self._check_monetary_values(result, errors)
        self._check_discount(result, errors)
        self._check_subtotal(result, errors)
        self._check_tax(result, errors)
        self._check_total_consistency(result, errors)
        self._check_currency(result, errors)
        self._check_payment_method(result, errors)
        self._check_event_type(result, errors)

        self.checked_count += 1

        if errors:
            self.failed_count += 1
            status = "failed"

            self.logger.warning(
                "Data quality failure | event_id=%s | errors=%s",
                result.get("event_id", "UNKNOWN"),
                errors,
            )

        else:
            self.passed_count += 1
            status = "passed"

        result["data_quality_checked"] = True
        result["data_quality_status"] = status
        result["data_quality_errors"] = errors

        return result

    def _check_identity_fields(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Check transaction identity fields."""
        for field, error_code in (
            ("event_id", "event_id_missing"),
            ("customer_id", "customer_id_missing"),
            ("session_id", "session_id_missing"),
            ("product_id", "product_id_missing"),
        ):
            value = event.get(field)

            if value is None:
                errors.append(error_code)
                continue

            if isinstance(value, str) and not value.strip():
                errors.append(error_code)

    def _check_quantity(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Quantity must be a positive whole number."""
        quantity = event.get("quantity")

        try:
            numeric_quantity = float(quantity)

            if not math.isfinite(numeric_quantity):
                errors.append("quantity_must_be_finite")
                return

            if numeric_quantity <= 0:
                errors.append("quantity_must_be_positive")

            if not numeric_quantity.is_integer():
                errors.append("quantity_must_be_integer")

        except (TypeError, ValueError):
            errors.append("quantity_must_be_numeric")

    def _check_monetary_values(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Monetary values must be numeric, finite, and non-negative."""
        for field in MONETARY_FIELDS:
            value = event.get(field)

            try:
                numeric_value = float(value)

                if not math.isfinite(numeric_value):
                    errors.append(
                        f"{field}_must_be_finite"
                    )
                    continue

                if numeric_value < 0:
                    errors.append(
                        f"{field}_must_be_non_negative"
                    )

            except (TypeError, ValueError):
                errors.append(
                    f"{field}_must_be_numeric"
                )

    def _check_discount(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Discount cannot exceed subtotal."""
        subtotal = self._to_float(
            event.get("subtotal")
        )

        discount = self._to_float(
            event.get("discount_amount")
        )

        if subtotal is None or discount is None:
            return

        if discount > subtotal + AMOUNT_TOLERANCE:
            errors.append(
                "discount_exceeds_subtotal"
            )

    def _check_subtotal(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Subtotal should equal quantity multiplied by unit price."""
        quantity = self._to_float(
            event.get("quantity")
        )

        unit_price = self._to_float(
            event.get("unit_price")
        )

        subtotal = self._to_float(
            event.get("subtotal")
        )

        if (
            quantity is None
            or unit_price is None
            or subtotal is None
        ):
            return

        calculated_subtotal = quantity * unit_price

        if (
            abs(subtotal - calculated_subtotal)
            > AMOUNT_TOLERANCE
        ):
            errors.append(
                "subtotal_inconsistent"
            )

    def _check_tax(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Tax must be numeric after normalization.

        A source null tax is valid because EventNormalizer converts it
        to 0.0 and records tax_was_null=True.
        """
        tax = self._to_float(
            event.get("tax_amount")
        )

        if tax is None:
            errors.append(
                "tax_amount_invalid"
            )

    def _check_total_consistency(
        self,
        event: dict,
        errors: list[str],
    ) -> None:
        # If tax was originally null, the source event did not provide
        # enough information to independently validate the final total.
        # The normalizer has already converted tax_amount to 0.0.
        if event.get("tax_was_null") is True:
            return

        subtotal = self._to_float(event.get("subtotal"))
        discount = self._to_float(event.get("discount_amount"))
        shipping = self._to_float(event.get("shipping_amount"))
        tax = self._to_float(event.get("tax_amount"))
        total = self._to_float(event.get("total_amount"))

        if None in (subtotal, discount, shipping, tax, total):
            errors.append("total_amount_invalid")
            return

        calculated_total = round(
            subtotal - discount + shipping + tax,
            2,
        )

        difference = abs(total - calculated_total)

        if difference > AMOUNT_TOLERANCE:
            errors.append("total_amount_inconsistent")

    def _check_currency(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Currency must be a non-empty three-letter code."""
        currency = event.get("currency")

        if not isinstance(currency, str):
            errors.append("currency_invalid")
            return

        normalized_currency = currency.strip()

        if (
            len(normalized_currency) != 3
            or not normalized_currency.isalpha()
        ):
            errors.append("currency_invalid")

    def _check_payment_method(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Payment method must be a non-empty string."""
        payment_method = event.get(
            "payment_method"
        )

        if not isinstance(payment_method, str):
            errors.append(
                "payment_method_invalid"
            )
            return

        if not payment_method.strip():
            errors.append(
                "payment_method_invalid"
            )

    def _check_event_type(
        self,
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        """Only checkout events are valid for this processing pipeline."""
        if event.get("event_type") != "checkout":
            errors.append(
                "event_type_invalid"
            )

    @staticmethod
    def _to_float(
        value: Any,
    ) -> float | None:
        """Safely convert a value to a finite float."""
        try:
            converted = float(value)

            if not math.isfinite(converted):
                return None

            return converted

        except (TypeError, ValueError):
            return None


# ============================================================================
# Event Enrichment
# ============================================================================


class EventEnricher:
    """Add processing metadata to a processed event."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def enrich(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Add processing metadata."""
        result = dict(event)

        result["processed"] = True
        result["processing_stage"] = (
            "flink_stream_processor"
        )

        result["processed_timestamp"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        return result


# ============================================================================
# Event Serialization
# ============================================================================


class EventSerializer:
    """Serialize processed events to compact JSON."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def serialize(
        self,
        event: dict[str, Any],
    ) -> str:
        """Serialize event dictionary to JSON."""
        try:
            return json.dumps(
                event,
                separators=(",", ":"),
            )

        except (TypeError, ValueError) as exc:
            self.logger.error(
                "Serialization error: %s | event=%s",
                exc,
                event,
                exc_info=True,
            )

            return json.dumps(
                {
                    "error": "serialization_failed",
                    "event_id": event.get(
                        "event_id",
                        "UNKNOWN",
                    ),
                }
            )


# ============================================================================
# Flink-Specific Adapters
# ============================================================================


def _get_flink_adapters():
    """Create PyFlink adapters only when PyFlink is available."""
    try:
        from pyflink.datastream.functions import (
            FilterFunction,
            MapFunction,
        )

        class EventDeserializerAdapter(MapFunction):
            def __init__(self):
                self.deserializer = EventDeserializer()

            def map(self, message: str):
                return self.deserializer.deserialize(
                    message
                )

        class EventValidatorAdapter(MapFunction):
            def __init__(self):
                self.validator = EventValidator()

            def map(self, event):
                return self.validator.validate(
                    event
                )

        class BusinessEventFilterAdapter(FilterFunction):
            def __init__(self):
                self.event_filter = BusinessEventFilter()

            def filter(self, event):
                return self.event_filter.keep(
                    event
                )

        class EventNormalizerAdapter(MapFunction):
            def __init__(self):
                self.normalizer = EventNormalizer()

            def map(self, event):
                return self.normalizer.normalize(
                    event
                )

        class EventMetricsCalculatorAdapter(MapFunction):
            def __init__(self):
                self.calculator = (
                    EventMetricsCalculator()
                )

            def map(self, event):
                return self.calculator.calculate(
                    event
                )

        class DataQualityCheckerAdapter(MapFunction):
            def __init__(self):
                self.checker = DataQualityChecker()

            def map(self, event):
                return self.checker.check(event)

        class EventEnricherAdapter(MapFunction):
            def __init__(self):
                self.enricher = EventEnricher()

            def map(self, event):
                return self.enricher.enrich(
                    event
                )

        class EventSerializerAdapter(MapFunction):
            def __init__(self):
                self.serializer = EventSerializer()

            def map(self, event):
                return self.serializer.serialize(
                    event
                )

        return {
            "deserializer": EventDeserializerAdapter,
            "validator": EventValidatorAdapter,
            "filter": BusinessEventFilterAdapter,
            "normalizer": EventNormalizerAdapter,
            "metrics": EventMetricsCalculatorAdapter,
            "quality": DataQualityCheckerAdapter,
            "enricher": EventEnricherAdapter,
            "serializer": EventSerializerAdapter,
        }

    except ImportError:
        return None


# ============================================================================
# Main Flink Job
# ============================================================================


def run_checkout_processor(
    config: FlinkConfig,
) -> int:
    """Run the Flink checkout event processing job."""
    try:
        adapters = _get_flink_adapters()

        if not adapters:
            logger.error(
                "PyFlink is not installed. "
                "Install it with: pip install -r flink/requirements.txt"
            )
            return 1

        from pyflink.common import SimpleStringSchema
        from pyflink.datastream import (
            StreamExecutionEnvironment,
        )
        from pyflink.datastream.connectors.kafka import (
            FlinkKafkaConsumer,
            FlinkKafkaProducer,
        )

        logger.info(
            "Starting %s",
            config.flink_job_name,
        )

        logger.info(
            "Kafka bootstrap servers: %s",
            config.kafka_bootstrap_servers,
        )

        logger.info(
            "Input topic: %s",
            config.kafka_input_topic,
        )

        logger.info(
            "Output topic: %s",
            config.kafka_output_topic,
        )

        logger.info(
            "Parallelism: %s",
            config.flink_parallelism,
        )

        # ------------------------------------------------------------------
        # Flink execution environment
        # ------------------------------------------------------------------

        env = (
            StreamExecutionEnvironment
            .get_execution_environment()
        )

        env.set_parallelism(
            config.flink_parallelism
        )

        # ------------------------------------------------------------------
        # Kafka consumer
        # ------------------------------------------------------------------

        kafka_consumer = FlinkKafkaConsumer(
            topics=config.kafka_input_topic,
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": (
                    config.kafka_bootstrap_servers
                ),
                "group.id": (
                    config.kafka_consumer_group
                ),
                "auto.offset.reset": "earliest",
            },
        )

        # ------------------------------------------------------------------
        # Kafka producer
        # ------------------------------------------------------------------

        kafka_producer = FlinkKafkaProducer(
            topic=config.kafka_output_topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                "bootstrap.servers": (
                    config.kafka_bootstrap_servers
                ),
            },
        )

        # ------------------------------------------------------------------
        # Source
        # ------------------------------------------------------------------

        kafka_stream = env.add_source(
            kafka_consumer
        )

        # ------------------------------------------------------------------
        # Commit 11 processing pipeline
        # ------------------------------------------------------------------

        processed_stream = (
            kafka_stream

            # Operator 1: JSON deserialization
            .map(
                adapters["deserializer"]()
            )

            # Operator 2: schema/type validation
            .map(
                adapters["validator"]()
            )

            # Operator 3: remove malformed/
            # unsupported event types
            .filter(
                adapters["filter"]()
            )

            # Operator 4: normalize numeric values
            .map(
                adapters["normalizer"]()
            )

            # Operator 5: calculate derived metrics
            .map(
                adapters["metrics"]()
            )

            # Operator 6: apply data quality rules
            .map(
                adapters["quality"]()
            )

            # Operator 7: add processing metadata
            .map(
                adapters["enricher"]()
            )

            # Operator 8: serialize output JSON
            .map(
                adapters["serializer"]()
            )
        )

        # ------------------------------------------------------------------
        # Sink
        # ------------------------------------------------------------------

        processed_stream.add_sink(
            kafka_producer
        )

        logger.info(
            "Executing Flink job: %s",
            config.flink_job_name,
        )

        env.execute(
            config.flink_job_name
        )

        return 0

    except ImportError as exc:
        logger.error(
            "Required module not found: %s",
            exc,
            exc_info=True,
        )

        logger.error(
            "Ensure PyFlink is installed with: "
            "pip install -r flink/requirements.txt"
        )

        return 1

    except Exception as exc:
        logger.error(
            "Job failed with exception: %s",
            exc,
            exc_info=True,
        )

        return 1


if __name__ == "__main__":
    config = FlinkConfig.from_env()

    exit_code = run_checkout_processor(
        config
    )

    sys.exit(exit_code)
