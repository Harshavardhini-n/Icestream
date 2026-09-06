"""
IceStream Flink checkout event processor.

Commit 12:
- Kafka input/output configuration
- JSON deserialization
- Schema validation
- Business event filtering
- Event normalization
- Derived event metrics
- Data quality validation
- Processing enrichment
- Observability metrics
- JSON serialization

The Flink-specific imports are intentionally lazy so the module
can still be imported and unit-tested in the local Windows
Python environment where PyFlink may not be installed.
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

from flink.config import FlinkConfig


logger = logging.getLogger(__name__)


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


class EventDeserializer:
    """Deserialize Kafka JSON messages into Python dictionaries."""

    def __init__(self) -> None:
        # Commit 6/7 public counter.
        self.malformed_count = 0

        # Additional success counter.
        self.success_count = 0

    def deserialize(
        self,
        message: str | bytes,
    ) -> dict[str, Any] | None:
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            event = json.loads(message)

            if not isinstance(event, dict):
                raise ValueError(
                    "JSON payload must be an object"
                )

            self.success_count += 1
            return event

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            self.malformed_count += 1

            logger.warning(
                "Malformed JSON (count=%s): %s | message=%s",
                self.malformed_count,
                exc,
                message,
            )

            return None


class EventValidator:
    """Validate that an event contains required checkout fields."""

    def __init__(self) -> None:
        # Commit 6/7 public counters.
        self.validation_success_count = 0
        self.validation_error_count = 0

        # Descriptive aliases.
        self.success_count = 0
        self.failure_count = 0

    def validate(
        self,
        event: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """
        Validate an event.

        None is treated as an invalid event and returns None
        instead of raising an exception.
        """
        if event is None:
            self.validation_error_count += 1
            self.failure_count += 1
            return None

        missing_fields = sorted(
            field
            for field in REQUIRED_FIELDS
            if field not in event
        )

        if missing_fields:
            self.validation_error_count += 1
            self.failure_count += 1

            logger.warning(
                "Missing required fields: %s | event_id=%s",
                missing_fields,
                event.get("event_id", "UNKNOWN"),
            )

            return None

        for field in NUMERIC_FIELDS:
            value = event.get(field)

            if value is None:
                continue

            try:
                float(value)
            except (TypeError, ValueError):
                self.validation_error_count += 1
                self.failure_count += 1

                logger.warning(
                    "Invalid numeric field: %s=%r | event_id=%s",
                    field,
                    value,
                    event.get("event_id", "UNKNOWN"),
                )

                return None

        self.validation_success_count += 1
        self.success_count += 1

        return event


class EventNormalizer:
    """Normalize event values into consistent types and defaults."""

    def normalize(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(event)

        for field in NUMERIC_FIELDS:
            value = normalized.get(field)

            if value is None:
                continue

            try:
                numeric_value = float(value)

                # Quantity remains a float for compatibility with
                # the original Commit 7 implementation/tests.
                if field == "quantity":
                    normalized[field] = numeric_value
                else:
                    normalized[field] = round(
                        numeric_value,
                        2,
                    )

            except (TypeError, ValueError):
                # DataQualityChecker handles invalid numeric values.
                pass

        # Preserve information that the original tax value was null.
        if normalized.get("tax_amount") is None:
            normalized["tax_amount"] = 0.0
            normalized["tax_was_null"] = True
        else:
            normalized["tax_was_null"] = False

        currency = normalized.get("currency")

        if isinstance(currency, str):
            normalized["currency"] = (
                currency.strip().upper()
            )

        payment_method = normalized.get(
            "payment_method"
        )

        if isinstance(payment_method, str):
            normalized["payment_method"] = (
                payment_method.strip()
            )

        event_type = normalized.get("event_type")

        if isinstance(event_type, str):
            normalized["event_type"] = (
                event_type.strip().lower()
            )

        return normalized


class EventMetricsCalculator:
    """Calculate derived financial metrics."""

    def calculate(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        enriched = dict(event)

        quantity = self._to_float(
            enriched.get("quantity")
        )

        unit_price = self._to_float(
            enriched.get("unit_price")
        )

        subtotal = self._to_float(
            enriched.get("subtotal")
        )

        discount = self._to_float(
            enriched.get("discount_amount")
        )

        shipping = self._to_float(
            enriched.get("shipping_amount")
        )

        tax = self._to_float(
            enriched.get("tax_amount")
        )

        total = self._to_float(
            enriched.get("total_amount")
        )

        if (
            quantity is not None
            and unit_price is not None
        ):
            calculated_subtotal = round(
                quantity * unit_price,
                2,
            )
        else:
            calculated_subtotal = None

        if (
            subtotal is not None
            and discount is not None
            and shipping is not None
            and tax is not None
        ):
            calculated_total = round(
                subtotal
                - discount
                + shipping
                + tax,
                2,
            )
        else:
            calculated_total = None

        enriched["calculated_subtotal"] = (
            calculated_subtotal
        )

        enriched["calculated_total_amount"] = (
            calculated_total
        )

        if (
            total is not None
            and calculated_total is not None
        ):
            difference = round(
                total - calculated_total,
                2,
            )

            enriched["amount_difference"] = (
                difference
            )

            enriched["amount_consistent"] = (
                abs(difference)
                <= AMOUNT_TOLERANCE
            )

        else:
            enriched["amount_difference"] = None
            enriched["amount_consistent"] = False

        discount_value = discount or 0.0

        enriched["has_discount"] = (
            discount_value > 0
        )

        return enriched

    @staticmethod
    def _to_float(
        value: Any,
    ) -> float | None:
        try:
            if value is None:
                return None

            result = float(value)

            if not math.isfinite(result):
                return None

            return result

        except (TypeError, ValueError):
            return None


class BusinessEventFilter:
    """
    Keep only supported business checkout events.

    The public counters preserve the Commit 7 API.
    """

    def __init__(self) -> None:
        self.accepted_count = 0
        self.rejected_count = 0

        # Commit 7 public counter.
        self.filtered_count = 0

    def keep(
        self,
        event: dict[str, Any] | None,
    ) -> bool:
        """
        Return whether an event is a checkout event.

        This method intentionally updates filtered_count
        because the original Commit 7 tests use keep()
        directly.
        """
        if event is None:
            self.rejected_count += 1
            self.filtered_count += 1
            return False

        if event.get("event_type") != "checkout":
            self.rejected_count += 1
            self.filtered_count += 1
            return False

        self.accepted_count += 1
        return True

    def filter(
        self,
        event: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """
        Filter an event while preserving the original event object.
        """
        if not self.keep(event):
            if event is not None:
                logger.info(
                    "Business event filtered | event_id=%s | "
                    "event_type=%s",
                    event.get(
                        "event_id",
                        "UNKNOWN",
                    ),
                    event.get("event_type"),
                )

            return None

        return event


class DataQualityChecker:
    """
    Apply event-level data quality rules.

    Failed events remain in the stream during Commit 11/12.
    Later commits introduce quarantine and DLQ handling.
    """

    def __init__(self) -> None:
        self.checked_count = 0
        self.passed_count = 0
        self.failed_count = 0

    def check(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        self.checked_count += 1

        errors: list[str] = []

        self._check_identity_fields(
            event,
            errors,
        )

        self._check_quantity(
            event,
            errors,
        )

        self._check_monetary_values(
            event,
            errors,
        )

        self._check_discount(
            event,
            errors,
        )

        self._check_subtotal(
            event,
            errors,
        )

        self._check_tax(
            event,
            errors,
        )

        self._check_total_consistency(
            event,
            errors,
        )

        self._check_currency(
            event,
            errors,
        )

        self._check_payment_method(
            event,
            errors,
        )

        self._check_event_type(
            event,
            errors,
        )

        result = dict(event)

        result["data_quality_checked"] = True
        result["data_quality_errors"] = errors

        if errors:
            self.failed_count += 1

            result["data_quality_status"] = (
                "failed"
            )

            logger.warning(
                "Data quality failure | event_id=%s | errors=%s",
                event.get(
                    "event_id",
                    "UNKNOWN",
                ),
                errors,
            )

        else:
            self.passed_count += 1

            result["data_quality_status"] = (
                "passed"
            )

        return result

    @staticmethod
    def _check_identity_fields(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        identity_fields = (
            "event_id",
            "customer_id",
            "session_id",
            "product_id",
        )

        for field in identity_fields:
            value = event.get(field)

            if (
                not isinstance(value, str)
                or not value.strip()
            ):
                errors.append(
                    f"{field}_missing"
                )

    @staticmethod
    def _check_quantity(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        quantity = (
            DataQualityChecker._to_float(
                event.get("quantity")
            )
        )

        if quantity is None:
            errors.append(
                "quantity_invalid"
            )
            return

        if not math.isfinite(quantity):
            errors.append(
                "quantity_not_finite"
            )
            return

        if quantity <= 0:
            errors.append(
                "quantity_must_be_positive"
            )

        if not quantity.is_integer():
            errors.append(
                "quantity_must_be_integer"
            )

    @staticmethod
    def _check_monetary_values(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        for field in MONETARY_FIELDS:
            value = event.get(field)

            numeric_value = (
                DataQualityChecker._to_float(
                    value
                )
            )

            if numeric_value is None:
                errors.append(
                    f"{field}_invalid"
                )
                continue

            if not math.isfinite(numeric_value):
                errors.append(
                    f"{field}_not_finite"
                )
                continue

            if numeric_value < 0:
                errors.append(
                    f"{field}_must_be_non_negative"
                )

    @staticmethod
    def _check_discount(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        subtotal = (
            DataQualityChecker._to_float(
                event.get("subtotal")
            )
        )

        discount = (
            DataQualityChecker._to_float(
                event.get("discount_amount")
            )
        )

        if subtotal is None or discount is None:
            return

        if (
            discount
            > subtotal + AMOUNT_TOLERANCE
        ):
            errors.append(
                "discount_exceeds_subtotal"
            )

    @staticmethod
    def _check_subtotal(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        quantity = (
            DataQualityChecker._to_float(
                event.get("quantity")
            )
        )

        unit_price = (
            DataQualityChecker._to_float(
                event.get("unit_price")
            )
        )

        subtotal = (
            DataQualityChecker._to_float(
                event.get("subtotal")
            )
        )

        if (
            quantity is None
            or unit_price is None
            or subtotal is None
        ):
            return

        calculated_subtotal = round(
            quantity * unit_price,
            2,
        )

        if (
            abs(
                subtotal
                - calculated_subtotal
            )
            > AMOUNT_TOLERANCE
        ):
            errors.append(
                "subtotal_inconsistent"
            )

    @staticmethod
    def _check_tax(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        tax = (
            DataQualityChecker._to_float(
                event.get("tax_amount")
            )
        )

        if tax is None:
            errors.append(
                "tax_amount_invalid"
            )
            return

        if not math.isfinite(tax):
            errors.append(
                "tax_amount_not_finite"
            )
            return

        if tax < 0:
            errors.append(
                "tax_amount_must_be_non_negative"
            )

    @staticmethod
    def _check_total_consistency(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        # When tax was originally null, the source event did
        # not provide enough information to independently verify
        # whether the supplied total included a calculated tax.
        if event.get("tax_was_null") is True:
            return

        subtotal = (
            DataQualityChecker._to_float(
                event.get("subtotal")
            )
        )

        discount = (
            DataQualityChecker._to_float(
                event.get("discount_amount")
            )
        )

        shipping = (
            DataQualityChecker._to_float(
                event.get("shipping_amount")
            )
        )

        tax = (
            DataQualityChecker._to_float(
                event.get("tax_amount")
            )
        )

        total = (
            DataQualityChecker._to_float(
                event.get("total_amount")
            )
        )

        if None in (
            subtotal,
            discount,
            shipping,
            tax,
            total,
        ):
            errors.append(
                "total_amount_invalid"
            )
            return

        calculated_total = round(
            subtotal
            - discount
            + shipping
            + tax,
            2,
        )

        difference = abs(
            total - calculated_total
        )

        if difference > AMOUNT_TOLERANCE:
            errors.append(
                "total_amount_inconsistent"
            )

    @staticmethod
    def _check_currency(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        currency = event.get("currency")

        if (
            not isinstance(currency, str)
            or len(currency.strip()) != 3
            or not currency.strip().isalpha()
        ):
            errors.append(
                "currency_invalid"
            )

    @staticmethod
    def _check_payment_method(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        payment_method = event.get(
            "payment_method"
        )

        if (
            not isinstance(payment_method, str)
            or not payment_method.strip()
        ):
            errors.append(
                "payment_method_invalid"
            )

    @staticmethod
    def _check_event_type(
        event: dict[str, Any],
        errors: list[str],
    ) -> None:
        if event.get("event_type") != "checkout":
            errors.append(
                "event_type_invalid"
            )

    @staticmethod
    def _to_float(
        value: Any,
    ) -> float | None:
        try:
            if value is None:
                return None

            result = float(value)

            if not math.isfinite(result):
                return None

            return result

        except (TypeError, ValueError):
            return None


class ObservabilityEngine:
    """
    Track pipeline-level observability metrics.

    Commit 12 observes the pipeline but does not stop or
    pause it. Circuit-breaker behavior is introduced in
    Commit 13.
    """

    def __init__(
        self,
        max_recent_failures: int = 10,
    ) -> None:
        if max_recent_failures < 1:
            raise ValueError(
                "max_recent_failures must be at least 1"
            )

        self.max_recent_failures = (
            max_recent_failures
        )

        self.total_events = 0
        self.processed_events = 0
        self.quality_passed = 0
        self.quality_failed = 0
        self.validation_failed = 0
        self.deserialization_failed = 0

        self.total_processing_time_ms = 0.0

        self.recent_failures: list[
            dict[str, Any]
        ] = []

    def record_event(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Record observability for a processed event."""

        self.total_events += 1
        self.processed_events += 1

        quality_status = event.get(
            "data_quality_status",
            "unknown",
        )

        if quality_status == "passed":
            self.quality_passed += 1

        elif quality_status == "failed":
            self.quality_failed += 1
            self._record_failure(event)

        processing_time = event.get(
            "processing_time_ms"
        )

        if processing_time is not None:
            try:
                processing_time_value = float(
                    processing_time
                )

                if math.isfinite(
                    processing_time_value
                ):
                    self.total_processing_time_ms += (
                        processing_time_value
                    )

            except (TypeError, ValueError):
                pass

        result = dict(event)

        result["observability_recorded"] = True

        result["pipeline_status"] = (
            self.get_pipeline_status()
        )

        return result

    def record_validation_failure(
        self,
        event: dict[str, Any] | None = None,
    ) -> None:
        """Record an event rejected by validation."""

        self.total_events += 1
        self.validation_failed += 1

        if event is not None:
            self._record_failure(
                event,
                failure_type="validation",
            )

    def record_deserialization_failure(
        self,
        event: dict[str, Any] | None = None,
    ) -> None:
        """Record an event rejected during deserialization."""

        self.total_events += 1
        self.deserialization_failed += 1

        if event is not None:
            self._record_failure(
                event,
                failure_type="deserialization",
            )

    def _record_failure(
        self,
        event: dict[str, Any],
        failure_type: str = "data_quality",
    ) -> None:
        failure = {
            "event_id": event.get(
                "event_id",
                "UNKNOWN",
            ),
            "failure_type": failure_type,
            "errors": list(
                event.get(
                    "data_quality_errors",
                    [],
                )
            ),
        }

        self.recent_failures.append(
            failure
        )

        if (
            len(self.recent_failures)
            > self.max_recent_failures
        ):
            self.recent_failures.pop(0)

    def quality_pass_rate(self) -> float:
        """Return DQ pass percentage."""

        if self.processed_events == 0:
            return 0.0

        return round(
            (
                self.quality_passed
                / self.processed_events
            )
            * 100,
            2,
        )

    def quality_failure_rate(self) -> float:
        """Return DQ failure percentage."""

        if self.processed_events == 0:
            return 0.0

        return round(
            (
                self.quality_failed
                / self.processed_events
            )
            * 100,
            2,
        )

    def average_processing_time_ms(self) -> float:
        """Return average processing time."""

        if self.processed_events == 0:
            return 0.0

        return round(
            self.total_processing_time_ms
            / self.processed_events,
            2,
        )

    def get_pipeline_status(self) -> str:
        """
        Return current pipeline health.

        Commit 12 only observes the pipeline.
        """

        if self.quality_failed > 0:
            return "degraded"

        return "healthy"

    def snapshot(self) -> dict[str, Any]:
        """Return the current observability state."""

        return {
            "total_events": self.total_events,
            "processed_events": self.processed_events,
            "quality_passed": self.quality_passed,
            "quality_failed": self.quality_failed,
            "validation_failed": (
                self.validation_failed
            ),
            "deserialization_failed": (
                self.deserialization_failed
            ),
            "quality_pass_rate": (
                self.quality_pass_rate()
            ),
            "quality_failure_rate": (
                self.quality_failure_rate()
            ),
            "average_processing_time_ms": (
                self.average_processing_time_ms()
            ),
            "pipeline_status": (
                self.get_pipeline_status()
            ),
            "recent_failures": list(
                self.recent_failures
            ),
        }


class EventEnricher:
    """Add processing metadata to events."""

    def enrich(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        start_time = time.perf_counter()

        enriched = dict(event)

        enriched["processed"] = True

        # Preserve Commit 7 public behavior.
        enriched["processing_stage"] = (
            "flink_stream_processor"
        )

        enriched["processed_timestamp"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        enriched["processing_time_ms"] = round(
            (
                time.perf_counter()
                - start_time
            )
            * 1000,
            3,
        )

        return enriched


class EventSerializer:
    """Serialize processed events to compact JSON."""

    def serialize(
        self,
        event: dict[str, Any],
    ) -> str:
        return json.dumps(
            event,
            separators=(",", ":"),
            default=str,
        )


class DataQualityCheckerAdapter:
    """Adapter for applying DataQualityChecker."""

    def __init__(self) -> None:
        self.checker = DataQualityChecker()

    def process(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        return self.checker.check(event)


class ObservabilityEngineAdapter:
    """Adapter for applying ObservabilityEngine."""

    def __init__(self) -> None:
        self.engine = ObservabilityEngine()

    def process(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        return self.engine.record_event(event)


def _get_flink_adapters() -> tuple[
    Any,
    Any,
    Any,
    Any,
]:
    """
    Lazily import PyFlink adapters.

    This prevents local unit tests from requiring PyFlink.
    """

    from pyflink.common import SimpleStringSchema

    from pyflink.datastream import (
        StreamExecutionEnvironment,
    )

    from pyflink.datastream.connectors.kafka import (
        FlinkKafkaConsumer,
        FlinkKafkaProducer,
    )

    return (
        StreamExecutionEnvironment,
        SimpleStringSchema,
        FlinkKafkaConsumer,
        FlinkKafkaProducer,
    )


def run_checkout_processor() -> None:
    """
    Build and execute the Flink checkout pipeline.

    Pipeline:

        Deserialize
        -> Validate
        -> Business Filter
        -> Normalize
        -> Metrics
        -> Data Quality
        -> Enrich
        -> Observability
        -> Serialize
    """

    (
        StreamExecutionEnvironment,
        SimpleStringSchema,
        FlinkKafkaConsumer,
        FlinkKafkaProducer,
    ) = _get_flink_adapters()

    config = FlinkConfig.from_env()

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    env.set_parallelism(
        config.parallelism
    )

    consumer_properties = {
        "bootstrap.servers": (
            config.kafka_bootstrap_servers
        ),
        "group.id": config.kafka_group_id,
        "auto.offset.reset": "earliest",
    }

    producer_properties = {
        "bootstrap.servers": (
            config.kafka_bootstrap_servers
        ),
    }

    consumer = FlinkKafkaConsumer(
        config.input_topic,
        SimpleStringSchema(),
        consumer_properties,
    )

    producer = FlinkKafkaProducer(
        config.output_topic,
        SimpleStringSchema(),
        producer_properties,
    )

    stream = env.add_source(
        consumer
    ).name("kafka-source")

    deserializer = EventDeserializer()
    validator = EventValidator()
    business_filter = BusinessEventFilter()
    normalizer = EventNormalizer()
    metrics_calculator = EventMetricsCalculator()
    quality_checker = DataQualityChecker()
    enricher = EventEnricher()
    observability = ObservabilityEngine()
    serializer = EventSerializer()

    def process_message(
        message: str,
    ) -> str | None:
        # 1. Deserialize
        event = deserializer.deserialize(
            message
        )

        if event is None:
            observability.record_deserialization_failure()
            return None

        # 2. Validate
        validated = validator.validate(
            event
        )

        if validated is None:
            observability.record_validation_failure(
                event
            )
            return None

        # 3. Business filter
        filtered = business_filter.filter(
            validated
        )

        if filtered is None:
            return None

        # 4. Normalize
        normalized = normalizer.normalize(
            filtered
        )

        # 5. Calculate derived metrics
        calculated = (
            metrics_calculator.calculate(
                normalized
            )
        )

        # 6. Data quality
        quality_checked = (
            quality_checker.check(
                calculated
            )
        )

        # 7. Enrich
        enriched = enricher.enrich(
            quality_checked
        )

        # 8. Observability
        observed = observability.record_event(
            enriched
        )

        # 9. Serialize
        return serializer.serialize(
            observed
        )

    processed_stream = (
        stream
        .map(process_message)
        .filter(
            lambda value: value is not None
        )
        .name(
            "checkout-processing-pipeline"
        )
    )

    processed_stream.add_sink(
        producer
    ).name("kafka-output")

    env.execute(
        config.job_name
    )


if __name__ == "__main__":
    run_checkout_processor()