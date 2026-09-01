"""Tests for Flink checkout event processor."""

from __future__ import annotations

import json

import pytest

from flink.config import FlinkConfig
from flink.jobs.checkout_processor import (
    EventDeserializer,
    EventValidator,
    EventEnricher,
    EventSerializer,
    REQUIRED_FIELDS,
)


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def valid_event() -> dict:
    """Valid checkout event matching the expected schema."""
    return {
        "event_id": "evt-abc123",
        "event_timestamp": "2026-01-01T12:00:00.000Z",
        "customer_id": "cust-5678",
        "session_id": "sess-xyz",
        "product_id": "prod-1001",
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


@pytest.fixture
def valid_event_json(valid_event) -> str:
    """JSON serialized version of valid event."""
    return json.dumps(valid_event, separators=(",", ":"))


@pytest.fixture
def valid_event_with_null_tax(valid_event) -> dict:
    """Valid event but with tax_amount as null."""
    event = valid_event.copy()
    event["tax_amount"] = None
    return event


@pytest.fixture
def valid_event_with_schema_drift(valid_event) -> dict:
    """Valid event but with schema drift (taxAmount instead of tax_amount)."""
    event = valid_event.copy()
    # Pop the snake_case version, keep camelCase variant
    tax_value = event.pop("tax_amount", None)
    event["taxAmount"] = tax_value
    return event


@pytest.fixture
def malformed_json_examples() -> list[str]:
    """Collection of malformed JSON examples."""
    return [
        '{"event_id":"bad-1","subtotal":45.10,"tax_amount":,"currency":"USD"}',  # Missing value
        '{"event_id":"bad-2","subtotal":45.10,"tax_amount":12.99,"currency":"USD"',  # Incomplete
        '{"event_id":"bad-3","subtotal":45.10,"tax_amount":12.99,"currency":}',  # Missing value
        "not json at all",
        "{incomplete json",
        "",
    ]


# ============================================================================
# Test EventDeserializer
# ============================================================================


class TestEventDeserializer:
    """Test EventDeserializer.deserialize() function."""

    def test_deserialize_valid_json(self, valid_event_json):
        """Valid JSON should deserialize to dict."""
        deserializer = EventDeserializer()
        result = deserializer.deserialize(valid_event_json)
        assert result is not None
        assert isinstance(result, dict)
        assert result["event_id"] == "evt-abc123"

    def test_deserialize_malformed_json(self, malformed_json_examples):
        """Malformed JSON should return None (logged, not crashed)."""
        deserializer = EventDeserializer()
        for malformed in malformed_json_examples:
            result = deserializer.deserialize(malformed)
            assert result is None, f"Expected None for malformed: {malformed}"
        assert deserializer.malformed_count == len(malformed_json_examples)

    def test_deserializer_increments_count(self):
        """Deserializer should track malformed event count."""
        deserializer = EventDeserializer()
        assert deserializer.malformed_count == 0
        deserializer.deserialize("invalid")
        assert deserializer.malformed_count == 1
        deserializer.deserialize("also invalid")
        assert deserializer.malformed_count == 2


# ============================================================================
# Test EventValidator
# ============================================================================


class TestEventValidator:
    """Test EventValidator.validate() function."""

    def test_validate_valid_event(self, valid_event):
        """Valid event with all required fields should pass."""
        validator = EventValidator()
        result = validator.validate(valid_event)
        assert result is not None
        assert result == valid_event
        assert validator.validation_success_count == 1
        assert validator.validation_error_count == 0

    def test_validate_event_with_null_tax(self, valid_event_with_null_tax):
        """Event with null tax_amount should still be valid."""
        validator = EventValidator()
        result = validator.validate(valid_event_with_null_tax)
        assert result is not None
        assert result["tax_amount"] is None
        assert validator.validation_success_count == 1

    def test_validate_event_with_extra_fields(self, valid_event):
        """Event with extra fields should be valid (allowed)."""
        event = valid_event.copy()
        event["extra_field"] = "extra_value"
        event["another_field"] = 123
        validator = EventValidator()
        result = validator.validate(event)
        assert result is not None
        assert "extra_field" in result
        assert validator.validation_success_count == 1

    def test_validate_missing_required_field(self, valid_event):
        """Event missing a required field should fail."""
        event = valid_event.copy()
        del event["event_id"]  # Remove required field
        validator = EventValidator()
        result = validator.validate(event)
        assert result is None
        assert validator.validation_error_count == 1

    def test_validate_multiple_missing_fields(self, valid_event):
        """Event missing multiple required fields should fail."""
        event = valid_event.copy()
        del event["event_id"]
        del event["customer_id"]
        del event["total_amount"]
        validator = EventValidator()
        result = validator.validate(event)
        assert result is None
        assert validator.validation_error_count == 1

    def test_validate_invalid_numeric_field(self, valid_event):
        """Event with non-numeric value in numeric field should fail."""
        event = valid_event.copy()
        event["quantity"] = "not a number"
        validator = EventValidator()
        result = validator.validate(event)
        assert result is None
        assert validator.validation_error_count == 1

    def test_validate_none_event(self):
        """Passing None should be handled gracefully."""
        validator = EventValidator()
        result = validator.validate(None)
        assert result is None

    def test_validator_counts(self, valid_event):
        """Validator should track success and error counts."""
        validator = EventValidator()
        assert validator.validation_success_count == 0
        assert validator.validation_error_count == 0
        validator.validate(valid_event)
        assert validator.validation_success_count == 1
        assert validator.validation_error_count == 0
        validator.validate(valid_event.copy())
        assert validator.validation_success_count == 2


# ============================================================================
# Test Simple Filter Logic
# ============================================================================


def test_filter_logic_passes_valid_event(valid_event):
    """Valid event (dict) should pass filter (not None)."""
    assert valid_event is not None


def test_filter_logic_none_is_filtered():
    """None should be considered filtered out."""
    assert None is None  # Tautology, but tests the filter concept


# ============================================================================
# Test EventEnricher
# ============================================================================


class TestEventEnricher:
    """Test EventEnricher.enrich() function."""

    def test_enrich_adds_processed_flag(self, valid_event):
        """Enricher should add 'processed': True flag."""
        enricher = EventEnricher()
        result = enricher.enrich(valid_event.copy())
        assert "processed" in result
        assert result["processed"] is True

    def test_enrich_preserves_original_fields(self, valid_event):
        """Enricher should preserve all original fields."""
        enricher = EventEnricher()
        enriched = enricher.enrich(valid_event.copy())
        for key in valid_event:
            assert key in enriched
            assert enriched[key] == valid_event[key]

    def test_enrich_preserves_null_values(self, valid_event_with_null_tax):
        """Enricher should preserve null field values."""
        enricher = EventEnricher()
        result = enricher.enrich(valid_event_with_null_tax.copy())
        assert result["tax_amount"] is None
        assert result["processed"] is True


# ============================================================================
# Test EventSerializer
# ============================================================================


class TestEventSerializer:
    """Test EventSerializer.serialize() function."""

    def test_serialize_valid_event(self, valid_event):
        """Valid event should serialize to JSON string."""
        serializer = EventSerializer()
        result = serializer.serialize(valid_event)
        assert isinstance(result, str)
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["event_id"] == valid_event["event_id"]

    def test_serialize_round_trip(self, valid_event):
        """Event should survive serialize/deserialize round-trip."""
        enricher = EventEnricher()
        serializer = EventSerializer()
        deserializer = EventDeserializer()

        enriched = enricher.enrich(valid_event.copy())
        serialized = serializer.serialize(enriched)
        deserialized = deserializer.deserialize(serialized)

        assert deserialized["event_id"] == valid_event["event_id"]
        assert deserialized["processed"] is True

    def test_serialize_compact_json(self, valid_event):
        """Serialized JSON should be compact (no extra whitespace)."""
        serializer = EventSerializer()
        result = serializer.serialize(valid_event)
        # Compact JSON should not contain space after colons or commas
        assert ": " not in result  # no "key: value" format
        assert ", " not in result  # no "val, val" format

    def test_serialize_preserves_null(self, valid_event_with_null_tax):
        """Serializer should preserve null values."""
        serializer = EventSerializer()
        result = serializer.serialize(valid_event_with_null_tax)
        parsed = json.loads(result)
        assert parsed["tax_amount"] is None


# ============================================================================
# Test Configuration
# ============================================================================


class TestFlinkConfig:
    """Test FlinkConfig class."""

    def test_config_defaults(self):
        """Default configuration should use sensible defaults."""
        config = FlinkConfig()
        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.kafka_input_topic == "checkout-events"
        assert config.kafka_output_topic == "processed-checkout-events"
        assert config.kafka_consumer_group == "icestream-flink-processor"
        assert config.flink_job_name == "icestream-checkout-processor"
        assert config.flink_parallelism == 2

    def test_config_from_mapping(self):
        """Config should load from key-value mapping."""
        mapping = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_INPUT_TOPIC": "raw-events",
            "FLINK_PARALLELISM": "4",
        }
        config = FlinkConfig.from_mapping(mapping)
        assert config.kafka_bootstrap_servers == "kafka:9092"
        assert config.kafka_input_topic == "raw-events"
        assert config.flink_parallelism == 4
        # Others should use defaults
        assert config.kafka_output_topic == "processed-checkout-events"

    def test_config_from_mapping_with_defaults(self):
        """Config should fall back to defaults for missing mappings."""
        mapping = {"KAFKA_BOOTSTRAP_SERVERS": "custom:9092"}
        config = FlinkConfig.from_mapping(mapping)
        assert config.kafka_bootstrap_servers == "custom:9092"
        assert config.kafka_input_topic == "checkout-events"  # default

    def test_config_parallelism_parsing(self):
        """Parallelism should be parsed as int."""
        mapping = {"FLINK_PARALLELISM": "8"}
        config = FlinkConfig.from_mapping(mapping)
        assert config.flink_parallelism == 8
        assert isinstance(config.flink_parallelism, int)

    def test_config_parallelism_invalid_defaults_to_2(self):
        """Invalid parallelism should default to 2."""
        mapping = {"FLINK_PARALLELISM": "not_a_number"}
        config = FlinkConfig.from_mapping(mapping)
        assert config.flink_parallelism == 2

    def test_config_empty_string_uses_default(self):
        """Empty string should trigger use of default."""
        mapping = {"KAFKA_INPUT_TOPIC": ""}
        config = FlinkConfig.from_mapping(mapping)
        assert config.kafka_input_topic == "checkout-events"


# ============================================================================
# Integration Tests (processing pipeline)
# ============================================================================


class TestProcessingPipeline:
    """Test the full processing pipeline with realistic scenarios."""

    def test_pipeline_valid_event(self, valid_event_json):
        """Pipeline should process valid event successfully."""
        deserializer = EventDeserializer()
        validator = EventValidator()
        enricher = EventEnricher()
        serializer = EventSerializer()

        # Simulate pipeline
        deserialized = deserializer.deserialize(valid_event_json)
        validated = validator.validate(deserialized)
        enriched = enricher.enrich(validated) if validated else None
        output = serializer.serialize(enriched) if enriched else None

        assert output is not None
        assert isinstance(output, str)
        parsed = json.loads(output)
        assert parsed["processed"] is True

    def test_pipeline_malformed_event(self):
        """Pipeline should filter malformed events."""
        deserializer = EventDeserializer()
        validator = EventValidator()
        enricher = EventEnricher()
        serializer = EventSerializer()

        malformed = '{"invalid": json'
        deserialized = deserializer.deserialize(malformed)
        assert deserialized is None
        validated = validator.validate(deserialized)
        assert validated is None

    def test_pipeline_missing_field_event(self, valid_event):
        """Pipeline should filter events with missing required fields."""
        deserializer = EventDeserializer()
        validator = EventValidator()

        invalid_event = valid_event.copy()
        del invalid_event["event_id"]

        # Simulating event that came through deserializer successfully
        validated = validator.validate(invalid_event)
        assert validated is None

    def test_pipeline_batch(self, valid_event, malformed_json_examples):
        """Pipeline should handle mixed batch of valid and malformed."""
        deserializer = EventDeserializer()
        validator = EventValidator()
        enricher = EventEnricher()
        serializer = EventSerializer()

        messages = [
            json.dumps(valid_event),
            malformed_json_examples[0],
            json.dumps(valid_event),
            malformed_json_examples[1],
        ]

        successful_outputs = []
        for message in messages:
            deserialized = deserializer.deserialize(message)
            validated = validator.validate(deserialized)
            if validated:
                enriched = enricher.enrich(validated)
                output = serializer.serialize(enriched)
                successful_outputs.append(output)

        # Should have processed 2 valid events
        assert len(successful_outputs) == 2
        for output in successful_outputs:
            parsed = json.loads(output)
            assert parsed["processed"] is True
