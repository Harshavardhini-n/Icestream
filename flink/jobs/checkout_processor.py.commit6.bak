"""Flink job for processing checkout events from Kafka.

This job:
1. Consumes raw checkout events from Kafka (checkout-events topic)
2. Deserializes JSON payloads
3. Validates event structure against expected schema
4. Handles malformed events gracefully without crashing
5. Preserves valid events with processing metadata
6. Produces processed events to Kafka (processed-checkout-events topic)
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from flink.config import FlinkConfig


# Set up logging
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
    "tax_amount",  # Can be null, but field should exist
    "total_amount",
    "currency",
    "payment_method",
    "event_type",
}


# ============================================================================
# Processing Functions (Framework-Independent)
# ============================================================================


class EventDeserializer:
    """Deserialize JSON string into event dictionary.
    
    If JSON is malformed, logs the error and returns None.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.malformed_count = 0

    def deserialize(self, message: str) -> dict[str, Any] | None:
        """Deserialize JSON message to dict or None if malformed."""
        try:
            event = json.loads(message)
            return event
        except (json.JSONDecodeError, ValueError) as e:
            self.malformed_count += 1
            self.logger.warning(
                f"Malformed JSON (count={self.malformed_count}): {str(e)[:100]} | "
                f"message={message[:100]}"
            )
            return None


class EventValidator:
    """Validate event structure and schema.
    
    Checks for required fields. Tolerant of extra fields and null values
    in fields that allow them. Logs validation issues but doesn't crash.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_error_count = 0
        self.validation_success_count = 0

    def validate(self, event: dict[str, Any] | None) -> dict[str, Any] | None:
        """Validate event structure or return None if invalid."""
        if event is None:
            return None

        try:
            # Check for required fields
            missing_fields = REQUIRED_FIELDS - set(event.keys())
            if missing_fields:
                self.validation_error_count += 1
                self.logger.warning(
                    f"Missing required fields: {missing_fields} | "
                    f"event_id={event.get('event_id', 'UNKNOWN')}"
                )
                return None

            # Basic type validation (lenient approach for now)
            # We validate only critical numeric fields
            try:
                _ = float(event.get("quantity", 0))
                _ = float(event.get("unit_price", 0))
                _ = float(event.get("total_amount", 0))
            except (TypeError, ValueError):
                self.validation_error_count += 1
                self.logger.warning(
                    f"Invalid numeric field type | event_id={event.get('event_id')}"
                )
                return None

            self.validation_success_count += 1
            return event

        except Exception as e:
            self.validation_error_count += 1
            self.logger.error(f"Validation exception: {e}", exc_info=True)
            return None


class EventEnricher:
    """Add processing metadata to validated event.
    
    Preserves all original fields and adds:
    - processed: true (boolean flag indicating successful processing)
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def enrich(self, event: dict[str, Any]) -> dict[str, Any]:
        """Add processing metadata to event."""
        # Add processing flag
        event["processed"] = True
        return event


class EventSerializer:
    """Serialize event dictionary back to JSON string."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def serialize(self, event: dict[str, Any]) -> str:
        """Serialize event dict to JSON string."""
        try:
            return json.dumps(event, separators=(",", ":"))
        except (TypeError, ValueError) as e:
            self.logger.error(f"Serialization error: {e} | event={event}", exc_info=True)
            # Return a minimal error record instead of crashing
            fallback = json.dumps({
                "error": "serialization_failed",
                "event_id": event.get("event_id", "UNKNOWN"),
            })
            return fallback


# ============================================================================
# Flink-Specific Adapters (only loaded when using Flink)
# ============================================================================

def _get_flink_adapters():
    """Dynamically import and wrap functions for Flink framework.
    
    This is only called when running within Flink.
    Returns Flink MapFunction and FilterFunction wrappers.
    """
    try:
        from pyflink.datastream.functions import MapFunction, FilterFunction
        
        class EventDeserializerAdapter(MapFunction):
            def __init__(self):
                self.deserializer = EventDeserializer()
            
            def map(self, message: str) -> dict[str, Any] | None:
                return self.deserializer.deserialize(message)
        
        class EventValidatorAdapter(MapFunction):
            def __init__(self):
                self.validator = EventValidator()
            
            def map(self, event: dict[str, Any] | None) -> dict[str, Any] | None:
                return self.validator.validate(event)
        
        class MalformedEventFilterAdapter(FilterFunction):
            def filter(self, event: dict[str, Any] | None) -> bool:
                return event is not None
        
        class EventEnricherAdapter(MapFunction):
            def __init__(self):
                self.enricher = EventEnricher()
            
            def map(self, event: dict[str, Any]) -> dict[str, Any]:
                return self.enricher.enrich(event)
        
        class EventSerializerAdapter(MapFunction):
            def __init__(self):
                self.serializer = EventSerializer()
            
            def map(self, event: dict[str, Any]) -> str:
                return self.serializer.serialize(event)
        
        return {
            'deserializer': EventDeserializerAdapter,
            'validator': EventValidatorAdapter,
            'filter': MalformedEventFilterAdapter,
            'enricher': EventEnricherAdapter,
            'serializer': EventSerializerAdapter,
        }
    except ImportError:
        return None


# ============================================================================
# Main Job
# ============================================================================


def run_checkout_processor(config: FlinkConfig) -> int:
    """Run the Flink checkout processor job.
    
    Args:
        config: FlinkConfig instance with Kafka and Flink settings
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Check if PyFlink is available
        adapters = _get_flink_adapters()
        if not adapters:
            logger.error(
                "PyFlink is not installed. Install it with: pip install -r flink/requirements.txt"
            )
            return 1
        
        # Import Flink only if available
        from pyflink.common import SimpleStringSchema
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
        
        logger.info(f"Starting {config.flink_job_name}")
        logger.info(f"Kafka bootstrap servers: {config.kafka_bootstrap_servers}")
        logger.info(f"Input topic: {config.kafka_input_topic}")
        logger.info(f"Output topic: {config.kafka_output_topic}")
        logger.info(f"Parallelism: {config.flink_parallelism}")

        # Create Flink execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(config.flink_parallelism)

        # Create Kafka consumer
        # Note: Using kafka_bootstrap_servers directly - in Docker, this should be
        # set to kafka:9092 (internal Docker network hostname)
        kafka_consumer = FlinkKafkaConsumer(
            topics=config.kafka_input_topic,
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.kafka_consumer_group,
                "auto.offset.reset": "earliest",
            },
        )

        # Create Kafka producer
        kafka_producer = FlinkKafkaProducer(
            topic=config.kafka_output_topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                "bootstrap.servers": config.kafka_bootstrap_servers,
            },
        )

        # Build the processing pipeline
        kafka_stream = env.add_source(kafka_consumer)

        # Process events:
        # 1. Deserialize JSON
        # 2. Validate schema
        # 3. Filter malformed events
        # 4. Enrich with metadata
        # 5. Serialize back to JSON
        # 6. Send to output Kafka topic
        processed_stream = (
            kafka_stream
            .map(adapters['deserializer']())
            .map(adapters['validator']())
            .filter(adapters['filter']())
            .map(adapters['enricher']())
            .map(adapters['serializer']())
        )

        processed_stream.add_sink(kafka_producer)

        logger.info(f"Executing {config.flink_job_name}...")
        env.execute(config.flink_job_name)

        return 0

    except ImportError as e:
        logger.error(f"Required module not found: {e}", exc_info=True)
        logger.error("Ensure PyFlink is installed: pip install -r flink/requirements.txt")
        return 1
    except Exception as e:
        logger.error(f"Job failed with exception: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    config = FlinkConfig.from_env()
    exit_code = run_checkout_processor(config)
    sys.exit(exit_code)
