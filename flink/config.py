"""Flink job configuration for checkout event processing."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class FlinkConfig:
    """Configuration for Flink checkout processor job.
    
    Environment Variables:
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker address(es). Defaults to localhost:9092.
    - KAFKA_INPUT_TOPIC: Input topic with raw checkout events. Defaults to checkout-events.
    - KAFKA_OUTPUT_TOPIC: Output topic for processed events. Defaults to processed-checkout-events.
    - KAFKA_CONSUMER_GROUP: Kafka consumer group. Defaults to icestream-flink-processor.
    - FLINK_JOB_NAME: Name of the Flink job. Defaults to icestream-checkout-processor.
    - FLINK_PARALLELISM: Parallelism level. Defaults to 2.
    """
    
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "checkout-events"
    kafka_output_topic: str = "processed-checkout-events"
    kafka_consumer_group: str = "icestream-flink-processor"
    flink_job_name: str = "icestream-checkout-processor"
    flink_parallelism: int = 2
    
    @staticmethod
    def _get_env_value(mapping: Mapping[str, str], key: str, default: str) -> str:
        """Get environment variable with default fallback."""
        value = mapping.get(key)
        if value is None or value == "":
            return default
        return str(value)
    
    @staticmethod
    def _parse_int(value: str, default: int) -> int:
        """Parse integer with default fallback."""
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(0, parsed)
    
    @classmethod
    def from_mapping(cls, values: Mapping[str, str | int]) -> FlinkConfig:
        """Create config from a key-value mapping."""
        data = {str(key): str(value) for key, value in values.items()}
        return cls(
            kafka_bootstrap_servers=cls._get_env_value(
                data, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            kafka_input_topic=cls._get_env_value(
                data, "KAFKA_INPUT_TOPIC", "checkout-events"
            ),
            kafka_output_topic=cls._get_env_value(
                data, "KAFKA_OUTPUT_TOPIC", "processed-checkout-events"
            ),
            kafka_consumer_group=cls._get_env_value(
                data, "KAFKA_CONSUMER_GROUP", "icestream-flink-processor"
            ),
            flink_job_name=cls._get_env_value(
                data, "FLINK_JOB_NAME", "icestream-checkout-processor"
            ),
            flink_parallelism=cls._parse_int(
                cls._get_env_value(data, "FLINK_PARALLELISM", "2"), 2
            ),
        )
    
    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> FlinkConfig:
        """Create config from environment variables."""
        source = os.environ if env is None else env
        values = {
            "KAFKA_BOOTSTRAP_SERVERS": cls._get_env_value(
                source, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "KAFKA_INPUT_TOPIC": cls._get_env_value(
                source, "KAFKA_INPUT_TOPIC", "checkout-events"
            ),
            "KAFKA_OUTPUT_TOPIC": cls._get_env_value(
                source, "KAFKA_OUTPUT_TOPIC", "processed-checkout-events"
            ),
            "KAFKA_CONSUMER_GROUP": cls._get_env_value(
                source, "KAFKA_CONSUMER_GROUP", "icestream-flink-processor"
            ),
            "FLINK_JOB_NAME": cls._get_env_value(
                source, "FLINK_JOB_NAME", "icestream-checkout-processor"
            ),
            "FLINK_PARALLELISM": cls._get_env_value(
                source, "FLINK_PARALLELISM", "2"
            ),
        }
        return cls.from_mapping(values)
