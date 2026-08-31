from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_checkout_topic: str = "checkout-events"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    max_events_in_memory: int = 100

    @staticmethod
    def _get_env_value(mapping: Mapping[str, str], key: str, default: str) -> str:
        value = mapping.get(key)
        if value is None or value == "":
            return default
        return str(value)

    @staticmethod
    def _parse_int(value: str, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(0, parsed)

    @classmethod
    def from_mapping(cls, values: Mapping[str, str | int | float]) -> "Settings":
        data = {str(key): str(value) for key, value in values.items()}
        return cls(
            kafka_bootstrap_servers=cls._get_env_value(data, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_checkout_topic=cls._get_env_value(data, "KAFKA_CHECKOUT_TOPIC", "checkout-events"),
            api_host=cls._get_env_value(data, "API_HOST", "0.0.0.0"),
            api_port=cls._parse_int(cls._get_env_value(data, "API_PORT", "8000"), 8000),
            max_events_in_memory=cls._parse_int(cls._get_env_value(data, "MAX_EVENTS_IN_MEMORY", "100"), 100),
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        source = os.environ if env is None else env
        values = {
            "KAFKA_BOOTSTRAP_SERVERS": cls._get_env_value(source, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "KAFKA_CHECKOUT_TOPIC": cls._get_env_value(source, "KAFKA_CHECKOUT_TOPIC", "checkout-events"),
            "API_HOST": cls._get_env_value(source, "API_HOST", "0.0.0.0"),
            "API_PORT": cls._get_env_value(source, "API_PORT", "8000"),
            "MAX_EVENTS_IN_MEMORY": cls._get_env_value(source, "MAX_EVENTS_IN_MEMORY", "100"),
        }
        return cls(
            kafka_bootstrap_servers=values["KAFKA_BOOTSTRAP_SERVERS"],
            kafka_checkout_topic=values["KAFKA_CHECKOUT_TOPIC"],
            api_host=values["API_HOST"],
            api_port=cls._parse_int(values["API_PORT"], 8000),
            max_events_in_memory=cls._parse_int(values["MAX_EVENTS_IN_MEMORY"], 100),
        )


def get_settings() -> Settings:
    return Settings.from_env()
