from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_checkout_topic: str = "checkout-events"
    events_per_second: int = 100
    max_events: int = 0
    null_tax_rate: float = 0.0
    schema_drift_rate: float = 0.0
    malformed_event_rate: float = 0.0
    random_seed: int | None = None

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

    @staticmethod
    def _parse_float(value: str, default: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        normalized = parsed if parsed <= 1.0 else parsed / 100.0
        return max(0.0, min(1.0, normalized))

    @staticmethod
    def _parse_seed(value: str | int | float | None) -> int | None:
        if value is None:
            return None
        text = str(value).strip()
        if text in {"", "None", "null"}:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    @classmethod
    def from_mapping(cls, values: Mapping[str, str | int | float]) -> "Settings":
        data = {str(key): str(value) for key, value in values.items()}
        return cls(
            kafka_bootstrap_servers=cls._get_env_value(data, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_checkout_topic=cls._get_env_value(data, "KAFKA_CHECKOUT_TOPIC", "checkout-events"),
            events_per_second=cls._parse_int(cls._get_env_value(data, "EVENTS_PER_SECOND", "100"), 100),
            max_events=cls._parse_int(cls._get_env_value(data, "MAX_EVENTS", "0"), 0),
            null_tax_rate=cls._parse_float(cls._get_env_value(data, "NULL_TAX_RATE", "0.0"), 0.0),
            schema_drift_rate=cls._parse_float(cls._get_env_value(data, "SCHEMA_DRIFT_RATE", "0.0"), 0.0),
            malformed_event_rate=cls._parse_float(cls._get_env_value(data, "MALFORMED_EVENT_RATE", "0.0"), 0.0),
            random_seed=cls._parse_seed(data.get("RANDOM_SEED")),
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        source = os.environ if env is None else env
        values = {
            "KAFKA_BOOTSTRAP_SERVERS": cls._get_env_value(source, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "KAFKA_CHECKOUT_TOPIC": cls._get_env_value(source, "KAFKA_CHECKOUT_TOPIC", "checkout-events"),
            "EVENTS_PER_SECOND": cls._get_env_value(source, "EVENTS_PER_SECOND", "100"),
            "MAX_EVENTS": cls._get_env_value(source, "MAX_EVENTS", "0"),
            "NULL_TAX_RATE": cls._get_env_value(source, "NULL_TAX_RATE", "0.0"),
            "SCHEMA_DRIFT_RATE": cls._get_env_value(source, "SCHEMA_DRIFT_RATE", "0.0"),
            "MALFORMED_EVENT_RATE": cls._get_env_value(source, "MALFORMED_EVENT_RATE", "0.0"),
            "RANDOM_SEED": cls._get_env_value(source, "RANDOM_SEED", ""),
        }
        return cls(
            kafka_bootstrap_servers=values["KAFKA_BOOTSTRAP_SERVERS"],
            kafka_checkout_topic=values["KAFKA_CHECKOUT_TOPIC"],
            events_per_second=cls._parse_int(values["EVENTS_PER_SECOND"], 100),
            max_events=cls._parse_int(values["MAX_EVENTS"], 0),
            null_tax_rate=cls._parse_float(values["NULL_TAX_RATE"], 0.0),
            schema_drift_rate=cls._parse_float(values["SCHEMA_DRIFT_RATE"], 0.0),
            malformed_event_rate=cls._parse_float(values["MALFORMED_EVENT_RATE"], 0.0),
            random_seed=cls._parse_seed(values["RANDOM_SEED"]),
        )
