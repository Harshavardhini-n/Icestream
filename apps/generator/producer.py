from __future__ import annotations

import json
from typing import Any

from .config import Settings

try:
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover - only for environments without the package installed.
    KafkaProducer = None  # type: ignore[assignment]


class KafkaCheckoutProducer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.bootstrap_servers = [
            item.strip() for item in settings.kafka_bootstrap_servers.split(",") if item.strip()
        ]
        self.topic = settings.kafka_checkout_topic
        self._producer: Any | None = None
        self._producer = self._build_producer()

    def _build_producer(self):
        if KafkaProducer is None:
            raise RuntimeError(
                "The kafka-python dependency is not installed. Run 'pip install -r apps/generator/requirements.txt'."
            )

        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            retries=3,
            linger_ms=10,
            compression_type="gzip",
            value_serializer=lambda value: value if isinstance(value, bytes) else json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            api_version=(2, 8, 1),
        )

    def publish(self, event: dict[str, Any] | str, *, key: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer was not initialized.")

        if isinstance(event, str):
            payload = event.encode("utf-8")
            msg_key = key or "malformed"
            self._producer.send(self.topic, key=msg_key, value=payload)
            self._producer.flush()
            return

        msg_key = key or event.get("event_id", "unknown")
        self._producer.send(self.topic, key=msg_key, value=event)
        self._producer.flush()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    def __enter__(self) -> "KafkaCheckoutProducer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
