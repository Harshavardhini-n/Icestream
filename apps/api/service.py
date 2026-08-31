from __future__ import annotations

from typing import Any

from apps.api.config import get_settings
from apps.api.kafka_consumer import KafkaCheckoutConsumer


class ApiService:
    def __init__(self, consumer: KafkaCheckoutConsumer | None = None) -> None:
        settings = get_settings()
        self.consumer = consumer or KafkaCheckoutConsumer(
            settings.kafka_bootstrap_servers,
            settings.kafka_checkout_topic,
            max_events_in_memory=settings.max_events_in_memory,
        )

    def get_recent_events(self, limit: int = 10) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        return self.consumer.get_recent_events(limit)

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        return self.consumer.get_event(event_id)

    def get_statistics(self) -> dict[str, int]:
        return self.consumer.get_statistics()

    def get_health(self) -> dict[str, Any]:
        return self.consumer.get_health()
