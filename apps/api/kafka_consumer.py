from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError


class KafkaCheckoutConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        group_id: str = "icestream-api",
        max_events_in_memory: int = 100,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.max_events_in_memory = max_events_in_memory
        self._logger = logging.getLogger("apps.api.kafka_consumer")
        self._lock = threading.RLock()
        self._consumer: KafkaConsumer | None = None
        self._connected = False
        self._running = True
        self._thread: threading.Thread | None = None
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=max_events_in_memory)
        self._stats: dict[str, int] = {
            "total_events": 0,
            "valid_events": 0,
            "malformed_events": 0,
            "consumer_errors": 0,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, name="icestream-kafka-consumer", daemon=True)
        self._thread.start()
        self._logger.info("Kafka consumer thread started for topic %s", self.topic)

    def stop(self) -> None:
        self._running = False
        self._logger.info("Stopping Kafka consumer")
        self._close_consumer()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    def get_recent_events(self, limit: int = 10) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self._recent_events)
        if limit <= 0:
            return []
        return items[-limit:]

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            for event in reversed(self._recent_events):
                if event.get("event_id") == event_id:
                    return dict(event)
        return None

    def get_statistics(self) -> dict[str, int]:
        with self._lock:
            return {
                "total_events": self._stats["total_events"],
                "valid_events": self._stats["valid_events"],
                "malformed_events": self._stats["malformed_events"],
                "consumer_errors": self._stats["consumer_errors"],
                "events_in_memory": len(self._recent_events),
            }

    def get_health(self) -> dict[str, Any]:
        connected = self.is_connected()
        return {
            "status": "healthy" if connected else "degraded",
            "kafka_connected": connected,
        }

    def _close_consumer(self) -> None:
        consumer = self._consumer
        if consumer is None:
            self._connected = False
            return
        try:
            consumer.close()
        except Exception as exc:  # pragma: no cover - defensive path
            self._logger.warning("Kafka consumer close warning: %s", exc)
        finally:
            self._consumer = None
            self._connected = False
            self._logger.info("Kafka consumer closed")

    def _connect(self) -> bool:
        try:
            server_list = [item.strip() for item in self.bootstrap_servers.split(",") if item.strip()]
            self._consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=server_list,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=None,
            )
            self._connected = True
            self._logger.info("Kafka connection successful for brokers=%s topic=%s", self.bootstrap_servers, self.topic)
            return True
        except KafkaError as exc:
            self._logger.warning("Kafka connection failed for brokers=%s topic=%s: %s", self.bootstrap_servers, self.topic, exc)
            self._connected = False
            self._stats["consumer_errors"] += 1
            self._consumer = None
            return False
        except Exception as exc:  # pragma: no cover - defensive path
            self._logger.warning("Kafka connection unexpected error for brokers=%s topic=%s: %s", self.bootstrap_servers, self.topic, exc)
            self._connected = False
            self._stats["consumer_errors"] += 1
            self._consumer = None
            return False

    def _consume_loop(self) -> None:
        while self._running:
            if self._consumer is None:
                if not self._connect():
                    time.sleep(5)
                    continue

            try:
                for message in self._consumer.poll(timeout_ms=500, max_records=25).values():
                    for record in message:
                        self._process_message(record.value)
            except KafkaError as exc:
                self._stats["consumer_errors"] += 1
                self._logger.warning("Kafka consumer error: %s", exc)
                self._close_consumer()
                time.sleep(5)
            except Exception as exc:  # pragma: no cover - defensive path
                self._stats["consumer_errors"] += 1
                self._logger.exception("Unexpected Kafka consumer failure: %s", exc)
                self._close_consumer()
                time.sleep(5)

    def _process_message(self, raw_message: Any) -> None:
        if raw_message is None:
            with self._lock:
                self._stats["total_events"] += 1
                self._stats["malformed_events"] += 1
            self._logger.warning("Malformed Kafka message received: empty payload")
            return

        try:
            if isinstance(raw_message, bytes):
                payload = raw_message.decode("utf-8")
            else:
                payload = str(raw_message)
            event = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            with self._lock:
                self._stats["total_events"] += 1
                self._stats["malformed_events"] += 1
            self._logger.warning("Malformed Kafka event received: %s", exc)
            return

        if not isinstance(event, dict):
            with self._lock:
                self._stats["total_events"] += 1
                self._stats["malformed_events"] += 1
            self._logger.warning("Kafka event payload is not a JSON object: %s", type(event).__name__)
            return

        with self._lock:
            self._stats["total_events"] += 1
            self._stats["valid_events"] += 1
            self._recent_events.append(dict(event))

        self._logger.info("Kafka event received: event_id=%s", event.get("event_id", "unknown"))
