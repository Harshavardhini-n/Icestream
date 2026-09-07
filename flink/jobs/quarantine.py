from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class QuarantineConfig:
    """Configuration for failed-event quarantine."""

    source_topic: str = "checkout-events"
    dlq_topic: str = "checkout-dlq"


@dataclass
class QuarantinedEvent:
    """Represents an event removed from the healthy processing path."""

    event: Dict[str, Any]
    reason: str
    errors: List[str] = field(default_factory=list)
    quarantined_at: str = ""
    source_topic: str = ""
    dlq_topic: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event,
            "reason": self.reason,
            "errors": self.errors,
            "quarantined_at": self.quarantined_at,
            "source_topic": self.source_topic,
            "dlq_topic": self.dlq_topic,
        }


class EventQuarantine:
    """
    Routes failed events into a quarantine representation.

    The quarantine layer does not attempt to repair events.
    It preserves the original event and records why it was rejected.
    """

    def __init__(
        self,
        config: Optional[QuarantineConfig] = None,
    ) -> None:
        self.config = config or QuarantineConfig()

        self.quarantined_count = 0
        self.data_quality_failures = 0
        self.validation_failures = 0
        self.processing_failures = 0

        self._recent_events: List[Dict[str, Any]] = []

    def quarantine(
        self,
        event: Optional[Dict[str, Any]],
        reason: str,
        errors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a quarantine record while preserving the original event.
        """

        if event is None:
            event = {}

        if not reason:
            reason = "unknown_failure"

        error_list = list(errors or [])

        quarantined = QuarantinedEvent(
            event=dict(event),
            reason=reason,
            errors=error_list,
            quarantined_at=datetime.now(timezone.utc).isoformat(),
            source_topic=self.config.source_topic,
            dlq_topic=self.config.dlq_topic,
        )

        result = quarantined.to_dict()

        self.quarantined_count += 1

        if reason == "data_quality_failure":
            self.data_quality_failures += 1
        elif reason == "validation_failure":
            self.validation_failures += 1
        elif reason == "processing_failure":
            self.processing_failures += 1

        self._recent_events.append(result)

        # Keep only a bounded recent history.
        if len(self._recent_events) > 100:
            self._recent_events.pop(0)

        return result

    def quarantine_data_quality_failure(
        self,
        event: Optional[Dict[str, Any]],
        errors: List[str],
    ) -> Dict[str, Any]:
        return self.quarantine(
            event=event,
            reason="data_quality_failure",
            errors=errors,
        )

    def quarantine_validation_failure(
        self,
        event: Optional[Dict[str, Any]],
        errors: List[str],
    ) -> Dict[str, Any]:
        return self.quarantine(
            event=event,
            reason="validation_failure",
            errors=errors,
        )

    def quarantine_processing_failure(
        self,
        event: Optional[Dict[str, Any]],
        errors: List[str],
    ) -> Dict[str, Any]:
        return self.quarantine(
            event=event,
            reason="processing_failure",
            errors=errors,
        )

    def recent_events(self) -> List[Dict[str, Any]]:
        return list(self._recent_events)

    def status(self) -> Dict[str, Any]:
        return {
            "quarantined_count": self.quarantined_count,
            "data_quality_failures": self.data_quality_failures,
            "validation_failures": self.validation_failures,
            "processing_failures": self.processing_failures,
            "recent_events": len(self._recent_events),
            "source_topic": self.config.source_topic,
            "dlq_topic": self.config.dlq_topic,
        }

    def clear_history(self) -> None:
        self._recent_events.clear()
class DLQSerializer:
    """Serializes quarantine records for Kafka DLQ delivery."""

    @staticmethod
    def serialize(record: Dict[str, Any]) -> str:
        import json

        return json.dumps(
            record,
            separators=(",", ":"),
            default=str,
        )