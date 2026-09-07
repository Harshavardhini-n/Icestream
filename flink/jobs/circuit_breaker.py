from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import monotonic
from typing import Any, Dict, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: float = 0.50
    minimum_samples: int = 5
    recovery_timeout_seconds: float = 30.0


class CircuitBreaker:
    """
    Protects the downstream processing pipeline from sustained failures.

    CLOSED:
        Events are allowed through normally.

    OPEN:
        Events are blocked because the observed failure rate exceeded
        the configured threshold.

    HALF_OPEN:
        After the recovery timeout, one event is allowed as a recovery
        probe. A successful probe closes the circuit; a failed probe
        opens it again.
    """

    def __init__(
        self,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> None:
        self.config = config or CircuitBreakerConfig()

        if not 0.0 <= self.config.failure_threshold <= 1.0:
            raise ValueError("failure_threshold must be between 0 and 1")

        if self.config.minimum_samples < 1:
            raise ValueError("minimum_samples must be at least 1")

        if self.config.recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds cannot be negative")

        self.state = CircuitState.CLOSED

        self.total_samples = 0
        self.failed_samples = 0

        self.allowed_count = 0
        self.blocked_count = 0
        self.success_count = 0
        self.failure_count = 0

        self.opened_count = 0
        self.closed_count = 0
        self.half_open_count = 0

        self._opened_at: Optional[float] = None
        self._probe_in_progress = False

    @property
    def failure_rate(self) -> float:
        if self.total_samples == 0:
            return 0.0

        return self.failed_samples / self.total_samples

    def can_process(self, now: Optional[float] = None) -> bool:
        """
        Determine whether the current event is allowed through.
        """

        current_time = monotonic() if now is None else now

        if self.state == CircuitState.CLOSED:
            self.allowed_count += 1
            return True

        if self.state == CircuitState.OPEN:
            if self._opened_at is None:
                self.blocked_count += 1
                return False

            elapsed = current_time - self._opened_at

            if elapsed < self.config.recovery_timeout_seconds:
                self.blocked_count += 1
                return False

            self.state = CircuitState.HALF_OPEN
            self.half_open_count += 1
            self._probe_in_progress = True

            self.allowed_count += 1
            return True

        # HALF_OPEN
        if self._probe_in_progress:
            self.blocked_count += 1
            return False

        self._probe_in_progress = True
        self.allowed_count += 1
        return True

    def record_success(self) -> None:
        self.total_samples += 1
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.closed_count += 1
            self._opened_at = None
            self._probe_in_progress = False
            return

        if (
            self.state == CircuitState.CLOSED
            and self.total_samples >= self.config.minimum_samples
            and self.failure_rate >= self.config.failure_threshold
        ):
            self._open()

        self._probe_in_progress = False

    def record_failure(self) -> None:
        self.total_samples += 1
        self.failed_samples += 1
        self.failure_count += 1

        if self.state == CircuitState.HALF_OPEN:
            self._open()
            self._probe_in_progress = False
            return

        if (
            self.state == CircuitState.CLOSED
            and self.total_samples >= self.config.minimum_samples
            and self.failure_rate >= self.config.failure_threshold
        ):
            self._open()

        self._probe_in_progress = False

    def _open(self) -> None:
        if self.state != CircuitState.OPEN:
            self.opened_count += 1

        self.state = CircuitState.OPEN
        self._opened_at = monotonic()

    def reset(self) -> None:
        """
        Explicitly reset the circuit and its rolling statistics.
        """

        self.state = CircuitState.CLOSED

        self.total_samples = 0
        self.failed_samples = 0

        self.allowed_count = 0
        self.blocked_count = 0
        self.success_count = 0
        self.failure_count = 0

        self.opened_count = 0
        self.closed_count = 0
        self.half_open_count = 0

        self._opened_at = None
        self._probe_in_progress = False

    def status(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "total_samples": self.total_samples,
            "failed_samples": self.failed_samples,
            "failure_rate": round(self.failure_rate, 4),
            "allowed_count": self.allowed_count,
            "blocked_count": self.blocked_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "opened_count": self.opened_count,
            "closed_count": self.closed_count,
            "half_open_count": self.half_open_count,
            "failure_threshold": self.config.failure_threshold,
            "minimum_samples": self.config.minimum_samples,
            "recovery_timeout_seconds": (
                self.config.recovery_timeout_seconds
            ),
        }