import time

import pytest

from flink.jobs.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


def test_starts_closed():
    breaker = CircuitBreaker()

    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_rate == 0.0


def test_closed_circuit_allows_events():
    breaker = CircuitBreaker()

    assert breaker.can_process() is True
    assert breaker.allowed_count == 1


def test_circuit_opens_after_failure_threshold():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=4,
        )
    )

    for _ in range(2):
        assert breaker.can_process() is True
        breaker.record_failure()

    for _ in range(2):
        assert breaker.can_process() is True
        breaker.record_success()

    assert breaker.state == CircuitState.OPEN
    assert breaker.failure_rate == 0.5
    assert breaker.opened_count == 1


def test_minimum_samples_prevents_premature_opening():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=5,
        )
    )

    for _ in range(2):
        breaker.can_process()
        breaker.record_failure()

    assert breaker.state == CircuitState.CLOSED


def test_open_circuit_blocks_events():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=2,
            recovery_timeout_seconds=60,
        )
    )

    breaker.can_process()
    breaker.record_failure()

    breaker.can_process()
    breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.can_process() is False
    assert breaker.blocked_count == 1


def test_open_circuit_enters_half_open_after_timeout():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=2,
            recovery_timeout_seconds=1,
        )
    )

    breaker.can_process()
    breaker.record_failure()

    breaker.can_process()
    breaker.record_failure()

    assert breaker.state == CircuitState.OPEN

    breaker._opened_at = time.monotonic() - 2

    assert breaker.can_process() is True
    assert breaker.state == CircuitState.HALF_OPEN


def test_successful_probe_closes_circuit():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=2,
            recovery_timeout_seconds=1,
        )
    )

    breaker.can_process()
    breaker.record_failure()

    breaker.can_process()
    breaker.record_failure()

    breaker._opened_at = time.monotonic() - 2

    assert breaker.can_process() is True
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_success()

    assert breaker.state == CircuitState.CLOSED
    assert breaker.closed_count == 1


def test_failed_probe_reopens_circuit():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=2,
            recovery_timeout_seconds=1,
        )
    )

    breaker.can_process()
    breaker.record_failure()

    breaker.can_process()
    breaker.record_failure()

    breaker._opened_at = time.monotonic() - 2

    assert breaker.can_process() is True
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.opened_count == 2


def test_half_open_allows_only_one_probe():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_samples=2,
            recovery_timeout_seconds=1,
        )
    )

    breaker.can_process()
    breaker.record_failure()

    breaker.can_process()
    breaker.record_failure()

    breaker._opened_at = time.monotonic() - 2

    assert breaker.can_process() is True
    assert breaker.can_process() is False


def test_status_contains_observability_metrics():
    breaker = CircuitBreaker()

    breaker.can_process()
    breaker.record_success()

    status = breaker.status()

    assert status["state"] == "closed"
    assert status["total_samples"] == 1
    assert status["success_count"] == 1
    assert status["failure_count"] == 0
    assert status["failure_rate"] == 0.0


def test_reset_restores_initial_state():
    breaker = CircuitBreaker()

    breaker.can_process()
    breaker.record_failure()

    breaker.reset()

    assert breaker.state == CircuitState.CLOSED
    assert breaker.total_samples == 0
    assert breaker.failed_samples == 0
    assert breaker.failure_rate == 0.0


def test_invalid_threshold_is_rejected():
    with pytest.raises(ValueError):
        CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=1.5,
            )
        )


def test_invalid_minimum_samples_is_rejected():
    with pytest.raises(ValueError):
        CircuitBreaker(
            CircuitBreakerConfig(
                minimum_samples=0,
            )
        )