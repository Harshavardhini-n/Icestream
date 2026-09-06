"""Tests for Commit 12 observability engine."""

from __future__ import annotations

from flink.jobs.checkout_processor import (
    ObservabilityEngine,
)


def make_event(
    event_id: str,
    quality_status: str = "passed",
    processing_time_ms: float = 5.0,
) -> dict:
    return {
        "event_id": event_id,
        "data_quality_status": quality_status,
        "data_quality_errors": (
            []
            if quality_status == "passed"
            else ["subtotal_inconsistent"]
        ),
        "processing_time_ms": processing_time_ms,
    }


class TestObservabilityEngine:
    def test_valid_event_is_recorded(self):
        engine = ObservabilityEngine()

        result = engine.record_event(
            make_event("evt-001")
        )

        assert result["observability_recorded"] is True
        assert result["pipeline_status"] == "healthy"
        assert engine.total_events == 1
        assert engine.processed_events == 1
        assert engine.quality_passed == 1
        assert engine.quality_failed == 0

    def test_quality_failure_marks_pipeline_degraded(self):
        engine = ObservabilityEngine()

        result = engine.record_event(
            make_event(
                "evt-002",
                quality_status="failed",
            )
        )

        assert result["observability_recorded"] is True
        assert result["pipeline_status"] == "degraded"
        assert engine.quality_failed == 1
        assert engine.quality_passed == 0

    def test_quality_pass_rate(self):
        engine = ObservabilityEngine()

        engine.record_event(make_event("evt-001"))
        engine.record_event(make_event("evt-002"))
        engine.record_event(
            make_event(
                "evt-003",
                quality_status="failed",
            )
        )
        engine.record_event(make_event("evt-004"))

        assert engine.quality_pass_rate() == 75.0

    def test_quality_failure_rate(self):
        engine = ObservabilityEngine()

        engine.record_event(make_event("evt-001"))
        engine.record_event(
            make_event(
                "evt-002",
                quality_status="failed",
            )
        )
        engine.record_event(
            make_event(
                "evt-003",
                quality_status="failed",
            )
        )
        engine.record_event(make_event("evt-004"))

        assert engine.quality_failure_rate() == 50.0

    def test_average_processing_time(self):
        engine = ObservabilityEngine()

        engine.record_event(
            make_event(
                "evt-001",
                processing_time_ms=10.0,
            )
        )
        engine.record_event(
            make_event(
                "evt-002",
                processing_time_ms=20.0,
            )
        )

        assert engine.average_processing_time_ms() == 15.0

    def test_validation_failure_is_recorded(self):
        engine = ObservabilityEngine()

        engine.record_validation_failure(
            {"event_id": "evt-invalid"}
        )

        snapshot = engine.snapshot()

        assert engine.total_events == 1
        assert engine.validation_failed == 1
        assert snapshot["validation_failed"] == 1

    def test_deserialization_failure_is_recorded(self):
        engine = ObservabilityEngine()

        engine.record_deserialization_failure()

        snapshot = engine.snapshot()

        assert engine.total_events == 1
        assert engine.deserialization_failed == 1
        assert snapshot["deserialization_failed"] == 1

    def test_recent_failures_are_recorded(self):
        engine = ObservabilityEngine()

        engine.record_event(
            make_event(
                "evt-failed",
                quality_status="failed",
            )
        )

        snapshot = engine.snapshot()

        assert len(snapshot["recent_failures"]) == 1
        assert (
            snapshot["recent_failures"][0]["event_id"]
            == "evt-failed"
        )
        assert (
            snapshot["recent_failures"][0]["failure_type"]
            == "data_quality"
        )

    def test_recent_failures_are_bounded(self):
        engine = ObservabilityEngine(
            max_recent_failures=2
        )

        for index in range(3):
            engine.record_event(
                make_event(
                    f"evt-{index}",
                    quality_status="failed",
                )
            )

        failures = engine.snapshot()["recent_failures"]

        assert len(failures) == 2
        assert failures[0]["event_id"] == "evt-1"
        assert failures[1]["event_id"] == "evt-2"

    def test_snapshot_contains_expected_metrics(self):
        engine = ObservabilityEngine()

        engine.record_event(
            make_event("evt-001")
        )

        snapshot = engine.snapshot()

        assert snapshot["total_events"] == 1
        assert snapshot["processed_events"] == 1
        assert snapshot["quality_passed"] == 1
        assert snapshot["quality_failed"] == 0
        assert snapshot["quality_pass_rate"] == 100.0
        assert snapshot["quality_failure_rate"] == 0.0
        assert snapshot["pipeline_status"] == "healthy"

    def test_empty_engine_has_safe_metrics(self):
        engine = ObservabilityEngine()

        snapshot = engine.snapshot()

        assert snapshot["total_events"] == 0
        assert snapshot["quality_pass_rate"] == 0.0
        assert snapshot["quality_failure_rate"] == 0.0
        assert snapshot["average_processing_time_ms"] == 0.0
        assert snapshot["pipeline_status"] == "healthy"