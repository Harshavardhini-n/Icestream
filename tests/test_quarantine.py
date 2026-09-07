import json

from flink.jobs.quarantine import (
    DLQSerializer,
    EventQuarantine,
    QuarantineConfig,
)


def test_quarantine_preserves_original_event():
    quarantine = EventQuarantine()

    event = {
        "event_id": "evt-001",
        "event_type": "checkout",
        "quantity": 2,
    }

    result = quarantine.quarantine(
        event,
        reason="data_quality_failure",
        errors=["quantity_invalid"],
    )

    assert result["event"] == event
    assert result["reason"] == "data_quality_failure"
    assert result["errors"] == ["quantity_invalid"]


def test_quarantine_adds_timestamp():
    quarantine = EventQuarantine()

    result = quarantine.quarantine(
        {"event_id": "evt-002"},
        reason="validation_failure",
        errors=["missing_product_id"],
    )

    assert result["quarantined_at"]
    assert "T" in result["quarantined_at"]


def test_quarantine_tracks_data_quality_failure():
    quarantine = EventQuarantine()

    quarantine.quarantine_data_quality_failure(
        {"event_id": "evt-003"},
        ["amount_inconsistent"],
    )

    assert quarantine.quarantined_count == 1
    assert quarantine.data_quality_failures == 1


def test_quarantine_tracks_validation_failure():
    quarantine = EventQuarantine()

    quarantine.quarantine_validation_failure(
        {"event_id": "evt-004"},
        ["missing_event_type"],
    )

    assert quarantine.quarantined_count == 1
    assert quarantine.validation_failures == 1


def test_quarantine_tracks_processing_failure():
    quarantine = EventQuarantine()

    quarantine.quarantine_processing_failure(
        {"event_id": "evt-005"},
        ["unexpected_exception"],
    )

    assert quarantine.quarantined_count == 1
    assert quarantine.processing_failures == 1


def test_quarantine_none_event_is_safe():
    quarantine = EventQuarantine()

    result = quarantine.quarantine(
        None,
        reason="processing_failure",
        errors=["null_event"],
    )

    assert result["event"] == {}
    assert result["reason"] == "processing_failure"


def test_empty_reason_gets_default():
    quarantine = EventQuarantine()

    result = quarantine.quarantine(
        {"event_id": "evt-006"},
        reason="",
    )

    assert result["reason"] == "unknown_failure"


def test_status_contains_counts():
    quarantine = EventQuarantine()

    quarantine.quarantine_data_quality_failure(
        {"event_id": "evt-007"},
        ["bad_total"],
    )

    status = quarantine.status()

    assert status["quarantined_count"] == 1
    assert status["data_quality_failures"] == 1
    assert status["validation_failures"] == 0
    assert status["processing_failures"] == 0


def test_recent_events_are_available():
    quarantine = EventQuarantine()

    quarantine.quarantine(
        {"event_id": "evt-008"},
        reason="validation_failure",
        errors=["bad_event"],
    )

    recent = quarantine.recent_events()

    assert len(recent) == 1
    assert recent[0]["event"]["event_id"] == "evt-008"


def test_recent_history_is_bounded():
    quarantine = EventQuarantine()

    for index in range(105):
        quarantine.quarantine(
            {"event_id": f"evt-{index}"},
            reason="processing_failure",
        )

    recent = quarantine.recent_events()

    assert len(recent) == 100
    assert recent[0]["event"]["event_id"] == "evt-5"


def test_clear_history():
    quarantine = EventQuarantine()

    quarantine.quarantine(
        {"event_id": "evt-009"},
        reason="validation_failure",
    )

    quarantine.clear_history()

    assert quarantine.recent_events() == []
    assert quarantine.quarantined_count == 1


def test_custom_topics():
    config = QuarantineConfig(
        source_topic="custom-input",
        dlq_topic="custom-dlq",
    )

    quarantine = EventQuarantine(config)

    result = quarantine.quarantine(
        {"event_id": "evt-010"},
        reason="validation_failure",
    )

    assert result["source_topic"] == "custom-input"
    assert result["dlq_topic"] == "custom-dlq"


def test_dlq_serializer_returns_json():
    quarantine = EventQuarantine()

    record = quarantine.quarantine(
        {"event_id": "evt-011"},
        reason="data_quality_failure",
        errors=["invalid_total"],
    )

    serialized = DLQSerializer.serialize(record)

    decoded = json.loads(serialized)

    assert decoded["event"]["event_id"] == "evt-011"
    assert decoded["reason"] == "data_quality_failure"
    assert decoded["errors"] == ["invalid_total"]


def test_dlq_serializer_handles_datetime_values():
    record = {
        "event_id": "evt-012",
        "timestamp": object(),
    }

    serialized = DLQSerializer.serialize(record)

    assert isinstance(serialized, str)
    assert "evt-012" in serialized