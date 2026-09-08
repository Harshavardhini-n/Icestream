from apps.api.observability_service import ObservabilityService


def test_service_status():
    service = ObservabilityService()

    result = service.status()

    assert result["status"] == "healthy"
    assert result["service"] == "observability"
    assert result["checks"] == 1
    assert result["recovery_available"] is False


def test_status_increments_check_count():
    service = ObservabilityService()

    service.status()
    result = service.status()

    assert result["checks"] == 2


def test_snapshot():
    service = ObservabilityService()

    service.status()
    service.status()

    result = service.snapshot()

    assert result["status"] == "healthy"
    assert result["service"] == "observability"
    assert result["total_checks"] == 2
    assert result["recovery_available"] is False