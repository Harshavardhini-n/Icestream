from __future__ import annotations

from typing import Any


class ObservabilityService:
    """
    Provides a lightweight observability layer for the IceStream API.

    This service exposes observability status information without
    depending on recovery components that are not currently present
    in the repository.
    """

    def __init__(self) -> None:
        self.total_checks = 0

    def status(self) -> dict[str, Any]:
        """
        Return the current observability service status.
        """
        self.total_checks += 1

        return {
            "status": "healthy",
            "service": "observability",
            "checks": self.total_checks,
            "recovery_available": False,
        }

    def snapshot(self) -> dict[str, Any]:
        """
        Return the current observability snapshot.
        """
        return {
            "status": "healthy",
            "service": "observability",
            "total_checks": self.total_checks,
            "recovery_available": False,
        }