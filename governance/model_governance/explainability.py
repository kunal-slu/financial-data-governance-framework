"""Optional explainability reporting model for FDGF monitoring outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ExplainabilityStatus(str, Enum):
    IMPLEMENTED = "implemented"
    UNAVAILABLE = "unavailable"
    SKIPPED = "skipped"
    NOT_CONFIGURED = "not_configured"


@dataclass
class ExplainabilityReport:
    status: ExplainabilityStatus
    note: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "note": self.note,
            "details": self.details,
        }


def default_explainability_report() -> ExplainabilityReport:
    return ExplainabilityReport(
        status=ExplainabilityStatus.NOT_CONFIGURED,
        note="Explainability reporting is not configured in the current reference implementation.",
    )
