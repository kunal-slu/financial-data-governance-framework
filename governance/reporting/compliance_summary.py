"""Utilities for turning raw governance artifacts into reviewer-friendly summaries."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


class ComplianceSummaryBuilder:
    """Build concise roll-up summaries from generated governance artifacts."""

    def build_validation_summary(self, bundle: Any) -> dict[str, Any]:
        results = getattr(bundle, "results", []) or []
        critical_rules = [
            r.rule_id
            for r in results
            if getattr(r, "severity", "") == "CRITICAL" and not getattr(r, "passed", True)
        ]
        return {
            "bundle_id": getattr(bundle, "bundle_id", ""),
            "reporting_date": getattr(bundle, "reporting_date", ""),
            "regulatory_scope": getattr(bundle, "regulatory_scope", ""),
            "total_rules": getattr(bundle, "total_rules", 0),
            "passed_rules": getattr(bundle, "passed_rules", 0),
            "failed_rules": getattr(bundle, "failed_rules", 0),
            "critical_failures": getattr(bundle, "critical_failures", 0),
            "critical_checks_passed": getattr(
                bundle,
                "critical_checks_passed",
                getattr(bundle, "submission_ready", False),
            ),
            "critical_rule_ids": critical_rules,
        }

    def build_model_summary(self, report: Any) -> dict[str, Any]:
        drift_results = getattr(report, "drift_results", []) or []
        fairness_results = getattr(report, "fairness_results", []) or []
        return {
            "model_id": getattr(report, "model_id", ""),
            "model_version": getattr(report, "model_version", ""),
            "overall_status": getattr(report, "overall_status", "UNKNOWN"),
            "drift_findings": len([r for r in drift_results if getattr(r, "drifted", False)]),
            "fairness_findings": len([r for r in fairness_results if not getattr(r, "passed", True)]),
            "ready_for_review": getattr(report, "ready_for_review", False),
            "explainability_status": self._extract_explainability_status(
                getattr(report, "explainability", None)
            ),
        }

    def write_json(self, payload: dict[str, Any], path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=self._default))
        return path

    def write_markdown(self, payload: dict[str, Any], title: str, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [f"# {title}", ""]
        for key, value in payload.items():
            lines.append(f"- **{key}**: {value}")
        lines.append("")
        path.write_text("\n".join(lines))
        return path

    @staticmethod
    def _default(value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @staticmethod
    def _extract_explainability_status(explainability: Any) -> str:
        status = getattr(explainability, "status", None)
        if hasattr(status, "value"):
            return status.value
        if isinstance(status, str):
            return status
        return "unknown"
