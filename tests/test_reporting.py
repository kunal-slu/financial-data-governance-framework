from governance.data_quality.validators import AuditBundle, ValidationResult, RuleType, Severity
from governance.model_governance.drift_detector import ModelGovernanceReport
from governance.model_governance.explainability import (
    ExplainabilityReport,
    ExplainabilityStatus,
)
from governance.reporting import ComplianceSummaryBuilder


def test_validation_summary_builder(tmp_path):
    bundle = AuditBundle(
        bundle_id="B001",
        reporting_date="2026-03-31",
        regulatory_scope="Basel III",
        dataset_fingerprint="abc",
        total_rules=2,
        passed_rules=1,
        failed_rules=1,
        critical_failures=1,
        results=[
            ValidationResult(
                rule_id="R1",
                rule_type=RuleType.NULLABILITY,
                column="loan_id",
                severity=Severity.CRITICAL,
                passed=False,
                records_checked=10,
                records_failed=1,
                failure_rate_pct=10.0,
                details="missing",
            )
        ],
    )
    builder = ComplianceSummaryBuilder()
    payload = builder.build_validation_summary(bundle)
    assert payload["critical_checks_passed"] is False
    assert payload["critical_rule_ids"] == ["R1"]

    json_path = builder.write_json(payload, tmp_path / "summary.json")
    md_path = builder.write_markdown(payload, "Validation Summary", tmp_path / "summary.md")
    assert json_path.exists()
    assert md_path.exists()


def test_model_summary_builder_uses_explainability_report_status():
    report = ModelGovernanceReport(
        report_id="M001",
        model_id="MODEL",
        model_version="1.0",
        reporting_date="2026-03-31",
        monitoring_period="MONTHLY",
        drift_results=[],
        fairness_results=[],
        explainability=ExplainabilityReport(
            status=ExplainabilityStatus.NOT_CONFIGURED,
            note="Demo-only monitoring run.",
        ),
        ready_for_review=False,
    )

    payload = ComplianceSummaryBuilder().build_model_summary(report)

    assert payload["explainability_status"] == "not_configured"
    assert payload["ready_for_review"] is False
