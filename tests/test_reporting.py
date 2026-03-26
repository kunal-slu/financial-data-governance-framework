from governance.data_quality.validators import AuditBundle, ValidationResult, RuleType, Severity
from governance.model_governance.drift_detector import FairnessResult, ModelGovernanceReport
from governance.model_governance.explainability import (
    ExplainabilityReport,
    ExplainabilityStatus,
)
from governance.reporting import ComplianceSummaryBuilder
from governance.reporting.compliance_summary import DATASET_FINGERPRINT_SCOPE
from governance._version import FRAMEWORK_VERSION


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
    assert payload["framework_version"] == FRAMEWORK_VERSION
    assert payload["dataset_fingerprint"] == "abc"
    assert payload["dataset_fingerprint_method"] == "schema_row_count"
    assert payload["dataset_fingerprint_scope"] == DATASET_FINGERPRINT_SCOPE

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

    assert payload["framework_version"] == FRAMEWORK_VERSION
    assert payload["has_fairness_violation"] is False
    assert payload["fairness_skipped"] == 0
    assert payload["has_skipped_fairness"] is False
    assert payload["explainability_status"] == "not_configured"
    assert payload["ready_for_review"] is False
    assert report.framework_version == FRAMEWORK_VERSION


def test_model_summary_builder_surfaces_skipped_fairness():
    report = ModelGovernanceReport(
        report_id="M002",
        model_id="MODEL",
        model_version="1.0",
        reporting_date="2026-03-31",
        monitoring_period="MONTHLY",
        drift_results=[],
        fairness_results=[
            FairnessResult(
                model_id="MODEL",
                metric="AIR_SELECTION",
                protected_group="A",
                reference_group="B",
                group_rate=0.0,
                reference_rate=0.0,
                air_ratio=0.0,
                passed=False,
                details="Missing sensitive attribute.",
                status="SKIPPED",
            )
        ],
        explainability=ExplainabilityReport(
            status=ExplainabilityStatus.NOT_CONFIGURED,
            note="Demo-only monitoring run.",
        ),
        ready_for_review=False,
    )

    payload = ComplianceSummaryBuilder().build_model_summary(report)

    assert payload["overall_status"] == "FAIRNESS_SKIPPED"
    assert payload["has_fairness_violation"] is False
    assert payload["fairness_findings"] == 0
    assert payload["fairness_skipped"] == 1
    assert payload["has_skipped_fairness"] is True


def test_model_summary_builder_surfaces_failed_fairness():
    report = ModelGovernanceReport(
        report_id="M003",
        model_id="MODEL",
        model_version="1.0",
        reporting_date="2026-03-31",
        monitoring_period="MONTHLY",
        drift_results=[],
        fairness_results=[
            FairnessResult(
                model_id="MODEL",
                metric="AIR_SELECTION",
                protected_group="A",
                reference_group="B",
                group_rate=0.5,
                reference_rate=0.9,
                air_ratio=0.56,
                passed=False,
                details="Selection-rate disparity exceeded threshold.",
                status="COMPUTED",
            )
        ],
        explainability=ExplainabilityReport(
            status=ExplainabilityStatus.NOT_CONFIGURED,
            note="Demo-only monitoring run.",
        ),
        ready_for_review=False,
    )

    payload = ComplianceSummaryBuilder().build_model_summary(report)

    assert payload["overall_status"] == "FAIRNESS_FAILED"
    assert payload["has_fairness_violation"] is True
    assert payload["fairness_findings"] == 1
    assert payload["fairness_skipped"] == 0
    assert payload["has_skipped_fairness"] is False


def test_model_summary_builder_surfaces_passed_fairness():
    report = ModelGovernanceReport(
        report_id="M004",
        model_id="MODEL",
        model_version="1.0",
        reporting_date="2026-03-31",
        monitoring_period="MONTHLY",
        drift_results=[],
        fairness_results=[
            FairnessResult(
                model_id="MODEL",
                metric="AIR_SELECTION",
                protected_group="A",
                reference_group="B",
                group_rate=0.82,
                reference_rate=0.90,
                air_ratio=0.91,
                passed=True,
                details="Selection-rate disparity within threshold.",
                status="COMPUTED",
            )
        ],
        explainability=ExplainabilityReport(
            status=ExplainabilityStatus.NOT_CONFIGURED,
            note="Demo-only monitoring run.",
        ),
        ready_for_review=True,
    )

    payload = ComplianceSummaryBuilder().build_model_summary(report)

    assert payload["overall_status"] == "MONITORING_COMPLETE"
    assert payload["has_fairness_violation"] is False
    assert payload["fairness_findings"] == 0
    assert payload["fairness_skipped"] == 0
    assert payload["has_skipped_fairness"] is False
    assert payload["ready_for_review"] is True
