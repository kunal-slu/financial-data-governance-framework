"""
tests/test_validators.py

Unit tests for the FDGF data quality validation engine.
Demonstrates that the framework produces verifiable, reproducible
audit evidence for U.S. regulatory reporting.

Author: Kunal Kumar Singh
License: Apache 2.0
"""

from __future__ import annotations

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from pathlib import Path

from governance.data_quality.validators import (
    RegulatoryDataValidator,
    ValidationResult,
    AuditBundle,
    RuleLoader,
    ContractValidationError,
    Severity,
    RuleType,
)
from governance._version import FRAMEWORK_VERSION


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    """Mock SparkSession for unit testing without a live Spark cluster."""
    spark = MagicMock()
    return spark


@pytest.fixture
def sample_rules():
    return [
        {
            "id":             "TEST-NULL-001",
            "type":           "nullability",
            "column":         "counterparty_id",
            "severity":       "CRITICAL",
            "max_null_pct":   0.0,
            "regulatory_ref": "BCBS 239 Principle 3",
        },
        {
            "id":           "TEST-RNG-001",
            "type":         "range",
            "column":       "risk_weight_pct",
            "severity":     "CRITICAL",
            "min":          0,
            "max":          1250,
            "regulatory_ref": "Basel III — Table of Risk Weights",
        },
        {
            "id":       "TEST-UNQ-001",
            "type":     "uniqueness",
            "columns":  ["counterparty_id", "facility_id"],
            "severity": "HIGH",
            "regulatory_ref": "BCBS 239 Principle 4",
        },
    ]


# ---------------------------------------------------------------------------
# ValidationResult Tests
# ---------------------------------------------------------------------------

class TestValidationResult:

    def test_creates_with_correct_fields(self):
        result = ValidationResult(
            rule_id="BAS-NULL-001",
            rule_type=RuleType.NULLABILITY,
            column="counterparty_id",
            severity=Severity.CRITICAL,
            passed=True,
            records_checked=1000,
            records_failed=0,
            failure_rate_pct=0.0,
            details="All values populated",
            regulatory_ref="BCBS 239 Principle 3",
        )
        assert result.passed is True
        assert result.records_failed == 0
        assert result.severity == Severity.CRITICAL

    def test_to_dict_is_serializable(self):
        result = ValidationResult(
            rule_id="BAS-NULL-001",
            rule_type=RuleType.NULLABILITY,
            column="exposure_amount",
            severity=Severity.HIGH,
            passed=False,
            records_checked=500,
            records_failed=10,
            failure_rate_pct=2.0,
            details="10 null values found",
        )
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["rule_id"] == "BAS-NULL-001"
        assert d["failure_rate_pct"] == 2.0


class TestRuleLoader:

    def test_basel3_contract_loads(self):
        contract_path = (
            Path(__file__).resolve().parents[1]
            / "templates"
            / "data_contracts"
            / "basel3_contract.yaml"
        )

        rules = RuleLoader(contract_path).load()

        assert len(rules) > 0
        assert any(rule["id"] == "BAS-NULL-001" for rule in rules)

    def test_universal_control_set_loads(self):
        contract_path = (
            Path(__file__).resolve().parents[1]
            / "assets"
            / "control_sets"
            / "universal_record_controls.yaml"
        )

        rules = RuleLoader(contract_path).load()

        assert len(rules) > 0
        assert any(rule["id"] == "UCR_001" for rule in rules)

    def test_invalid_rule_type_raises_contract_error(self, tmp_path):
        contract_path = tmp_path / "invalid_type.yaml"
        contract_path.write_text(
            "rules:\n"
            "  - id: BAD-001\n"
            "    type: nope\n"
            "    column: value\n"
            "    severity: HIGH\n"
        )

        with pytest.raises(ContractValidationError):
            RuleLoader(contract_path).load()

    def test_invalid_range_raises_contract_error(self, tmp_path):
        contract_path = tmp_path / "invalid_range.yaml"
        contract_path.write_text(
            "rules:\n"
            "  - id: BAD-002\n"
            "    type: range\n"
            "    column: value\n"
            "    severity: HIGH\n"
            "    min: 10\n"
            "    max: 5\n"
        )

        with pytest.raises(ContractValidationError):
            RuleLoader(contract_path).load()

    def test_invalid_schema_match_type_mapping_raises_contract_error(self, tmp_path):
        contract_path = tmp_path / "invalid_schema.yaml"
        contract_path.write_text(
            "rules:\n"
            "  - id: BAD-003\n"
            "    type: schema_match\n"
            "    severity: HIGH\n"
            "    required_columns:\n"
            "      counterparty_id: 42\n"
        )

        with pytest.raises(ContractValidationError):
            RuleLoader(contract_path).load()


# ---------------------------------------------------------------------------
# AuditBundle Tests
# ---------------------------------------------------------------------------

class TestAuditBundle:

    def _make_bundle(self, critical_failures=0):
        results = [
            ValidationResult(
                rule_id="R001", rule_type=RuleType.NULLABILITY, column="col_a",
                severity=Severity.CRITICAL if critical_failures else Severity.LOW,
                passed=critical_failures == 0,
                records_checked=100, records_failed=critical_failures,
                failure_rate_pct=critical_failures, details="test",
            )
        ]
        return AuditBundle(
            bundle_id="TEST001",
            reporting_date="2026-03-31",
            regulatory_scope="Basel III RWA",
            dataset_fingerprint="abc123",
            total_rules=1,
            passed_rules=1 if critical_failures == 0 else 0,
            failed_rules=0 if critical_failures == 0 else 1,
            critical_failures=critical_failures,
            results=results,
        )

    def test_critical_checks_passed_when_no_critical_failures(self):
        bundle = self._make_bundle(critical_failures=0)
        assert bundle.critical_checks_passed is True

    def test_critical_checks_fail_when_critical_failures_exist(self):
        bundle = self._make_bundle(critical_failures=3)
        assert bundle.critical_checks_passed is False

    def test_pass_rate_calculation(self):
        bundle = self._make_bundle(critical_failures=0)
        assert bundle.pass_rate_pct == 100.0

    def test_to_json_writes_file(self, tmp_path):
        bundle = self._make_bundle()
        out    = tmp_path / "audit_bundle.json"
        bundle.to_json(out)
        assert out.exists()
        import json
        with open(out) as f:
            data = json.load(f)
        assert data["bundle_id"] == "TEST001"
        assert data["critical_checks_passed"] is True
        assert data["framework_version"] == FRAMEWORK_VERSION
        assert data["dataset_fingerprint_method"] == "schema_row_count"
        assert "pass_rate_pct" in data


class TestValidatorFingerprint:

    def test_dataframe_fingerprint_uses_schema_and_row_count_only(self, mock_spark, tmp_path):
        validator = RegulatoryDataValidator(mock_spark, tmp_path / "unused.yaml")
        df = MagicMock()
        schema = MagicMock()
        schema.jsonValue.return_value = {
            "type": "struct",
            "fields": [{"name": "counterparty_id", "type": "string"}],
        }
        df.schema = schema

        fingerprint_a = validator._fingerprint_dataframe(df, 10)
        fingerprint_b = validator._fingerprint_dataframe(df, 10)
        fingerprint_c = validator._fingerprint_dataframe(df, 11)

        assert fingerprint_a == fingerprint_b
        assert fingerprint_a != fingerprint_c
        df.limit.assert_not_called()

    def test_dataframe_fingerprint_changes_when_schema_changes(self, mock_spark, tmp_path):
        validator = RegulatoryDataValidator(mock_spark, tmp_path / "unused.yaml")
        df_a = MagicMock()
        df_b = MagicMock()
        schema_a = MagicMock()
        schema_b = MagicMock()
        schema_a.jsonValue.return_value = {
            "type": "struct",
            "fields": [{"name": "counterparty_id", "type": "string"}],
        }
        schema_b.jsonValue.return_value = {
            "type": "struct",
            "fields": [
                {"name": "counterparty_id", "type": "string"},
                {"name": "facility_id", "type": "string"},
            ],
        }
        df_a.schema = schema_a
        df_b.schema = schema_b

        assert validator._fingerprint_dataframe(df_a, 10) != validator._fingerprint_dataframe(df_b, 10)

    def test_timeliness_reference_values_are_reporting_date_based(self, mock_spark, tmp_path):
        validator = RegulatoryDataValidator(mock_spark, tmp_path / "unused.yaml")

        assert validator._timeliness_reference_date("2026-03-31") == "2026-03-31"
        assert validator._timeliness_reference_timestamp("2026-03-31") == "2026-03-31 23:59:59"


# ---------------------------------------------------------------------------
# Drift Detection Tests
# ---------------------------------------------------------------------------

class TestDriftDetection:

    def test_psi_no_drift(self):
        from governance.model_governance.drift_detector import PSICalculator
        import numpy as np

        calc     = PSICalculator()
        baseline = pd.Series(np.random.normal(600, 50, 10000))   # FICO-like distribution
        current  = pd.Series(np.random.normal(600, 50, 10000))   # Same distribution

        result = calc.calculate(baseline, current, "fico_score", "TEST_MODEL")
        assert result.metric == "PSI"
        assert result.value < 0.10  # Expect low PSI for identical distributions
        assert result.drifted is False

    def test_psi_significant_drift(self):
        from governance.model_governance.drift_detector import PSICalculator
        import numpy as np

        calc     = PSICalculator()
        baseline = pd.Series(np.random.normal(650, 30, 10000))   # Good borrower pool
        current  = pd.Series(np.random.normal(550, 60, 10000))   # Degraded borrower pool

        result = calc.calculate(baseline, current, "fico_score", "TEST_MODEL")
        assert result.drifted is True
        assert result.severity in ("MEDIUM", "CRITICAL")

    def test_psi_handles_constant_baseline(self):
        from governance.model_governance.drift_detector import PSICalculator

        calc = PSICalculator()
        baseline = pd.Series([600.0] * 100)
        current = pd.Series([600.0] * 100)

        result = calc.calculate(baseline, current, "fico_score", "TEST_MODEL")
        assert result.drifted is False
        assert "skipped" in result.interpretation.lower()

    def test_ks_skips_empty_baseline(self):
        from governance.model_governance.drift_detector import KSTestMonitor

        result = KSTestMonitor().calculate(
            baseline_scores=pd.Series([], dtype=float),
            current_scores=pd.Series([0.1, 0.2, 0.3]),
            model_id="TEST_MODEL",
        )

        assert result.drifted is False
        assert "skipped" in result.interpretation.lower()

    def test_ks_skips_empty_current(self):
        from governance.model_governance.drift_detector import KSTestMonitor

        result = KSTestMonitor().calculate(
            baseline_scores=pd.Series([0.1, 0.2, 0.3]),
            current_scores=pd.Series([], dtype=float),
            model_id="TEST_MODEL",
        )

        assert result.drifted is False
        assert "skipped" in result.interpretation.lower()

    def test_ks_skips_all_null_inputs(self):
        from governance.model_governance.drift_detector import KSTestMonitor

        result = KSTestMonitor().calculate(
            baseline_scores=pd.Series([None, None]),
            current_scores=pd.Series([None, None]),
            model_id="TEST_MODEL",
        )

        assert result.drifted is False
        assert "skipped" in result.interpretation.lower()

    def test_fairness_air_passes_above_threshold(self):
        from governance.model_governance.drift_detector import FairnessChecker

        checker = FairnessChecker()
        y_pred  = pd.Series([1, 1, 1, 1, 0, 1, 1, 0, 1, 1])
        groups  = pd.Series(["A", "A", "A", "A", "A", "B", "B", "B", "B", "B"])

        results = checker.check_selection_rate(
            decision_series=y_pred,
            sensitive_attribute=groups,
            reference_group="A",
            model_id="CREDIT_MODEL",
        )
        assert len(results) == 1
        result = results[0]
        # Group B: 3/5 = 0.60, Group A: 4/5 = 0.80, AIR = 0.60/0.80 = 0.75
        assert result.metric == "AIR_SELECTION_RATE"
        assert isinstance(result.air_ratio, float)

    def test_fairness_air_fails_below_threshold(self):
        from governance.model_governance.drift_detector import FairnessChecker

        checker = FairnessChecker()
        # Reference group gets 90% approval, protected group gets 50% — AIR = 0.56
        y_pred  = pd.Series([1]*9 + [0] + [1]*5 + [0]*5)
        groups  = pd.Series(["REF"]*10 + ["PROT"]*10)

        results = checker.check_selection_rate(
            decision_series=y_pred,
            sensitive_attribute=groups,
            reference_group="REF",
            model_id="CREDIT_MODEL",
        )
        assert len(results) == 1
        assert results[0].passed is False   # AIR < 0.80

    def test_fairness_missing_reference_group_fails_cleanly(self):
        from governance.model_governance.drift_detector import FairnessChecker

        checker = FairnessChecker()
        with pytest.raises(ValueError):
            checker.check_selection_rate(
                decision_series=pd.Series([1, 0, 1]),
                sensitive_attribute=pd.Series(["A", "A", "B"]),
                reference_group="REF",
                model_id="MODEL",
            )

    def test_fairness_zero_reference_rate_fails_cleanly(self):
        from governance.model_governance.drift_detector import FairnessChecker

        checker = FairnessChecker()
        with pytest.raises(ValueError):
            checker.check_selection_rate(
                decision_series=pd.Series([0, 0, 1, 1]),
                sensitive_attribute=pd.Series(["REF", "REF", "B", "B"]),
                reference_group="REF",
                model_id="MODEL",
            )

    def test_model_monitor_flags_missing_decision_column_as_skipped(self, tmp_path):
        from governance.model_governance.drift_detector import ModelGovernanceMonitor

        monitor = ModelGovernanceMonitor(output_dir=tmp_path)
        baseline = pd.DataFrame({"score": [0.1, 0.2]})
        current = pd.DataFrame({"score": [0.1, 0.2], "segment": ["A", "B"]})

        report = monitor.run_full_assessment(
            model_id="MODEL",
            model_version="1.0",
            baseline_data=baseline,
            current_data=current,
            feature_columns=[],
            score_column="score",
            decision_column="decision_flag",
            sensitive_column="segment",
            reference_group="A",
            reporting_date="2026-03-31",
        )

        assert report.ready_for_review is False
        assert len(report.fairness_results) == 1
        assert report.fairness_results[0].status == "SKIPPED"
        assert "decision_flag" in report.fairness_results[0].details

    def test_model_monitor_flags_missing_sensitive_column_as_skipped(self, tmp_path):
        from governance.model_governance.drift_detector import ModelGovernanceMonitor

        monitor = ModelGovernanceMonitor(output_dir=tmp_path)
        baseline = pd.DataFrame({"score": [0.1, 0.2]})
        current = pd.DataFrame({"score": [0.1, 0.2], "decision_flag": [1, 0]})

        report = monitor.run_full_assessment(
            model_id="MODEL",
            model_version="1.0",
            baseline_data=baseline,
            current_data=current,
            feature_columns=[],
            score_column="score",
            decision_column="decision_flag",
            sensitive_column="segment",
            reference_group="A",
            reporting_date="2026-03-31",
        )

        assert report.ready_for_review is False
        assert len(report.fairness_results) == 1
        assert report.fairness_results[0].status == "SKIPPED"
        assert "segment" in report.fairness_results[0].details


# ---------------------------------------------------------------------------
# Lineage Tracker Tests
# ---------------------------------------------------------------------------

class TestLineageTracker:

    def test_start_and_complete_run(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        tracker = LineageTracker(
            job_name="test_pipeline",
            job_namespace="fdgf.test",
            regulatory_scope="Basel III RWA",
            output_dir=tmp_path,
        )

        run_id = tracker.start_run()
        assert isinstance(run_id, str)
        assert len(run_id) == 36   # UUID format

        tracker.record_input("raw_exposures", "s3://test/", "CORE_BANKING", record_count=1000)
        tracker.record_transformation("rwa_calc", "AGGREGATE", sql_or_code="SELECT ...")
        tracker.record_output("rwa_report", "s3://test/out/", "FDGF", record_count=1000)

        bundle_path = tracker.complete_run()
        assert bundle_path is not None

        import json
        with open(bundle_path) as f:
            bundle = json.load(f)

        assert bundle["run_id"] == run_id
        assert bundle["regulatory_scope"] == "Basel III RWA"
        assert bundle["framework_version"] == FRAMEWORK_VERSION
        assert len(bundle["inputs"])  == 1
        assert len(bundle["outputs"]) == 1
        assert len(bundle["transformations"]) == 1
        assert "lineage_fingerprint" in bundle

    def test_lineage_fingerprint_is_deterministic(self, tmp_path):
        """Same inputs/outputs should produce the same lineage fingerprint."""
        from governance.lineage.tracker import LineageTracker

        def make_tracker():
            t = LineageTracker("job", "ns", "Basel III RWA", tmp_path)
            t.start_run()
            t.record_input("ds", "s3://x/", "SYS", record_count=100)
            t.record_output("out", "s3://y/", "FDGF", record_count=100)
            return t

        t1 = make_tracker()
        t2 = make_tracker()

        assert t1._fingerprint_lineage_graph() == t2._fingerprint_lineage_graph()

    def test_lineage_fingerprint_is_deterministic_across_reordered_equivalent_artifacts(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        first = LineageTracker("job", "ns", "Basel III RWA", tmp_path)
        first.start_run()
        first.record_input("a", "s3://one/", "SYS", record_count=10)
        first.record_input("b", "s3://two/", "SYS", record_count=20)
        first.record_transformation("join_data", "JOIN", sql_or_code="SELECT * FROM a JOIN b")
        first.record_output("out", "s3://out/", "FDGF", record_count=30)

        second = LineageTracker("job", "ns", "Basel III RWA", tmp_path)
        second.start_run()
        second.record_input("b", "s3://two/", "SYS", record_count=20)
        second.record_input("a", "s3://one/", "SYS", record_count=10)
        second.record_transformation("join_data", "JOIN", sql_or_code="SELECT * FROM a JOIN b")
        second.record_output("out", "s3://out/", "FDGF", record_count=30)

        assert first._fingerprint_lineage_graph() == second._fingerprint_lineage_graph()

    def test_recording_requires_active_run(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        tracker = LineageTracker("job", "ns", "scope", tmp_path)
        with pytest.raises(RuntimeError):
            tracker.record_input("ds", "s3://x/", "SYS", record_count=10)

    def test_start_run_generates_new_run_id_each_time(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        tracker = LineageTracker("job", "ns", "scope", tmp_path)
        first = tracker.start_run()
        tracker.complete_run()
        second = tracker.start_run()
        assert first != second

    def test_local_file_hash_uses_real_file_content(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        data_dir = tmp_path / "sample_data"
        data_dir.mkdir()
        sample_file = data_dir / "demo.csv"
        sample_file.write_text("id,value\n1,10\n")

        tracker = LineageTracker("job", "ns", "scope", tmp_path)
        tracker.start_run()
        tracker.record_input("demo.csv", f"local://{data_dir}", "LOCAL", record_count=1)

        expected = __import__("hashlib").sha256(sample_file.read_bytes()).hexdigest()
        assert tracker._inputs[0].dataset_fingerprint == expected
        assert tracker._inputs[0].dataset_fingerprint_method == "sha256_file_content"

    def test_non_local_dataset_uses_metadata_fingerprint_method(self, tmp_path):
        from governance.lineage.tracker import LineageTracker

        tracker = LineageTracker("job", "ns", "scope", tmp_path)
        tracker.start_run()
        tracker.record_input("demo", "s3://bucket/path", "REMOTE", record_count=10)

        assert tracker._inputs[0].dataset_fingerprint_method == "metadata_shape_fingerprint"
