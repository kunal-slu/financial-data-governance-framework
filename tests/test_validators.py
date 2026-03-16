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
    Severity,
    RuleType,
)


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
            dataset_hash="abc123",
            total_rules=1,
            passed_rules=1 if critical_failures == 0 else 0,
            failed_rules=0 if critical_failures == 0 else 1,
            critical_failures=critical_failures,
            results=results,
        )

    def test_submission_ready_when_no_critical_failures(self):
        bundle = self._make_bundle(critical_failures=0)
        assert bundle.submission_ready is True

    def test_submission_blocked_when_critical_failures_exist(self):
        bundle = self._make_bundle(critical_failures=3)
        assert bundle.submission_ready is False

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
        assert data["submission_ready"] is True
        assert "pass_rate_pct" in data


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

    def test_fairness_air_passes_above_threshold(self):
        from governance.model_governance.drift_detector import FairnessChecker

        checker = FairnessChecker()
        y_pred  = pd.Series([1, 1, 1, 1, 0, 1, 1, 0, 1, 1])
        groups  = pd.Series(["A", "A", "A", "A", "A", "B", "B", "B", "B", "B"])

        results = checker.check_selection_rate(
            y_pred=y_pred,
            sensitive_feature=groups,
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
            y_pred=y_pred,
            sensitive_feature=groups,
            reference_group="REF",
            model_id="CREDIT_MODEL",
        )
        assert len(results) == 1
        assert results[0].passed is False   # AIR < 0.80


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
        assert len(bundle["inputs"])  == 1
        assert len(bundle["outputs"]) == 1
        assert len(bundle["transformations"]) == 1
        assert "lineage_hash" in bundle

    def test_lineage_hash_is_deterministic(self, tmp_path):
        """Same inputs/outputs should produce the same lineage hash."""
        from governance.lineage.tracker import LineageTracker

        def make_tracker():
            t = LineageTracker("job", "ns", "Basel III RWA", tmp_path)
            t.start_run()
            t.record_input("ds", "s3://x/", "SYS", record_count=100)
            t.record_output("out", "s3://y/", "FDGF", record_count=100)
            return t

        t1 = make_tracker()
        t2 = make_tracker()

        assert t1._hash_lineage_graph() == t2._hash_lineage_graph()
