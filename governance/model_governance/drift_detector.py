"""
governance/model_governance/drift_detector.py

Illustrative model-governance monitoring utilities aligned with common review workflows.
Detects population shift, score-distribution drift, and fairness issues
in model-driven financial data workflows.

Implements:
  - Population Stability Index (PSI) — detects distribution drift
  - Kolmogorov-Smirnov Test (KS) — detects performance degradation
  - Fairlearn AIR metrics — detects demographic bias (Equal Credit Opportunity Act)
"""

from __future__ import annotations

import json
import logging
import warnings
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Optional heavy dependencies — graceful degradation if not installed
try:
    from scipy import stats as scipy_stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    warnings.warn("scipy not installed — KS test unavailable.", ImportWarning)

try:
    from fairlearn.metrics import MetricFrame, selection_rate, false_positive_rate
    FAIRLEARN_AVAILABLE = True
except ImportError:
    FAIRLEARN_AVAILABLE = False
    warnings.warn("fairlearn not installed — fairness checks unavailable.", ImportWarning)


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class DriftResult:
    model_id:       str
    feature:        str
    metric:         str           # "PSI", "KS", "CUSTOM"
    value:          float
    threshold:      float
    drifted:        bool
    severity:       str           # "CRITICAL", "HIGH", "MEDIUM", "LOW"
    interpretation: str
    timestamp:      str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    regulatory_ref: str = "Federal Reserve SR 11-7 — Ongoing Monitoring"


@dataclass
class FairnessResult:
    model_id:          str
    metric:            str        # "AIR_SELECTION", "AIR_FPR"
    protected_group:   str
    reference_group:   str
    group_rate:        float
    reference_rate:    float
    air_ratio:         float
    passed:            bool       # AIR >= 0.80 (standard ECOA/fair lending threshold)
    details:           str
    timestamp:         str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    regulatory_ref:    str = "Equal Credit Opportunity Act (ECOA) — AIR >= 0.80"


@dataclass
class ModelGovernanceReport:
    """
    Machine-readable model-governance monitoring report for example workflows.
    """
    report_id:        str
    model_id:         str
    model_version:    str
    reporting_date:   str
    monitoring_period: str
    drift_results:    list[DriftResult]
    fairness_results: list[FairnessResult]
    explanations:     dict[str, Any]
    ready_for_review: bool
    override_reason:  str = ""
    generated_at:     str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    framework_version: str = "1.0.0"

    @property
    def has_critical_drift(self) -> bool:
        return any(r.drifted and r.severity == "CRITICAL" for r in self.drift_results)

    @property
    def has_fairness_violation(self) -> bool:
        return any(not r.passed for r in self.fairness_results)

    @property
    def overall_status(self) -> str:
        if self.has_critical_drift or self.has_fairness_violation:
            return "REQUIRES_REVIEW"
        return "MONITORING_COMPLETE"

    def to_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            **asdict(self),
            "has_critical_drift":    self.has_critical_drift,
            "has_fairness_violation": self.has_fairness_violation,
            "overall_status":        self.overall_status,
        }
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        logger.info("Model governance report written → %s", path)


# ---------------------------------------------------------------------------
# PSI Calculator
# ---------------------------------------------------------------------------

class PSICalculator:
    """
    Population Stability Index — detects distribution drift between
    development (baseline) and production (current) model input data.

    SR 11-7 thresholds:
      PSI < 0.10  → No significant change    (GREEN)
      PSI 0.10–0.25 → Moderate change       (YELLOW — investigate)
      PSI > 0.25  → Significant shift        (RED — model redevelopment required)
    """

    PSI_GREEN    = 0.10
    PSI_YELLOW   = 0.25
    N_BINS       = 10

    def calculate(
        self,
        baseline: pd.Series,
        current:  pd.Series,
        feature_name: str,
        model_id: str,
    ) -> DriftResult:
        baseline_clean = pd.to_numeric(baseline, errors="coerce").dropna()
        current_clean = pd.to_numeric(current, errors="coerce").dropna()
        if baseline_clean.empty or current_clean.empty:
            return DriftResult(
                model_id=model_id,
                feature=feature_name,
                metric="PSI",
                value=0.0,
                threshold=self.PSI_YELLOW,
                drifted=False,
                severity="LOW",
                interpretation="PSI skipped because baseline or current series is empty.",
            )

        bins = self._build_bins(baseline_clean, current_clean)
        if bins is None:
            warnings.warn(
                f"PSI skipped for {feature_name}: insufficient variation to build stable bins.",
                RuntimeWarning,
            )
            return DriftResult(
                model_id=model_id,
                feature=feature_name,
                metric="PSI",
                value=0.0,
                threshold=self.PSI_YELLOW,
                drifted=False,
                severity="LOW",
                interpretation="PSI skipped because the feature does not have enough variation.",
            )

        baseline_pcts = self._bin_distribution(baseline_clean, bins)
        current_pcts  = self._bin_distribution(current_clean, bins)

        psi = float(np.sum(
            (current_pcts - baseline_pcts) * np.log((current_pcts + 1e-10) / (baseline_pcts + 1e-10))
        ))

        if psi < self.PSI_GREEN:
            severity = "LOW"
            drifted  = False
            interp   = f"PSI={psi:.4f} — No significant distribution change."
        elif psi < self.PSI_YELLOW:
            severity = "MEDIUM"
            drifted  = True
            interp   = f"PSI={psi:.4f} — Moderate shift detected. Investigate inputs."
        else:
            severity = "CRITICAL"
            drifted  = True
            interp   = f"PSI={psi:.4f} — Significant population shift. Model redevelopment may be required."

        return DriftResult(
            model_id=model_id,
            feature=feature_name,
            metric="PSI",
            value=round(psi, 6),
            threshold=self.PSI_YELLOW,
            drifted=drifted,
            severity=severity,
            interpretation=interp,
        )

    @staticmethod
    def _bin_distribution(series: pd.Series, bins: np.ndarray) -> np.ndarray:
        counts, _ = np.histogram(series.dropna(), bins=bins)
        total = counts.sum()
        if total == 0:
            return np.zeros(len(bins) - 1)
        pcts = counts / total
        return pcts

    def _build_bins(self, baseline: pd.Series, current: pd.Series) -> np.ndarray | None:
        combined = pd.concat([baseline, current]).dropna()
        if combined.nunique() < 2:
            return None

        quantiles = np.percentile(baseline, np.linspace(0, 100, self.N_BINS + 1))
        bins = np.unique(quantiles)
        if len(bins) < 3:
            minimum = float(combined.min())
            maximum = float(combined.max())
            if minimum == maximum:
                return None
            bins = np.linspace(minimum, maximum, self.N_BINS + 1)

        bins[0] -= 1e-8
        bins[-1] += 1e-8
        return bins


# ---------------------------------------------------------------------------
# KS Test
# ---------------------------------------------------------------------------

class KSTestMonitor:
    """
    Kolmogorov-Smirnov test for model score distribution drift.
    A significant KS statistic indicates the model's score distribution
    has changed materially from its validation baseline.
    """

    KS_CRITICAL_P_VALUE = 0.01   # p < 0.01 → significant drift

    def calculate(
        self,
        baseline_scores: pd.Series,
        current_scores:  pd.Series,
        model_id:        str,
    ) -> DriftResult:
        if not SCIPY_AVAILABLE:
            return DriftResult(
                model_id=model_id, feature="model_score", metric="KS",
                value=0.0, threshold=self.KS_CRITICAL_P_VALUE,
                drifted=False, severity="LOW",
                interpretation="scipy not installed — KS test skipped.",
            )

        ks_stat, p_value = scipy_stats.ks_2samp(
            baseline_scores.dropna().values,
            current_scores.dropna().values,
        )

        drifted  = p_value < self.KS_CRITICAL_P_VALUE
        severity = "CRITICAL" if drifted else "LOW"
        interp   = (
            f"KS statistic={ks_stat:.4f}, p-value={p_value:.6f}. "
            + ("Score distribution has shifted significantly — model review required."
               if drifted else "Score distribution stable.")
        )

        return DriftResult(
            model_id=model_id,
            feature="model_score",
            metric="KS",
            value=round(p_value, 8),
            threshold=self.KS_CRITICAL_P_VALUE,
            drifted=drifted,
            severity=severity,
            interpretation=interp,
            regulatory_ref="Federal Reserve SR 11-7 — Outcomes Analysis",
        )


# ---------------------------------------------------------------------------
# Fairness Checker (ECOA / Fair Lending)
# ---------------------------------------------------------------------------

class FairnessChecker:
    """
    Evaluates demographic parity and equal opportunity using the
    Adverse Impact Ratio (AIR) — the standard metric under ECOA and
    Regulation B for fair-lending compliance.

    AIR = (selection/approval rate for protected group) /
          (selection/approval rate for reference group)

    Threshold: AIR >= 0.80  (the "4/5ths rule" — EEOC / CFPB guidance)
    """

    AIR_THRESHOLD = 0.80

    def check_selection_rate(
        self,
        y_pred:           pd.Series,
        sensitive_feature: pd.Series,
        reference_group:  str,
        model_id:         str,
    ) -> list[FairnessResult]:
        if len(y_pred) != len(sensitive_feature):
            raise ValueError("y_pred and sensitive_feature must have the same length.")

        valid_mask = y_pred.notna() & sensitive_feature.notna()
        y_pred = y_pred[valid_mask]
        sensitive_feature = sensitive_feature[valid_mask]
        if y_pred.empty:
            raise ValueError("Fairness check requires at least one non-null prediction.")

        numeric_pred = pd.to_numeric(y_pred, errors="coerce")
        if numeric_pred.isna().any():
            raise ValueError("Fairness check requires binary numeric or boolean predictions.")
        if not numeric_pred.isin([0, 1]).all():
            raise ValueError("Fairness check expects predictions encoded as 0/1 or boolean values.")
        if reference_group not in set(sensitive_feature):
            raise ValueError(f"Reference group '{reference_group}' is not present in sensitive_feature.")

        results = []
        ref_rate = numeric_pred[sensitive_feature == reference_group].mean()
        if pd.isna(ref_rate):
            raise ValueError(f"Reference group '{reference_group}' has no valid predictions.")
        if ref_rate == 0:
            raise ValueError("Reference group selection rate is zero; AIR is not meaningful.")

        for group in sensitive_feature.unique():
            if group == reference_group:
                continue
            group_values = numeric_pred[sensitive_feature == group]
            if group_values.empty:
                continue
            group_rate = group_values.mean()
            air = group_rate / ref_rate if ref_rate > 0 else 0.0

            results.append(FairnessResult(
                model_id=model_id,
                metric="AIR_SELECTION_RATE",
                protected_group=str(group),
                reference_group=reference_group,
                group_rate=round(float(group_rate), 4),
                reference_rate=round(float(ref_rate), 4),
                air_ratio=round(float(air), 4),
                passed=bool(air >= self.AIR_THRESHOLD),
                details=(
                    f"Group '{group}' AIR={air:.4f} "
                    + ("≥" if air >= self.AIR_THRESHOLD else "<")
                    + f" threshold {self.AIR_THRESHOLD}"
                ),
            ))

        return results


# ---------------------------------------------------------------------------
# Model Governance Monitor — Orchestrates All Checks
# ---------------------------------------------------------------------------

class ModelGovernanceMonitor:
    """
    Orchestrates illustrative model-governance monitoring across drift
    and fairness checks, producing a machine-readable review report.

    Usage
    -----
    >>> monitor = ModelGovernanceMonitor(output_dir="audit_output/model_governance/")
    >>> report = monitor.run_full_assessment(
    ...     model_id="FRAUD_DETECT_V3",
    ...     model_version="3.2.1",
    ...     baseline_data=baseline_df,
    ...     current_data=current_df,
    ...     feature_columns=["fico_score", "dti_ratio", "credit_utilization"],
    ...     score_column="fraud_probability",
    ...     outcome_column="fraud_flag",
    ...     sensitive_column="race_ethnicity",
    ...     reference_group="WHITE_NON_HISPANIC",
    ...     reporting_date="2026-03-31",
    ... )
    >>> report.to_json(f"audit_output/model_governance/report_{report.report_id}.json")
    """

    def __init__(self, output_dir: str | Path = "audit_output/model_governance/") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.psi_calc    = PSICalculator()
        self.ks_monitor  = KSTestMonitor()
        self.fair_check  = FairnessChecker()

    def run_full_assessment(
        self,
        model_id:         str,
        model_version:    str,
        baseline_data:    pd.DataFrame,
        current_data:     pd.DataFrame,
        feature_columns:  list[str],
        score_column:     str,
        outcome_column:   str,
        sensitive_column: str,
        reference_group:  str,
        reporting_date:   str,
        monitoring_period: str = "MONTHLY",
    ) -> ModelGovernanceReport:

        drift_results:    list[DriftResult]    = []
        fairness_results: list[FairnessResult] = []

        # 1. PSI drift check per feature
        for feat in feature_columns:
            if feat in baseline_data.columns and feat in current_data.columns:
                result = self.psi_calc.calculate(
                    baseline=baseline_data[feat],
                    current=current_data[feat],
                    feature_name=feat,
                    model_id=model_id,
                )
                drift_results.append(result)

        # 2. KS test on model scores
        if score_column in baseline_data.columns and score_column in current_data.columns:
            ks_result = self.ks_monitor.calculate(
                baseline_scores=baseline_data[score_column],
                current_scores=current_data[score_column],
                model_id=model_id,
            )
            drift_results.append(ks_result)

        # 3. Fairness / AIR checks
        if sensitive_column in current_data.columns and outcome_column in current_data.columns:
            fairness_results = self.fair_check.check_selection_rate(
                y_pred=current_data[outcome_column],
                sensitive_feature=current_data[sensitive_column],
                reference_group=reference_group,
                model_id=model_id,
            )

        # 4. Placeholder explanation status for demo outputs
        explanations = {
            "status": "not_implemented",
            "note": "Explainability reporting is outside the scope of the current demo implementation.",
        }

        critical_drift   = any(r.drifted and r.severity == "CRITICAL" for r in drift_results)
        fairness_pass    = all(r.passed for r in fairness_results)
        ready_for_review = not critical_drift and fairness_pass

        report = ModelGovernanceReport(
            report_id         = f"{model_id}_{reporting_date.replace('-', '')}",
            model_id          = model_id,
            model_version     = model_version,
            reporting_date    = reporting_date,
            monitoring_period = monitoring_period,
            drift_results     = drift_results,
            fairness_results  = fairness_results,
            explanations      = explanations,
            ready_for_review  = ready_for_review,
        )

        self._log_report_summary(report)
        return report

    @staticmethod
    def _log_report_summary(report: ModelGovernanceReport) -> None:
        logger.info(
            "\n--- SR 11-7 Model Governance Report ---\n"
            "  Model ID        : %s v%s\n"
            "  Reporting Date  : %s\n"
            "  Drift Results   : %d checks (%d critical)\n"
            "  Fairness Results: %d checks (%d violations)\n"
            "  Overall Status  : %s\n",
            report.model_id, report.model_version,
            report.reporting_date,
            len(report.drift_results),
            sum(1 for r in report.drift_results if r.drifted and r.severity == "CRITICAL"),
            len(report.fairness_results),
            sum(1 for r in report.fairness_results if not r.passed),
            report.overall_status,
        )
