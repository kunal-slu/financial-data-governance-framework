"""
governance/data_quality/validators.py

Core data quality validation engine for U.S. regulatory reporting.
Aligned with BCBS 239, SR 11-7, and Financial Data Transparency Act requirements.

Author: Kunal Kumar Singh
License: Apache 2.0
"""

from __future__ import annotations

import json
import logging
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from governance._version import FRAMEWORK_VERSION

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType
    PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover - enables lightweight import/test mode
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore
    StructType = Any  # type: ignore
    F = None  # type: ignore
    PYSPARK_AVAILABLE = False

logger = logging.getLogger(__name__)


class ContractValidationError(ValueError):
    """Raised when a YAML rule contract is malformed or internally inconsistent."""


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class RuleType(str, Enum):
    NULLABILITY = "nullability"
    RANGE = "range"
    REFERENTIAL = "referential_integrity"
    UNIQUENESS = "uniqueness"
    CROSS_DATASET = "cross_dataset_reconciliation"
    REGULATORY_FORMAT = "regulatory_format"
    TIMELINESS = "timeliness"
    ROW_CONDITION = "row_condition"
    SCHEMA_MATCH = "schema_match"


@dataclass
class ValidationResult:
    rule_id: str
    rule_type: RuleType
    column: str
    severity: Severity
    passed: bool
    records_checked: int
    records_failed: int
    failure_rate_pct: float
    details: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    regulatory_ref: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AuditBundle:
    bundle_id: str
    reporting_date: str
    regulatory_scope: str
    dataset_fingerprint: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    critical_failures: int
    results: list[ValidationResult]
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    framework_version: str = FRAMEWORK_VERSION
    dataset_fingerprint_method: str = "schema_row_count"

    @property
    def pass_rate_pct(self) -> float:
        if self.total_rules == 0:
            return 0.0
        return round(self.passed_rules / self.total_rules * 100, 2)

    @property
    def critical_checks_passed(self) -> bool:
        return self.critical_failures == 0

    def to_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            json.dump(
                {
                    **asdict(self),
                    "pass_rate_pct": self.pass_rate_pct,
                    "critical_checks_passed": self.critical_checks_passed,
                },
                fh,
                indent=2,
                default=str,
            )
        logger.info("Audit bundle written → %s", path)


class RuleLoader:
    def __init__(self, rule_path: str | Path) -> None:
        self.rule_path = Path(rule_path)
        self._rules: list[dict] = []

    def load(self) -> list[dict]:
        with open(self.rule_path) as fh:
            contract = yaml.safe_load(fh) or {}
        rules = contract.get("rules", [])
        if not isinstance(rules, list):
            raise ContractValidationError(
                f"Contract {self.rule_path} must contain a top-level 'rules' list."
            )
        self._rules = [self._validate_rule(rule, idx) for idx, rule in enumerate(rules, start=1)]
        logger.info("Loaded %d rules from %s", len(self._rules), self.rule_path)
        return self._rules

    @property
    def rules(self) -> list[dict]:
        if not self._rules:
            self.load()
        return self._rules

    @staticmethod
    def _validate_rule(rule: dict[str, Any], index: int) -> dict[str, Any]:
        if not isinstance(rule, dict):
            raise ContractValidationError(f"Rule #{index} must be a mapping.")

        rule_id = rule.get("id", f"rule_{index}")
        for field_name in ("id", "type", "severity"):
            if field_name not in rule:
                raise ContractValidationError(f"Rule {rule_id} is missing required field '{field_name}'.")

        try:
            rule_type = RuleType(rule["type"])
        except ValueError as exc:
            raise ContractValidationError(
                f"Rule {rule_id} has unknown rule type '{rule['type']}'."
            ) from exc

        try:
            Severity(rule["severity"])
        except ValueError as exc:
            raise ContractValidationError(
                f"Rule {rule_id} has invalid severity '{rule['severity']}'."
            ) from exc

        if rule_type in {RuleType.NULLABILITY, RuleType.RANGE, RuleType.REFERENTIAL,
                         RuleType.CROSS_DATASET, RuleType.REGULATORY_FORMAT, RuleType.TIMELINESS}:
            if "column" not in rule:
                raise ContractValidationError(f"Rule {rule_id} requires field 'column'.")

        if rule_type == RuleType.RANGE:
            minimum = rule.get("min_value", rule.get("min"))
            maximum = rule.get("max_value", rule.get("max"))
            if minimum is None and maximum is None:
                raise ContractValidationError(
                    f"Rule {rule_id} requires at least one of 'min', 'max', 'min_value', or 'max_value'."
                )
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ContractValidationError(
                    f"Rule {rule_id} has invalid range: min {minimum} is greater than max {maximum}."
                )

        if rule_type == RuleType.UNIQUENESS:
            column = rule.get("column")
            columns = rule.get("columns")
            if column is None and columns is None:
                raise ContractValidationError(
                    f"Rule {rule_id} requires either 'column' or 'columns'."
                )
            if columns is not None and (not isinstance(columns, list) or not columns):
                raise ContractValidationError(
                    f"Rule {rule_id} field 'columns' must be a non-empty list."
                )

        if rule_type == RuleType.REFERENTIAL:
            allowed_values = rule.get("allowed_values")
            if allowed_values is not None and (not isinstance(allowed_values, list) or not allowed_values):
                raise ContractValidationError(
                    f"Rule {rule_id} field 'allowed_values' must be a non-empty list when provided."
                )

        if rule_type == RuleType.REGULATORY_FORMAT:
            if rule.get("pattern") is None and rule.get("regex_pattern") is None:
                raise ContractValidationError(
                    f"Rule {rule_id} requires 'pattern' or 'regex_pattern'."
                )

        if rule_type == RuleType.TIMELINESS:
            if rule.get("max_delay_days") is None and rule.get("max_lag_minutes") is None:
                raise ContractValidationError(
                    f"Rule {rule_id} requires 'max_delay_days' or 'max_lag_minutes'."
                )

        if rule_type == RuleType.ROW_CONDITION:
            if rule.get("condition_sql") is None:
                raise ContractValidationError(f"Rule {rule_id} requires 'condition_sql'.")

        if rule_type == RuleType.SCHEMA_MATCH:
            required_columns = rule.get("required_columns")
            if not isinstance(required_columns, dict) or not required_columns:
                raise ContractValidationError(
                    f"Rule {rule_id} requires a non-empty 'required_columns' mapping."
                )
            for column_name, expected_type in required_columns.items():
                if not isinstance(column_name, str) or not column_name:
                    raise ContractValidationError(
                        f"Rule {rule_id} has an invalid required_columns entry '{column_name}'."
                    )
                if expected_type is not None and not isinstance(expected_type, str):
                    raise ContractValidationError(
                        f"Rule {rule_id} column '{column_name}' must map to a Spark type string or null."
                    )

        return rule


class RegulatoryDataValidator:
    """
    Executes governance-as-code validation against PySpark DataFrames.
    """

    def __init__(self, spark: SparkSession, rules_path: str | Path) -> None:
        self.spark = spark
        self.rule_loader = RuleLoader(rules_path)
        self.results: list[ValidationResult] = []

    def validate(
        self,
        df: DataFrame,
        reporting_date: str,
        scope: str,
        reference_df: DataFrame | None = None,
    ) -> AuditBundle:
        self._require_pyspark()
        self.results = []
        rules = self.rule_loader.rules
        persisted_df = df.persist()
        total_rows = persisted_df.count()
        dataset_fingerprint = self._fingerprint_dataframe(persisted_df, total_rows)

        try:
            for rule in rules:
                rule_type = RuleType(rule["type"])
                try:
                    result = self._dispatch(
                        rule,
                        rule_type,
                        persisted_df,
                        reference_df,
                        total_rows,
                        reporting_date,
                    )
                    self.results.append(result)
                except Exception as exc:
                    logger.error("Rule %s failed with exception: %s", rule.get("id"), exc)
                    self.results.append(
                        ValidationResult(
                            rule_id=rule.get("id", "unknown"),
                            rule_type=rule_type,
                            column=rule.get("column", ""),
                            severity=Severity(rule.get("severity", "HIGH")),
                            passed=False,
                            records_checked=0,
                            records_failed=0,
                            failure_rate_pct=0.0,
                            details=f"Rule execution error: {exc}",
                            regulatory_ref=rule.get("regulatory_ref", ""),
                        )
                    )
        finally:
            persisted_df.unpersist()

        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]
        critical = [r for r in failed if r.severity == Severity.CRITICAL]

        bundle = AuditBundle(
            bundle_id=self._generate_bundle_id(scope, reporting_date),
            reporting_date=reporting_date,
            regulatory_scope=scope,
            dataset_fingerprint=dataset_fingerprint,
            total_rules=len(self.results),
            passed_rules=len(passed),
            failed_rules=len(failed),
            critical_failures=len(critical),
            results=self.results,
        )

        self._log_summary(bundle)
        return bundle

    def _require_pyspark(self) -> None:
        if not PYSPARK_AVAILABLE or F is None:
            raise ImportError(
                "pyspark is required to execute dataframe validation checks. "
                "Install requirements-full.txt for full pipeline support."
            )

    def _dispatch(
        self,
        rule: dict,
        rule_type: RuleType,
        df: DataFrame,
        reference_df: DataFrame | None,
        total_rows: int,
        reporting_date: str,
    ) -> ValidationResult:
        dispatch_map = {
            RuleType.NULLABILITY: self._check_nullability,
            RuleType.RANGE: self._check_range,
            RuleType.UNIQUENESS: self._check_uniqueness,
            RuleType.REFERENTIAL: self._check_referential_integrity,
            RuleType.CROSS_DATASET: self._check_cross_dataset_reconciliation,
            RuleType.REGULATORY_FORMAT: self._check_regulatory_format,
            RuleType.TIMELINESS: self._check_timeliness,
            RuleType.ROW_CONDITION: self._check_row_condition,
            RuleType.SCHEMA_MATCH: self._check_schema_match,
        }
        return dispatch_map[rule_type](rule, df, reference_df, total_rows, reporting_date)

    def _check_nullability(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, __: str
    ) -> ValidationResult:
        column = rule["column"]
        failed = df.filter(F.col(column).isNull()).count()
        return self._result(rule, RuleType.NULLABILITY, column, total_rows, failed, f"Null check on {column}")

    def _check_range(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, __: str
    ) -> ValidationResult:
        column = rule["column"]
        minimum = rule.get("min_value", rule.get("min"))
        maximum = rule.get("max_value", rule.get("max"))

        condition = None
        if minimum is not None:
            condition = F.col(column) < F.lit(minimum)
        if maximum is not None:
            upper = F.col(column) > F.lit(maximum)
            condition = upper if condition is None else (condition | upper)

        failed = df.filter(condition).count() if condition is not None else 0
        return self._result(rule, RuleType.RANGE, column, total_rows, failed, f"Range check on {column}")

    def _check_uniqueness(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, __: str
    ) -> ValidationResult:
        columns = rule.get("columns")
        column = rule.get("column")
        if columns:
            select_columns = columns
            label = ", ".join(columns)
        elif column:
            select_columns = [column]
            label = column
        else:
            raise ValueError("uniqueness rule requires 'column' or 'columns'")
        distinct_count = df.select(*select_columns).distinct().count()
        failed = total_rows - distinct_count
        return self._result(rule, RuleType.UNIQUENESS, label, total_rows, failed, f"Uniqueness check on {label}")

    def _check_referential_integrity(
        self, rule: dict, df: DataFrame, reference_df: DataFrame | None, total_rows: int, __: str
    ) -> ValidationResult:
        column = rule["column"]
        allowed_values = rule.get("allowed_values")
        if allowed_values is not None:
            failed = df.filter(~F.col(column).isin(allowed_values)).count()
            return self._result(
                rule,
                RuleType.REFERENTIAL,
                column,
                total_rows,
                failed,
                f"Allowed values check on {column}",
            )

        if reference_df is None:
            raise ValueError("reference_df is required for referential integrity checks")

        reference_column = rule.get("reference_column", column)
        failed = (
            df.join(
                reference_df.select(F.col(reference_column).alias("_ref_key")).distinct(),
                df[column] == F.col("_ref_key"),
                "left",
            )
            .filter(F.col("_ref_key").isNull())
            .count()
        )

        return self._result(
            rule,
            RuleType.REFERENTIAL,
            column,
            total_rows,
            failed,
            f"Referential integrity check on {column}",
        )

    def _check_cross_dataset_reconciliation(
        self, rule: dict, df: DataFrame, reference_df: DataFrame | None, _: int, __: str
    ) -> ValidationResult:
        if reference_df is None:
            raise ValueError("reference_df is required for reconciliation checks")

        column = rule["column"]
        tolerance = float(rule.get("tolerance", rule.get("tolerance_pct", 0.0)))

        source_sum = df.agg(F.sum(F.col(column)).alias("s")).collect()[0]["s"] or 0.0
        ref_sum = reference_df.agg(F.sum(F.col(column)).alias("s")).collect()[0]["s"] or 0.0
        diff = abs(float(source_sum) - float(ref_sum))
        failed = 0 if diff <= tolerance else 1

        return ValidationResult(
            rule_id=rule["id"],
            rule_type=RuleType.CROSS_DATASET,
            column=column,
            severity=Severity(rule.get("severity", "HIGH")),
            passed=(failed == 0),
            records_checked=1,
            records_failed=failed,
            failure_rate_pct=0.0 if failed == 0 else 100.0,
            details=f"Reconciliation diff={diff}, tolerance={tolerance}",
            regulatory_ref=rule.get("regulatory_ref", ""),
        )

    def _check_regulatory_format(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, __: str
    ) -> ValidationResult:
        column = rule["column"]
        pattern = rule.get("pattern", rule.get("regex_pattern"))
        if pattern is None:
            raise ValueError("regulatory_format rule requires 'pattern' or 'regex_pattern'")
        failed = df.filter(~F.col(column).rlike(pattern)).count()
        return self._result(rule, RuleType.REGULATORY_FORMAT, column, total_rows, failed, f"Format check on {column}")

    def _check_timeliness(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, reporting_date: str
    ) -> ValidationResult:
        column = rule["column"]
        max_delay_days = rule.get("max_delay_days")
        max_lag_minutes = rule.get("max_lag_minutes")
        if max_lag_minutes is not None:
            reference_timestamp = self._timeliness_reference_timestamp(reporting_date)
            cutoff_expr = F.expr(
                f"to_timestamp('{reference_timestamp}') - INTERVAL {int(max_lag_minutes)} MINUTES"
            )
            failed = df.filter(F.col(column) < cutoff_expr).count()
        else:
            max_delay_days = int(max_delay_days or 0)
            evaluation_date = F.to_date(F.lit(self._timeliness_reference_date(reporting_date)))
            failed = df.filter(
                F.datediff(evaluation_date, F.to_date(F.col(column))) > max_delay_days
            ).count()
        return self._result(rule, RuleType.TIMELINESS, column, total_rows, failed, f"Timeliness check on {column}")

    def _check_row_condition(
        self, rule: dict, df: DataFrame, _: DataFrame | None, total_rows: int, reporting_date: str
    ) -> ValidationResult:
        condition_sql = self._render_rule_template(rule["condition_sql"], reporting_date)
        label = rule.get("description", rule["id"])
        failed = df.filter(~F.coalesce(F.expr(condition_sql).cast("boolean"), F.lit(False))).count()
        return self._result(rule, RuleType.ROW_CONDITION, label, total_rows, failed, f"Row condition check: {condition_sql}")

    def _check_schema_match(
        self, rule: dict, df: DataFrame, _: DataFrame | None, __: int, ___: str
    ) -> ValidationResult:
        schema_fields = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        required_columns = rule["required_columns"]
        failures: list[str] = []
        for column, expected_type in required_columns.items():
            if column not in schema_fields:
                failures.append(f"missing:{column}")
                continue
            if expected_type is not None and schema_fields[column] != str(expected_type):
                failures.append(f"type:{column} expected {expected_type} got {schema_fields[column]}")

        checked = len(required_columns)
        failed = len(failures)
        return ValidationResult(
            rule_id=rule["id"],
            rule_type=RuleType.SCHEMA_MATCH,
            column="schema",
            severity=Severity(rule.get("severity", "HIGH")),
            passed=(failed == 0),
            records_checked=checked,
            records_failed=failed,
            failure_rate_pct=round((failed / checked * 100), 2) if checked else 0.0,
            details="; ".join(failures) if failures else "Schema matches expected columns and types.",
            regulatory_ref=rule.get("regulatory_ref", ""),
        )

    def _result(
        self,
        rule: dict,
        rule_type: RuleType,
        column: str,
        total: int,
        failed: int,
        details: str,
    ) -> ValidationResult:
        rate = round((failed / total * 100), 2) if total else 0.0
        return ValidationResult(
            rule_id=rule["id"],
            rule_type=rule_type,
            column=column,
            severity=Severity(rule.get("severity", "HIGH")),
            passed=(failed == 0),
            records_checked=total,
            records_failed=failed,
            failure_rate_pct=rate,
            details=details,
            regulatory_ref=rule.get("regulatory_ref", ""),
        )

    def _fingerprint_dataframe(self, df: DataFrame, total_rows: int) -> str:
        """Return a lightweight, deterministic dataset fingerprint.

        This intentionally reflects dataframe shape rather than full row content.
        """
        schema_value = df.schema.jsonValue() if hasattr(df.schema, "jsonValue") else str(df.schema)
        raw = json.dumps(
            {
                "schema": schema_value,
                "row_count": total_rows,
            },
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _generate_bundle_id(self, scope: str, reporting_date: str) -> str:
        raw = f"{scope}|{reporting_date}|{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _timeliness_reference_date(reporting_date: str) -> str:
        return reporting_date

    @staticmethod
    def _timeliness_reference_timestamp(reporting_date: str) -> str:
        return f"{reporting_date} 23:59:59"

    @staticmethod
    def _render_rule_template(value: str, reporting_date: str) -> str:
        return value.replace("{{reporting_date}}", reporting_date)

    def _log_summary(self, bundle: AuditBundle) -> None:
        logger.info(
            "Validation complete: %s | pass_rate=%s%% | critical_failures=%s | critical_checks_passed=%s",
            bundle.bundle_id,
            bundle.pass_rate_pct,
            bundle.critical_failures,
            bundle.critical_checks_passed,
        )
