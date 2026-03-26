from __future__ import annotations

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.errors import PySparkRuntimeError

from governance.data_quality.validators import RegulatoryDataValidator


@pytest.fixture(scope="module")
def spark():
    try:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("fdgf-integration-tests")
            .getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(f"PySpark integration tests skipped: {exc}")
    yield spark
    spark.stop()


def test_schema_match_rule_detects_missing_or_wrong_types(tmp_path, spark):
    rule_path = tmp_path / "schema_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: SCHEMA-001\n"
        "    type: schema_match\n"
        "    severity: CRITICAL\n"
        "    required_columns:\n"
        "      counterparty_id: string\n"
        "      exposure_amount: double\n"
    )

    df = spark.createDataFrame(
        [("CP1", "100.0")],
        "counterparty_id string, exposure_amount string",
    )

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration")

    result = bundle.results[0]
    assert result.rule_type.value == "schema_match"
    assert result.passed is False
    assert "expected double got string" in result.details


def test_row_condition_rule_detects_cross_field_failure(tmp_path, spark):
    rule_path = tmp_path / "row_condition_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: XFIELD-001\n"
        "    type: row_condition\n"
        "    severity: HIGH\n"
        "    condition_sql: \"CASE WHEN approach_type IN ('FIRB', 'AIRB') THEN pd_estimate IS NOT NULL ELSE TRUE END\"\n"
    )

    df = spark.createDataFrame(
        [("AIRB", None), ("SA", None)],
        "approach_type string, pd_estimate double",
    )

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration")

    result = bundle.results[0]
    assert result.rule_type.value == "row_condition"
    assert result.passed is False
    assert result.records_failed == 1


def test_row_condition_rule_renders_reporting_date_placeholder(tmp_path, spark):
    rule_path = tmp_path / "reporting_date_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: XFIELD-002\n"
        "    type: row_condition\n"
        "    severity: HIGH\n"
        "    condition_sql: \"reporting_date = '{{reporting_date}}'\"\n"
    )

    df = spark.createDataFrame(
        [("2026-03-31",), ("2026-03-30",)],
        "reporting_date string",
    )

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration")

    result = bundle.results[0]
    assert result.rule_type.value == "row_condition"
    assert result.passed is False
    assert result.records_failed == 1


def test_referential_integrity_uses_reference_dataset_join(tmp_path, spark):
    rule_path = tmp_path / "referential_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: REF-001\n"
        "    type: referential_integrity\n"
        "    column: counterparty_id\n"
        "    severity: CRITICAL\n"
    )

    df = spark.createDataFrame(
        [("CP1",), ("CP2",), ("CP3",)],
        "counterparty_id string",
    )
    reference_df = spark.createDataFrame(
        [("CP1",), ("CP2",)],
        "counterparty_id string",
    )

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration", reference_df=reference_df)

    result = bundle.results[0]
    assert result.rule_type.value == "referential_integrity"
    assert result.passed is False
    assert result.records_failed == 1


def test_timeliness_rule_uses_reporting_date_as_reference(tmp_path, spark):
    rule_path = tmp_path / "timeliness_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: TIME-001\n"
        "    type: timeliness\n"
        "    column: as_of_date\n"
        "    severity: HIGH\n"
        "    max_delay_days: 1\n"
    )

    df = spark.createDataFrame(
        [("2026-03-30",), ("2026-03-29",)],
        "as_of_date string",
    )

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration")

    result = bundle.results[0]
    assert result.rule_type.value == "timeliness"
    assert result.passed is False
    assert result.records_failed == 1


def test_timeliness_rule_uses_fixed_reporting_timestamp_for_max_lag_minutes(tmp_path, spark):
    rule_path = tmp_path / "timeliness_minutes_contract.yaml"
    rule_path.write_text(
        "rules:\n"
        "  - id: TIME-002\n"
        "    type: timeliness\n"
        "    column: updated_at\n"
        "    severity: HIGH\n"
        "    max_lag_minutes: 60\n"
    )

    df = spark.createDataFrame(
        [("2026-03-31 23:30:00",), ("2026-03-31 22:00:00",)],
        "updated_at string",
    ).selectExpr("to_timestamp(updated_at) as updated_at")

    validator = RegulatoryDataValidator(spark, rule_path)
    bundle = validator.validate(df, "2026-03-31", "integration")

    result = bundle.results[0]
    assert result.rule_type.value == "timeliness"
    assert result.passed is False
    assert result.records_failed == 1
