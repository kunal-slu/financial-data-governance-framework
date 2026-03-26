from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from governance.pipelines.basel3_pipeline import Basel3RWAPipeline


def _make_pipeline() -> Basel3RWAPipeline:
    return Basel3RWAPipeline(
        spark=MagicMock(),
        reporting_date="2026-03-31",
        output_path="output/basel3",
        audit_path="audit_output",
    )


def test_ensure_reporting_date_column_keeps_existing_column():
    pipeline = _make_pipeline()
    df = MagicMock()
    df.columns = ["reporting_date", "_fdgf_reporting_date"]

    result = pipeline._ensure_reporting_date_column(df)

    assert result is df
    df.withColumn.assert_not_called()


def test_ensure_reporting_date_column_uses_framework_metadata_column():
    pipeline = _make_pipeline()
    df = MagicMock()
    normalized_df = MagicMock()
    df.columns = ["_fdgf_reporting_date"]
    df.withColumnRenamed.return_value = normalized_df

    result = pipeline._ensure_reporting_date_column(df)

    assert result is normalized_df
    df.withColumnRenamed.assert_called_once_with("_fdgf_reporting_date", "reporting_date")


def test_ensure_reporting_date_column_raises_if_missing():
    pipeline = _make_pipeline()
    df = MagicMock()
    df.columns = ["counterparty_id", "exposure_amount"]

    with pytest.raises(ValueError, match="reporting_date"):
        pipeline._ensure_reporting_date_column(df)

def test_write_output_uses_date_scoped_paths_after_normalizing_reporting_date():
    pipeline = _make_pipeline()
    rwa_df = MagicMock()
    normalized_rwa_df = MagicMock()
    summary_df = MagicMock()
    normalized_summary_df = MagicMock()

    rwa_writer = MagicMock()
    rwa_writer.format.return_value = rwa_writer
    rwa_writer.mode.return_value = rwa_writer
    rwa_writer.option.return_value = rwa_writer
    rwa_writer.partitionBy.return_value = rwa_writer
    normalized_rwa_df.write = rwa_writer

    summary_writer = MagicMock()
    summary_writer.format.return_value = summary_writer
    summary_writer.mode.return_value = summary_writer
    summary_writer.option.return_value = summary_writer
    normalized_summary_df.write = summary_writer

    pipeline._ensure_reporting_date_column = MagicMock(
        side_effect=[normalized_rwa_df, normalized_summary_df]
    )

    pipeline._write_output(rwa_df, summary_df)

    assert pipeline._ensure_reporting_date_column.mock_calls == [
        call(rwa_df),
        call(summary_df),
    ]
    rwa_writer.partitionBy.assert_not_called()
    rwa_writer.save.assert_called_once_with(f"{pipeline.output_path}/rwa_detail/{pipeline.reporting_date}")
    summary_writer.option.assert_called_once_with("overwriteSchema", "true")
    summary_writer.save.assert_called_once_with(f"{pipeline.output_path}/capital_summary/{pipeline.reporting_date}")


def test_run_records_lineage_as_stages_succeed():
    pipeline = _make_pipeline()
    exposure_df = MagicMock()
    rwa_df = MagicMock()
    summary_df = MagicMock()
    audit_bundle = MagicMock()
    audit_bundle.critical_failures = 0
    audit_bundle.critical_checks_passed = True
    exposure_df.count.return_value = 12
    rwa_df.count.return_value = 12

    pipeline._require_pyspark = MagicMock()
    pipeline._ingest = MagicMock(return_value=exposure_df)
    pipeline._load_gl_reference = MagicMock(return_value=None)
    pipeline.validator.validate = MagicMock(return_value=audit_bundle)
    pipeline._calculate_rwa = MagicMock(return_value=rwa_df)
    pipeline._calculate_capital_summary = MagicMock(return_value=summary_df)
    pipeline._write_output = MagicMock()
    pipeline.tracker = MagicMock()
    pipeline.tracker.start_run.return_value = "run-123"

    result = pipeline.run("raw/exposures")

    assert result is audit_bundle
    pipeline._write_output.assert_called_once_with(rwa_df, summary_df)
    assert pipeline.tracker.mock_calls == [
        call.start_run(),
        call.record_input(
            name="raw_exposures",
            namespace="raw/exposures",
            source_system="CORE_BANKING",
            record_count=12,
        ),
        call.record_transformation(
            name="rwa_calculation",
            transform_type="AGGREGATE",
            sql_or_code=(
                "SELECT counterparty_id, asset_class, "
                "SUM(exposure_amount * risk_weight_pct / 100) AS rwa_amount "
                "FROM exposures GROUP BY counterparty_id, asset_class"
            ),
        ),
        call.record_output(
            name="basel3_rwa_report",
            namespace=pipeline.output_path,
            source_system="FDGF_PIPELINE",
            record_count=12,
        ),
        call.complete_run(),
    ]


def test_run_preserves_input_lineage_when_validation_halts_pipeline():
    pipeline = _make_pipeline()
    exposure_df = MagicMock()
    audit_bundle = MagicMock()
    audit_bundle.critical_failures = 2
    exposure_df.count.return_value = 7

    pipeline._require_pyspark = MagicMock()
    pipeline._ingest = MagicMock(return_value=exposure_df)
    pipeline._load_gl_reference = MagicMock(return_value=None)
    pipeline.validator.validate = MagicMock(return_value=audit_bundle)
    pipeline.tracker = MagicMock()
    pipeline.tracker.start_run.return_value = "run-123"

    result = pipeline.run("raw/exposures")

    assert result is audit_bundle
    assert pipeline.tracker.mock_calls == [
        call.start_run(),
        call.record_input(
            name="raw_exposures",
            namespace="raw/exposures",
            source_system="CORE_BANKING",
            record_count=7,
        ),
        call.fail_run(error="2 critical data quality failures"),
    ]
