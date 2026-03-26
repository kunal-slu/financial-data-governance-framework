from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from governance.pipelines.basel3_pipeline import Basel3RWAPipeline


def _make_pipeline() -> Basel3RWAPipeline:
    return Basel3RWAPipeline(
        spark=MagicMock(),
        reporting_date="2026-03-31",
        output_path="output/basel3",
        audit_path="audit_output",
    )


def test_ensure_reporting_partition_column_keeps_existing_column():
    pipeline = _make_pipeline()
    df = MagicMock()
    df.columns = ["reporting_date", "_fdgf_reporting_date"]

    result = pipeline._ensure_reporting_partition_column(df)

    assert result is df
    df.withColumn.assert_not_called()


def test_ensure_reporting_partition_column_uses_framework_metadata_column():
    pipeline = _make_pipeline()
    df = MagicMock()
    normalized_df = MagicMock()
    df.columns = ["_fdgf_reporting_date"]
    df.withColumnRenamed.return_value = normalized_df

    result = pipeline._ensure_reporting_partition_column(df)

    assert result is normalized_df
    df.withColumnRenamed.assert_called_once_with("_fdgf_reporting_date", "reporting_date")


def test_ensure_reporting_partition_column_raises_if_missing():
    pipeline = _make_pipeline()
    df = MagicMock()
    df.columns = ["counterparty_id", "exposure_amount"]

    with pytest.raises(ValueError, match="reporting_date"):
        pipeline._ensure_reporting_partition_column(df)


def test_write_output_partitions_after_normalizing_reporting_date():
    pipeline = _make_pipeline()
    rwa_df = MagicMock()
    normalized_rwa_df = MagicMock()
    summary_df = MagicMock()

    rwa_writer = MagicMock()
    rwa_writer.format.return_value = rwa_writer
    rwa_writer.mode.return_value = rwa_writer
    rwa_writer.option.return_value = rwa_writer
    rwa_writer.partitionBy.return_value = rwa_writer
    normalized_rwa_df.write = rwa_writer

    summary_writer = MagicMock()
    summary_writer.format.return_value = summary_writer
    summary_writer.mode.return_value = summary_writer
    summary_df.write = summary_writer

    pipeline._ensure_reporting_partition_column = MagicMock(return_value=normalized_rwa_df)

    pipeline._write_output(rwa_df, summary_df)

    pipeline._ensure_reporting_partition_column.assert_called_once_with(rwa_df)
    rwa_writer.partitionBy.assert_called_once_with("reporting_date")
