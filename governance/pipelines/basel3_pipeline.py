"""
governance/pipelines/basel3_pipeline.py

Reference PySpark pipeline for Basel III RWA regulatory reporting.
Embeds governance-as-code controls (validation, lineage, audit) directly
into the data processing workflow — replacing manual reconciliation with
continuous, automated, review-ready evidence generation.

Regulatory alignment:
  - Basel III Capital Adequacy Framework
  - BCBS 239 Principles 2, 3, 4, 5
  - Federal Reserve SR 11-7
  - Financial Data Transparency Act (2022)

Author: Kunal Kumar Singh
License: Apache 2.0
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType, TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover - enables lightweight import/test mode
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore
    F = None  # type: ignore
    StructType = Any  # type: ignore
    PYSPARK_AVAILABLE = False

from governance.data_quality.validators import RegulatoryDataValidator, AuditBundle
from governance.lineage.tracker import LineageTracker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema Definitions (Schema Enforcement — BCBS 239 Principle 2)
# ---------------------------------------------------------------------------

if PYSPARK_AVAILABLE:
    EXPOSURE_SCHEMA = StructType([
        StructField("record_id",              StringType(),    False),
        StructField("counterparty_id",        StringType(),    False),
        StructField("facility_id",            StringType(),    False),
        StructField("lei_code",               StringType(),    True),
        StructField("asset_class",            StringType(),    False),
        StructField("approach_type",          StringType(),    False),
        StructField("exposure_amount",        DoubleType(),    False),
        StructField("risk_weight_pct",        DoubleType(),    False),
        StructField("pd_estimate",            DoubleType(),    True),
        StructField("lgd_estimate",           DoubleType(),    True),
        StructField("maturity_years",         DoubleType(),    True),
        StructField("currency_code",          StringType(),    True),
        StructField("reporting_date",         StringType(),    False),
        StructField("source_system_id",       StringType(),    False),
        StructField("ingestion_timestamp",    TimestampType(), False),
        StructField("last_updated_timestamp", TimestampType(), False),
    ])
else:  # pragma: no cover - used only when pyspark is absent
    EXPOSURE_SCHEMA = None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Basel3RWAPipeline:
    """
    End-to-end Basel III Risk-Weighted Asset (RWA) pipeline with
    integrated governance-as-code controls.

    Pipeline stages:
      1. Ingest — load raw exposure data with schema enforcement
      2. Validate — run BCBS 239 / Basel III data quality rules
      3. Transform — calculate RWA amounts and capital requirements
      4. Reconcile — cross-check totals against GL reference data
      5. Output — write reference report artifacts with audit evidence
      6. Audit — persist lineage, validation bundle, and audit trail

    Usage
    -----
    >>> pipeline = Basel3RWAPipeline(
    ...     spark=spark,
    ...     reporting_date="2026-03-31",
    ...     rules_path="templates/data_contracts/basel3_contract.yaml",
    ...     output_path="s3://fdgf-prod/basel3/reports/",
    ...     audit_path="s3://fdgf-prod/audit/",
    ... )
    >>> result = pipeline.run(
    ...     exposure_path="s3://fdgf-prod/raw/exposures/20260331/",
    ...     gl_reference_path="s3://fdgf-prod/raw/gl_balances/20260331/",
    ... )
    >>> assert result.critical_checks_passed, "Critical data quality failures must be resolved first."
    """

    PIPELINE_NAME = "basel3_rwa_pipeline"
    SCOPE         = "Basel III RWA"

    def __init__(
        self,
        spark:          SparkSession,
        reporting_date: str,
        rules_path:     str | Path = "templates/data_contracts/basel3_contract.yaml",
        output_path:    str = "output/basel3/",
        audit_path:     str = "audit_output/",
    ) -> None:
        self.spark          = spark
        self.reporting_date = reporting_date
        self.rules_path     = rules_path
        self.output_path    = output_path
        self.audit_path     = audit_path

        self.validator = RegulatoryDataValidator(spark, rules_path)
        self.tracker   = LineageTracker(
            job_name        = self.PIPELINE_NAME,
            job_namespace   = "fdgf.regulatory.basel3",
            regulatory_scope = self.SCOPE,
            output_dir      = f"{audit_path}/lineage/",
        )

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------

    def run(
        self,
        exposure_path:     str,
        gl_reference_path: Optional[str] = None,
    ) -> AuditBundle:
        """
        Execute the full Basel III RWA pipeline.
        Returns an AuditBundle for downstream review.
        """
        self._require_pyspark()
        run_id = self.tracker.start_run()
        logger.info(
            "Basel III RWA Pipeline | reporting_date=%s | run_id=%s",
            self.reporting_date, run_id
        )

        try:
            # Stage 1: Ingest
            exposure_df = self._ingest(exposure_path)
            exposure_count = exposure_df.count()
            self.tracker.record_input(
                name="raw_exposures",
                namespace=exposure_path,
                source_system="CORE_BANKING",
                record_count=exposure_count,
            )
            gl_df       = self._load_gl_reference(gl_reference_path) if gl_reference_path else None

            # Stage 2: Validate (governance-as-code — runs before any transformation)
            audit_bundle = self.validator.validate(
                df=exposure_df,
                reporting_date=self.reporting_date,
                scope=self.SCOPE,
                reference_df=gl_df,
            )
            audit_bundle.to_json(
                f"{self.audit_path}/validation/basel3_validation_{self.reporting_date}.json"
            )

            if audit_bundle.critical_failures > 0:
                logger.error(
                    "%d CRITICAL validation failures — pipeline halted pending review.",
                    audit_bundle.critical_failures,
                )
                self.tracker.fail_run(
                    error=f"{audit_bundle.critical_failures} critical data quality failures"
                )
                return audit_bundle

            # Stage 3: Transform
            rwa_df = self._calculate_rwa(exposure_df)
            capital_summary_df = self._calculate_capital_summary(rwa_df)
            self.tracker.record_transformation(
                name="rwa_calculation",
                transform_type="AGGREGATE",
                sql_or_code=(
                    "SELECT counterparty_id, asset_class, "
                    "SUM(exposure_amount * risk_weight_pct / 100) AS rwa_amount "
                    "FROM exposures GROUP BY counterparty_id, asset_class"
                ),
            )

            # Stage 4: Output
            self._write_output(rwa_df, capital_summary_df)
            output_count = rwa_df.count()
            self.tracker.record_output(
                name="basel3_rwa_report",
                namespace=self.output_path,
                source_system="FDGF_PIPELINE",
                record_count=output_count,
            )
            self.tracker.complete_run()

            logger.info(
                "Basel III RWA Pipeline COMPLETE | critical_checks_passed=%s",
                audit_bundle.critical_checks_passed
            )
            return audit_bundle

        except Exception as exc:
            self.tracker.fail_run(error=str(exc))
            logger.error("Pipeline failed: %s", exc, exc_info=True)
            raise

    @staticmethod
    def _require_pyspark() -> None:
        if not PYSPARK_AVAILABLE or F is None or EXPOSURE_SCHEMA is None:
            raise ImportError(
                "pyspark is required to execute the Basel III reference pipeline. "
                "Install requirements-full.txt for pipeline support."
            )

    # ------------------------------------------------------------------
    # Stage 1: Ingest
    # ------------------------------------------------------------------

    def _ingest(self, path: str) -> DataFrame:
        """
        Load raw exposure data with schema enforcement.
        Rejects records that don't conform to the regulatory schema.
        """
        logger.info("Ingesting exposure data from: %s", path)
        df = (
            self.spark.read
            .schema(EXPOSURE_SCHEMA)
            .option("mode", "FAILFAST")          # reject malformed records
            .option("badRecordsPath", f"{self.audit_path}/bad_records/")
            .parquet(path)
        )

        # Add governance metadata columns
        df = df.withColumn(
            "_fdgf_pipeline_run_ts", F.current_timestamp()
        ).withColumn(
            "_fdgf_reporting_date", F.lit(self.reporting_date)
        ).withColumn(
            "_fdgf_data_hash",
            F.sha2(F.concat_ws("|", *[F.col(c) for c in EXPOSURE_SCHEMA.fieldNames()]), 256)
        )

        record_count = df.count()
        logger.info("Ingested %d exposure records", record_count)
        return df

    def _load_gl_reference(self, path: str) -> DataFrame:
        logger.info("Loading GL reference data from: %s", path)
        return self.spark.read.parquet(path)

    # ------------------------------------------------------------------
    # Stage 3: Transform — RWA Calculation
    # ------------------------------------------------------------------

    def _calculate_rwa(self, df: DataFrame) -> DataFrame:
        """
        Calculate Risk-Weighted Assets per Basel III Standardized Approach.
        RWA = Exposure Amount × Risk Weight (%)
        """
        logger.info("Calculating RWA amounts")

        rwa_df = df.withColumn(
            "rwa_amount",
            F.round(F.col("exposure_amount") * F.col("risk_weight_pct") / 100.0, 2)
        ).withColumn(
            "capital_requirement",
            F.round(F.col("rwa_amount") * 0.08, 2)  # 8% minimum capital ratio
        ).withColumn(
            "calculation_method", F.lit("STANDARDIZED_APPROACH")
        ).withColumn(
            "calculation_timestamp", F.current_timestamp()
        )

        return rwa_df

    def _calculate_capital_summary(self, rwa_df: DataFrame) -> DataFrame:
        """Aggregate RWA and capital requirements by asset class."""
        return rwa_df.groupBy("asset_class", "approach_type").agg(
            F.count("record_id").alias("exposure_count"),
            F.sum("exposure_amount").alias("total_exposure"),
            F.sum("rwa_amount").alias("total_rwa"),
            F.sum("capital_requirement").alias("total_capital_requirement"),
            F.avg("risk_weight_pct").alias("avg_risk_weight_pct"),
            F.max("_fdgf_reporting_date").alias("reporting_date"),
        ).orderBy("total_rwa", ascending=False)

    # ------------------------------------------------------------------
    # Stage 4: Output
    # ------------------------------------------------------------------

    def _write_output(self, rwa_df: DataFrame, summary_df: DataFrame) -> None:
        """
        Write output in Delta Lake format with version control.
        Partitioned by reporting_date for efficient regulatory retrieval.
        """
        rwa_df = self._ensure_reporting_partition_column(rwa_df)
        rwa_path     = f"{self.output_path}/rwa_detail"
        summary_path = f"{self.output_path}/capital_summary"

        logger.info("Writing RWA detail to: %s", rwa_path)
        (
            rwa_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("reporting_date")
            .save(rwa_path)
        )

        logger.info("Writing capital summary to: %s", summary_path)
        (
            summary_df.write
            .format("delta")
            .mode("overwrite")
            .save(summary_path)
        )

    @staticmethod
    def _ensure_reporting_partition_column(df: DataFrame) -> DataFrame:
        """Ensure the detail output has a reporting_date partition column."""
        if "reporting_date" in df.columns:
            return df
        if "_fdgf_reporting_date" in df.columns:
            return df.withColumnRenamed("_fdgf_reporting_date", "reporting_date")
        raise ValueError(
            "RWA output requires 'reporting_date' or '_fdgf_reporting_date' before writing."
        )
