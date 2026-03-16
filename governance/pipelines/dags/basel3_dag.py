"""
governance/pipelines/dags/basel3_dag.py

Apache Airflow DAG for Basel III RWA regulatory reporting pipeline.
Orchestrates ingestion, validation, transformation, and audit evidence
generation on a daily schedule aligned with BCBS 239 timeliness requirements.

Author: Kunal Kumar Singh
License: Apache 2.0
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# DAG Default Arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":            "kunal.singh.fdgf",
    "depends_on_past":  False,
    "email":            ["fdgf-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

# ---------------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------------

def validate_source_availability(**context) -> None:
    """Check that source data files are available before pipeline starts."""
    reporting_date = context["ds"]
    logger_msg     = f"Validating source availability for {reporting_date}"
    print(logger_msg)
    # In production: check S3/HDFS paths, raise AirflowSkipException if unavailable


def run_data_quality_checks(**context) -> None:
    """Execute BCBS 239 data quality validation rules."""
    from pyspark.sql import SparkSession
    from governance.data_quality.validators import RegulatoryDataValidator

    reporting_date = context["ds"]
    spark = SparkSession.builder.appName("fdgf-dq-checks").getOrCreate()

    validator = RegulatoryDataValidator(
        spark=spark,
        rules_path="templates/data_contracts/basel3_contract.yaml",
    )

    exposure_df = spark.read.parquet(
        f"s3://fdgf-prod/raw/exposures/{reporting_date.replace('-', '')}/"
    )

    bundle = validator.validate(
        df=exposure_df,
        reporting_date=reporting_date,
        scope="Basel III RWA",
    )

    bundle.to_json(f"s3://fdgf-prod/audit/validation/basel3_{reporting_date}.json")

    if not bundle.submission_ready:
        raise ValueError(
            f"Data quality validation failed: {bundle.critical_failures} critical failures. "
            f"Pass rate: {bundle.pass_rate_pct}%. Submission blocked."
        )

    # Push bundle stats to XCom for downstream tasks
    context["ti"].xcom_push(key="dq_pass_rate", value=bundle.pass_rate_pct)
    context["ti"].xcom_push(key="dq_bundle_id", value=bundle.bundle_id)


def run_rwa_calculation(**context) -> None:
    """Execute RWA transformation and capital calculation."""
    from pyspark.sql import SparkSession
    from governance.pipelines.basel3_pipeline import Basel3RWAPipeline

    reporting_date = context["ds"]
    spark = SparkSession.builder.appName("fdgf-rwa-calc").getOrCreate()

    pipeline = Basel3RWAPipeline(
        spark=spark,
        reporting_date=reporting_date,
        output_path="s3://fdgf-prod/basel3/reports/",
        audit_path="s3://fdgf-prod/audit/",
    )
    pipeline.run(
        exposure_path=f"s3://fdgf-prod/raw/exposures/{reporting_date.replace('-', '')}/",
        gl_reference_path=f"s3://fdgf-prod/raw/gl_balances/{reporting_date.replace('-', '')}/",
    )


def run_reconciliation_check(**context) -> None:
    """Cross-check RWA totals against GL and prior-period balances."""
    reporting_date = context["ds"]
    dq_pass_rate   = context["ti"].xcom_pull(key="dq_pass_rate")
    print(f"Reconciliation check for {reporting_date} | DQ pass rate: {dq_pass_rate}%")
    # In production: compare computed RWA totals against GL source of truth


def generate_audit_evidence(**context) -> None:
    """Compile final audit bundle for regulatory submission."""
    from governance.lineage.tracker import LineageTracker

    reporting_date = context["ds"]
    bundle_id      = context["ti"].xcom_pull(key="dq_bundle_id")
    print(f"Generating audit evidence | reporting_date={reporting_date} | bundle={bundle_id}")
    # In production: compile lineage + validation + model governance into a single submission package


def notify_submission_ready(**context) -> None:
    """Notify risk and compliance teams that the report is ready for regulatory submission."""
    reporting_date = context["ds"]
    print(f"✅ Basel III RWA report for {reporting_date} is SUBMISSION READY.")
    # In production: send email/Slack notification with audit bundle link


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="fdgf_basel3_rwa_daily",
    description=(
        "Financial Data Governance Framework — Basel III RWA daily pipeline. "
        "Validates, transforms, reconciles, and produces audit-ready regulatory data. "
        "Aligned with BCBS 239, SR 11-7, and FDTA requirements."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1-5",     # Weekdays at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["regulatory", "basel3", "fdgf", "bcbs239", "sr11-7"],
) as dag:

    start = EmptyOperator(task_id="pipeline_start")
    end   = EmptyOperator(task_id="pipeline_end")

    with TaskGroup("source_validation") as source_group:
        check_sources = PythonOperator(
            task_id="validate_source_availability",
            python_callable=validate_source_availability,
        )

    with TaskGroup("data_quality") as dq_group:
        dq_checks = PythonOperator(
            task_id="run_bcbs239_data_quality_checks",
            python_callable=run_data_quality_checks,
        )

    with TaskGroup("transformation") as transform_group:
        rwa_calc = PythonOperator(
            task_id="calculate_rwa_amounts",
            python_callable=run_rwa_calculation,
        )
        recon = PythonOperator(
            task_id="reconciliation_check",
            python_callable=run_reconciliation_check,
        )
        rwa_calc >> recon

    with TaskGroup("audit_and_submission") as audit_group:
        audit = PythonOperator(
            task_id="generate_audit_evidence",
            python_callable=generate_audit_evidence,
        )
        notify = PythonOperator(
            task_id="notify_submission_ready",
            python_callable=notify_submission_ready,
        )
        audit >> notify

    # Pipeline flow
    start >> source_group >> dq_group >> transform_group >> audit_group >> end
