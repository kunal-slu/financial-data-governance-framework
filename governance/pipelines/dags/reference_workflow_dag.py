"""
governance/pipelines/dags/reference_workflow_dag.py

Reference Airflow DAG with broad naming for capital-exposure workflow controls.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


DEFAULT_ARGS = {
    "owner": "fdgf",
    "depends_on_past": False,
    "email": ["fdgf-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}


def validate_source_readiness(**context) -> None:
    reporting_date = context["ds"]
    print(f"Validating source readiness for {reporting_date}")


def run_control_checks(**context) -> None:
    from pyspark.sql import SparkSession

    from governance.data_quality.validators import RegulatoryDataValidator

    reporting_date = context["ds"]
    spark = SparkSession.builder.appName("fdgf-control-checks").getOrCreate()

    validator = RegulatoryDataValidator(
        spark=spark,
        rules_path="assets/control_sets/capital_exposure_controls.yaml",
    )

    exposure_df = spark.read.parquet(
        f"s3://fdgf-prod/raw/exposures/{reporting_date.replace('-', '')}/"
    )

    bundle = validator.validate(
        df=exposure_df,
        reporting_date=reporting_date,
        scope="Capital Exposure Controls",
    )

    bundle.to_json(
        f"s3://fdgf-prod/audit/validation/capital_controls_{reporting_date}.json"
    )

    if not bundle.submission_ready:
        raise ValueError(
            f"Control validation failed: {bundle.critical_failures} critical failures. "
            f"Pass rate: {bundle.pass_rate_pct}%."
        )

    context["ti"].xcom_push(key="control_pass_rate", value=bundle.pass_rate_pct)
    context["ti"].xcom_push(key="control_bundle_id", value=bundle.bundle_id)


def run_reference_workflow(**context) -> None:
    from pyspark.sql import SparkSession

    from governance.pipelines.capital_workflow import CapitalExposureWorkflow

    reporting_date = context["ds"]
    spark = SparkSession.builder.appName("fdgf-reference-workflow").getOrCreate()

    workflow = CapitalExposureWorkflow(
        spark=spark,
        reporting_date=reporting_date,
        output_path="s3://fdgf-prod/capital-exposure/reports/",
        audit_path="s3://fdgf-prod/audit/",
    )
    workflow.run(
        exposure_path=f"s3://fdgf-prod/raw/exposures/{reporting_date.replace('-', '')}/",
        gl_reference_path=f"s3://fdgf-prod/raw/gl_balances/{reporting_date.replace('-', '')}/",
    )


def compile_run_artifacts(**context) -> None:
    reporting_date = context["ds"]
    bundle_id = context["ti"].xcom_pull(key="control_bundle_id")
    print(f"Compiling run artifacts | reporting_date={reporting_date} | bundle={bundle_id}")


with DAG(
    dag_id="fdgf_reference_workflow_daily",
    description=(
        "Financial Data Governance Framework reference workflow. "
        "Validates, transforms, and records audit-oriented run artifacts."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fdgf", "controls", "reference-workflow"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("readiness") as readiness_group:
        check_sources = PythonOperator(
            task_id="validate_source_readiness",
            python_callable=validate_source_readiness,
        )

    with TaskGroup("controls") as controls_group:
        control_checks = PythonOperator(
            task_id="run_control_checks",
            python_callable=run_control_checks,
        )

    with TaskGroup("workflow") as workflow_group:
        execute_workflow = PythonOperator(
            task_id="run_reference_workflow",
            python_callable=run_reference_workflow,
        )

    with TaskGroup("artifacts") as artifact_group:
        compile_artifacts = PythonOperator(
            task_id="compile_run_artifacts",
            python_callable=compile_run_artifacts,
        )

    start >> readiness_group >> controls_group >> workflow_group >> artifact_group >> end
