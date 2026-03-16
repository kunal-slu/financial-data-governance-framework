"""
governance/pipelines/capital_workflow.py

Generic capital-exposure workflow built on the repository's reference pipeline.
This module keeps the implementation concrete while using broader naming for
institutions that need a reusable workflow label rather than a bank-specific one.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from governance.pipelines.basel3_pipeline import Basel3RWAPipeline


class CapitalExposureWorkflow(Basel3RWAPipeline):
    """
    Broadly named workflow for capital-exposure processing.

    The implementation currently uses the same underlying logic as the Basel III
    reference pipeline but is exposed through a neutral name so it can be
    adapted across different financial institutions.
    """

    PIPELINE_NAME = "capital_exposure_workflow"
    SCOPE = "Capital Exposure Controls"

    def __init__(
        self,
        spark: SparkSession,
        reporting_date: str,
        rules_path: str | Path = "assets/control_sets/capital_exposure_controls.yaml",
        output_path: str = "output/capital_exposure/",
        audit_path: str = "audit_output/",
    ) -> None:
        super().__init__(
            spark=spark,
            reporting_date=reporting_date,
            rules_path=rules_path,
            output_path=output_path,
            audit_path=audit_path,
        )
