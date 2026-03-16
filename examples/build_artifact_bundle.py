import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from governance.lineage.tracker import LineageTracker
from governance.model_governance.drift_detector import PSICalculator
from governance.reporting import ComplianceSummaryBuilder

def main() -> None:
    out_dir = ROOT / "demo_output" / "artifact_bundle"
    out_dir.mkdir(parents=True, exist_ok=True)

    tracker = LineageTracker(
        job_name="fdgf_submission_demo",
        job_namespace="fdgf.examples",
        regulatory_scope="Illustrative regulatory control workflow",
        output_dir=out_dir / "lineage",
    )
    tracker.start_run()
    tracker.record_input("basel3_sample.csv", "local://sample_data", "DEMO_SOURCE", record_count=4)
    tracker.record_transformation(
        "demo_rollup",
        "AGGREGATE",
        sql_or_code="sum(exposure_amount) by as_of_date",
    )
    tracker.record_output("rollup_output", "local://demo_output", "FDGF_DEMO", record_count=1)
    lineage_path = tracker.complete_run()

    baseline = pd.Series([600, 610, 615, 620, 630, 640, 645, 650])
    current = pd.Series([590, 595, 605, 610, 615, 620, 625, 630])
    psi = PSICalculator().calculate(baseline, current, "fico_score", "DEMO_MODEL")

    summary = ComplianceSummaryBuilder()
    payload = {
        "artifact_type": "artifact_bundle_summary",
        "lineage_bundle": str(lineage_path),
        "psi_metric": psi.value,
        "psi_drifted": psi.drifted,
        "project_positioning": (
            "reusable, regulator-aligned framework for governance-as-code in "
            "regulated financial data workflows"
        ),
    }
    summary.write_json(payload, out_dir / "summary.json")
    summary.write_markdown(payload, "Artifact Bundle Summary", out_dir / "summary.md")
    print("Artifact bundle written to", out_dir)


if __name__ == "__main__":
    main()
