from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from governance.lineage.tracker import LineageTracker
from governance.model_governance.drift_detector import PSICalculator, FairnessChecker

def main() -> None:
    data = pd.read_csv(ROOT / "sample_data" / "basel3_sample.csv")

    tracker = LineageTracker(
        job_name="fdgf_lightweight_demo",
        job_namespace="fdgf.examples",
        regulatory_scope="Illustrative financial data workflow",
        output_dir=ROOT / "demo_output" / "lineage",
    )
    tracker.start_run()
    tracker.record_input("basel3_sample.csv", "local://sample_data", "DEMO_SOURCE", record_count=len(data))
    tracker.record_transformation("exposure_rollup", "AGGREGATE", sql_or_code="sum(exposure_amount) by as_of_date")
    tracker.record_output("daily_rollup", "local://demo_output", "FDGF_DEMO", record_count=1)
    bundle_path = tracker.complete_run()

    baseline = pd.Series([600, 610, 615, 620, 630, 640, 645, 650])
    current = pd.Series([590, 595, 605, 610, 615, 620, 625, 630])
    psi_result = PSICalculator().calculate(baseline, current, "fico_score", "DEMO_MODEL")

    fairness = FairnessChecker().check_selection_rate(
        y_pred=pd.Series([1, 1, 1, 0, 1, 0, 1, 1]),
        sensitive_feature=pd.Series(["A", "A", "A", "A", "B", "B", "B", "B"]),
        reference_group="A",
        model_id="DEMO_MODEL",
    )

    out_dir = ROOT / "demo_output"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "summary.txt").write_text(
        f"Rows: {len(data)}\n"
        f"Lineage bundle: {bundle_path}\n"
        f"PSI: {psi_result.value}\n"
        f"Fairness checks: {len(fairness)}\n"
    )
    print("Demo completed. See demo_output/.")


if __name__ == "__main__":
    main()
