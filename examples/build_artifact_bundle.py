import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from governance.data_quality.validators import AuditBundle, RuleLoader
from governance.lineage.tracker import LineageTracker
from governance.model_governance.drift_detector import ModelGovernanceMonitor
from governance.reporting import ComplianceSummaryBuilder


def _write_lineage_bundle(out_dir: Path) -> Path:
    lineage_work_dir = out_dir / "_lineage_run"
    tracker = LineageTracker(
        job_name="fdgf_artifact_bundle_reference",
        job_namespace="fdgf.examples",
        regulatory_scope="Regulatory control reference workflow",
        output_dir=lineage_work_dir,
    )
    sample_rows = len(pd.read_csv(ROOT / "sample_data" / "basel3_sample.csv"))
    tracker.start_run()
    tracker.record_input("basel3_sample.csv", "local://sample_data", "REFERENCE_SOURCE", record_count=sample_rows)
    tracker.record_transformation(
        "reference_rollup",
        "AGGREGATE",
        sql_or_code="sum(exposure_amount) by reporting_date",
    )
    tracker.record_output("daily_rollup", "local://artifact_bundle", "FDGF_REFERENCE", record_count=1)
    lineage_path = Path(tracker.complete_run())
    final_path = out_dir / "lineage_bundle.json"
    shutil.copyfile(lineage_path, final_path)
    shutil.rmtree(lineage_work_dir)
    return final_path


def _write_validation_summary(out_dir: Path, summary: ComplianceSummaryBuilder) -> Path:
    contract_rules = RuleLoader(ROOT / "templates" / "data_contracts" / "basel3_contract.yaml").load()
    bundle = AuditBundle(
        bundle_id="BUNDLE-DEMO-001",
        reporting_date="2026-03-31",
        regulatory_scope="Basel III RWA",
        dataset_fingerprint="illustrative-sample",
        total_rules=len(contract_rules),
        passed_rules=len(contract_rules),
        failed_rules=0,
        critical_failures=0,
        results=[],
    )
    payload = summary.build_validation_summary(bundle)
    summary.write_json(payload, out_dir / "validation_summary.json")
    summary.write_markdown(payload, "Validation Summary", out_dir / "validation_summary.md")
    return out_dir / "validation_summary.json"


def _write_model_summary(out_dir: Path, summary: ComplianceSummaryBuilder) -> Path:
    monitor = ModelGovernanceMonitor(output_dir=out_dir / "_model_governance")
    baseline_data = pd.DataFrame(
        {
            "fico_score": [600, 610, 615, 620, 630, 640, 645, 650],
            "model_score": [0.22, 0.20, 0.19, 0.18, 0.16, 0.15, 0.14, 0.13],
        }
    )
    current_data = pd.DataFrame(
        {
            "fico_score": [590, 595, 605, 610, 615, 620, 625, 630],
            "model_score": [0.24, 0.23, 0.21, 0.20, 0.19, 0.18, 0.17, 0.16],
            "decision_flag": [1, 1, 1, 0, 1, 0, 1, 1],
            "segment": ["A", "A", "A", "A", "B", "B", "B", "B"],
        }
    )
    report = monitor.run_full_assessment(
        model_id="REFERENCE_MODEL",
        model_version="1.0.0",
        baseline_data=baseline_data,
        current_data=current_data,
        feature_columns=["fico_score"],
        score_column="model_score",
        decision_column="decision_flag",
        sensitive_column="segment",
        reference_group="A",
        reporting_date="2026-03-31",
    )
    payload = summary.build_model_summary(report)
    summary.write_json(payload, out_dir / "model_monitoring_summary.json")
    shutil.rmtree(out_dir / "_model_governance")
    return out_dir / "model_monitoring_summary.json"


def _write_artifact_packet(out_dir: Path, summary: ComplianceSummaryBuilder) -> None:
    payload = {
        "artifact_type": "artifact_bundle_summary",
        "lineage_bundle": "lineage_bundle.json",
        "validation_summary": "validation_summary.json",
        "model_monitoring_summary": "model_monitoring_summary.json",
        "project_positioning": (
            "open, vendor-neutral governance-as-code reference implementation for "
            "regulated financial data workflows"
        ),
    }
    summary.write_markdown(payload, "Artifact Bundle Summary", out_dir / "compliance_summary.md")
    (out_dir / "audit_packet.md").write_text(
        "# Sample Audit Packet\n\n"
        "This sample packet shows how synthetic output artifacts can be assembled for review.\n\n"
        "Included artifacts:\n\n"
        "- validation summary: `validation_summary.json`\n"
        "- lineage bundle: `lineage_bundle.json`\n"
        "- compliance summary: `compliance_summary.md`\n"
        "- model monitoring summary: `model_monitoring_summary.json`\n"
    )


def main(output_dir: Path | None = None) -> None:
    out_dir = output_dir or (ROOT / "demo_output" / "artifact_bundle")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = ComplianceSummaryBuilder()
    _write_lineage_bundle(out_dir)
    _write_validation_summary(out_dir, summary)
    _write_model_summary(out_dir, summary)
    _write_artifact_packet(out_dir, summary)

    print("Artifact bundle written to", out_dir)


if __name__ == "__main__":
    main()
