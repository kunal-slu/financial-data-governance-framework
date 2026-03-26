from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_output_sample_json_files_parse():
    sample_paths = [
        ROOT / "examples" / "output_samples" / "validation_summary.json",
        ROOT / "examples" / "output_samples" / "lineage_bundle.json",
        ROOT / "examples" / "output_samples" / "model_monitoring_summary.json",
    ]

    for path in sample_paths:
        payload = json.loads(path.read_text())
        assert isinstance(payload, dict)


def test_lineage_sample_has_expected_shape():
    payload = json.loads((ROOT / "examples" / "output_samples" / "lineage_bundle.json").read_text())
    assert payload["bundle_type"] == "LINEAGE_AUDIT"
    assert payload["inputs"]
    assert payload["outputs"]
    assert payload["transformations"]
    assert payload["inputs"][0]["dataset_fingerprint_method"]
    assert payload["outputs"][0]["dataset_fingerprint_method"]


def test_validation_sample_exposes_fingerprint_method():
    payload = json.loads((ROOT / "examples" / "output_samples" / "validation_summary.json").read_text())
    assert payload["framework_version"]
    assert payload["dataset_fingerprint"]
    assert payload["dataset_fingerprint_method"] == "schema_row_count"
    assert payload["dataset_fingerprint_scope"] == "structural"


def test_model_monitoring_sample_exposes_framework_and_fairness_skip_fields():
    payload = json.loads((ROOT / "examples" / "output_samples" / "model_monitoring_summary.json").read_text())
    assert payload["framework_version"]
    assert "has_fairness_violation" in payload
    assert "has_skipped_fairness" in payload
