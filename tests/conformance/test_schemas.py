from __future__ import annotations

import json
from pathlib import Path

from jsonschema import validate


ROOT = Path(__file__).resolve().parents[2]


def test_validation_summary_conforms_to_audit_bundle_schema():
    schema = json.loads((ROOT / "schemas" / "audit_bundle.schema.json").read_text())
    payload = json.loads((ROOT / "examples" / "output_samples" / "validation_summary.json").read_text())
    validate(payload, schema)


def test_lineage_bundle_conforms_to_lineage_schema():
    schema = json.loads((ROOT / "schemas" / "lineage_event.schema.json").read_text())
    payload = json.loads((ROOT / "examples" / "output_samples" / "lineage_bundle.json").read_text())
    validate(payload, schema)
