from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def test_additional_contracts_load():
    contract_paths = [
        ROOT / "templates" / "data_contracts" / "liquidity_reporting_contract.yaml",
        ROOT / "templates" / "data_contracts" / "fdta_reporting_contract.yaml",
        ROOT / "templates" / "governance_policies" / "sr11_7_model_monitoring_policy.yaml",
        ROOT / "templates" / "control_mappings" / "regulatory_control_matrix.yaml",
    ]

    for path in contract_paths:
        payload = _load_yaml(path)
        assert payload


def test_control_mapping_has_entries():
    payload = _load_yaml(ROOT / "templates" / "control_mappings" / "regulatory_control_matrix.yaml")
    assert payload["framework"] == "FDGF"
    assert len(payload["control_mappings"]) >= 3
