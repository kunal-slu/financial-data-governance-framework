from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def test_framework_manifest_references_existing_assets():
    manifest = _load_yaml(ROOT / "assets" / "framework_manifest.yaml")

    for section in ("control_sets", "operating_profiles", "use_case_profiles"):
        for relative_path in manifest[section]:
            assert (ROOT / relative_path).exists(), relative_path


def test_use_case_profiles_have_required_fields():
    profile_dir = ROOT / "assets" / "use_case_profiles"
    profile_paths = sorted(profile_dir.glob("*.yaml"))

    assert profile_paths

    for path in profile_paths:
        profile = _load_yaml(path)
        assert profile["use_case_id"]
        assert profile["summary"]
        assert profile["applicable_institution_types"]
        assert profile["primary_modules"]
        assert profile["recommended_control_sets"]
