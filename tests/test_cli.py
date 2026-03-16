from __future__ import annotations

import json

from governance.cli import main


def test_build_summary_cli_writes_markdown(tmp_path, monkeypatch):
    input_path = tmp_path / "payload.json"
    output_path = tmp_path / "summary.md"
    input_path.write_text(json.dumps({"status": "ok", "count": 2}))

    monkeypatch.setattr(
        "sys.argv",
        [
            "fdgf",
            "build-summary",
            "--input",
            str(input_path),
            "--title",
            "Test Summary",
            "--output",
            str(output_path),
        ],
    )

    result = main()

    assert result == 0
    assert output_path.exists()
    assert "Test Summary" in output_path.read_text()


def test_inspect_contract_cli_prints_contract_summary(tmp_path, monkeypatch, capsys):
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        "rules:\n"
        "  - id: RULE-001\n"
        "    type: nullability\n"
        "    column: field_a\n"
        "    severity: CRITICAL\n"
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "fdgf",
            "inspect-contract",
            "--contract",
            str(contract_path),
        ],
    )

    result = main()

    assert result == 0
    assert "RULE-001" in capsys.readouterr().out
