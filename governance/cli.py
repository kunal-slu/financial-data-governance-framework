"""Command-line entry points for FDGF."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from governance.data_quality.validators import RuleLoader
from governance.lineage.tracker import LineageTracker
from governance.reporting import ComplianceSummaryBuilder


def _load_demo_main():
    demo_script = Path(__file__).resolve().parents[1] / "examples" / "run_lightweight_demo.py"
    if not demo_script.exists():
        raise SystemExit(
            "fdgf demo is intended to run from a source checkout; "
            "examples/run_lightweight_demo.py was not found."
        )

    from examples.run_lightweight_demo import main as run_demo

    return run_demo


def _cmd_demo(_: argparse.Namespace) -> int:
    run_demo = _load_demo_main()
    run_demo()
    return 0


def _cmd_build_summary(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.input).read_text())
    builder = ComplianceSummaryBuilder()
    builder.write_markdown(payload, args.title, args.output)
    return 0


def _cmd_inspect_contract(args: argparse.Namespace) -> int:
    contract = Path(args.contract)
    rules = RuleLoader(contract).load()
    payload = {
        "contract": str(contract),
        "rule_count": len(rules),
        "rule_ids": [rule.get("id", "") for rule in rules],
    }
    if args.output:
        Path(args.output).write_text(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, indent=2))
    return 0


def _cmd_lineage(args: argparse.Namespace) -> int:
    config = json.loads(Path(args.config).read_text())
    tracker = LineageTracker(
        job_name=config["job_name"],
        job_namespace=config["job_namespace"],
        regulatory_scope=config["regulatory_scope"],
        output_dir=config["output_dir"],
    )
    tracker.start_run()
    for item in config.get("inputs", []):
        tracker.record_input(**item)
    for item in config.get("transformations", []):
        tracker.record_transformation(
            name=item["name"],
            transform_type=item["transform_type"],
            sql_or_code=item.get("sql_or_code", ""),
        )
    for item in config.get("outputs", []):
        tracker.record_output(**item)
    bundle_path = tracker.complete_run()
    print(bundle_path)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="fdgf")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_parser = subparsers.add_parser(
        "demo",
        help="Run the lightweight demo from a source checkout",
    )
    demo_parser.set_defaults(func=_cmd_demo)

    inspect_parser = subparsers.add_parser(
        "inspect-contract",
        help="Load and summarize a rule contract",
    )
    inspect_parser.add_argument("--contract", required=True, help="Path to YAML contract")
    inspect_parser.add_argument("--output", help="Optional JSON output path")
    inspect_parser.set_defaults(func=_cmd_inspect_contract)

    lineage_parser = subparsers.add_parser("lineage", help="Generate a lineage bundle from a JSON config")
    lineage_parser.add_argument("--config", required=True, help="Path to JSON config")
    lineage_parser.set_defaults(func=_cmd_lineage)

    summary_parser = subparsers.add_parser("summarize", help="Convert a JSON payload to markdown")
    summary_parser.add_argument("--input", required=True, help="Path to input JSON")
    summary_parser.add_argument("--title", required=True, help="Markdown title")
    summary_parser.add_argument("--output", required=True, help="Path to output markdown")
    summary_parser.set_defaults(func=_cmd_build_summary)

    compat_summary = subparsers.add_parser("build-summary", help=argparse.SUPPRESS)
    compat_summary.add_argument("--input", required=True, help="Path to input JSON")
    compat_summary.add_argument("--title", required=True, help="Markdown title")
    compat_summary.add_argument("--output", required=True, help="Path to output markdown")
    compat_summary.set_defaults(func=_cmd_build_summary)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
