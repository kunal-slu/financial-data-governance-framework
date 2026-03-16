![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

# Financial Data Governance Framework (FDGF)

FDGF is a public technical framework for regulator-aligned financial data governance. It includes working examples of data-quality validation, lineage capture, model-monitoring checks, and audit-oriented pipeline components that can be adapted across regulated financial institutions, with the most concrete examples centered on U.S. regulatory reporting and risk-governance workflows.

## Repository Layout

- `governance/data_quality/validators.py`
  A validation engine that loads YAML rule sets and produces machine-readable audit bundles.
- `governance/lineage/tracker.py`
  A lineage tracker that records inputs, outputs, transformations, and run events.
- `governance/model_governance/drift_detector.py`
  Drift and fairness monitoring utilities for model-governance workflows.
- `governance/pipelines/basel3_pipeline.py`
  A Basel III-oriented reference pipeline showing how validation and lineage can be embedded into data processing.
- `governance/pipelines/capital_workflow.py`
  A broader workflow label for capital-exposure processing built on the same reference implementation.
- `governance/reporting/compliance_summary.py`
  Utilities for turning raw governance artifacts into concise JSON and Markdown summaries.
- `governance/cli.py`
  A lightweight CLI entry point for demos, contract inspection, lineage generation, and summary-generation workflows.
- `templates/data_contracts/`
  Example rule contracts for generic financial records as well as Basel III and CCAR-style datasets.
- `templates/control_mappings/`
  A simple control matrix mapping repository components to supervisory control themes.
- `templates/institution_profiles/`
  Example institution profiles showing how the framework can be adapted for different types of financial firms.
- `assets/control_sets/`
  Broadly named control sets intended for reuse across financial institutions.
- `assets/operating_profiles/`
  Broad operating profiles that map the framework to institution types.
- `assets/use_case_profiles/`
  Workflow-oriented profiles that map the framework to common governance scenarios.
- `docs/architecture.md`
  An architectural walkthrough of how contracts, controls, lineage, monitoring, and workflows fit together.
- `docs/control_catalog.md`
  A catalog of the control patterns currently implemented in the repository.
- `docs/control_mapping.md`
  A readable summary of module-to-control alignment.
- `docs/use_cases.md`
  Multi-workflow reuse scenarios across regulated data processes.
- `docs/problem_statement.md`
  A concise description of the operational failures the framework targets.
- `docs/failure_modes.md`
  A more detailed breakdown of recurring control and audit failure patterns.
- `docs/benchmark_report.md`
  An illustrative comparison between fragmented manual controls and generated governance artifacts.
- `docs/quickstart.md`
  A minimal setup and usage path for local evaluation.
- `docs/demo_and_integration_patterns.md`
  Lightweight local-demo and integration patterns.
- `docs/adr/`
  Architecture decision records describing core framework choices.
- `examples/run_lightweight_demo.py`
  A lightweight local demonstration that does not require a full Spark pipeline.
- `examples/build_artifact_bundle.py`
  A small example showing how generated artifacts can be summarized for review.
- `examples/output_samples/`
  Stable sample artifacts showing the kinds of outputs the framework produces.
- `schemas/`
  JSON schemas for selected generated artifact types.
- `tests/`
  Unit tests covering the core repository behavior.

## Problem Statement

Many U.S. financial institutions still prepare regulatory and risk-governance data through a mix of spreadsheets, ad hoc SQL, manual reconciliations, and fragmented audit documentation. This repository illustrates a software-based alternative: express validation rules, lineage records, and monitoring checks directly in reusable code and configuration.

The repository is designed for recurring control needs seen in:
- banks and credit unions
- regional and community-bank reporting teams
- broker-dealers and other regulated financial platforms with traceable reporting obligations

Examples in the repository are tied to familiar control domains such as:
- Basel III and CCAR reporting environments
- BCBS 239 data aggregation and risk reporting
- SR 11-7 model-risk governance
- machine-readable and traceable reporting data workflows

## Current Status

- The repository contains working code, example contracts, sample data, tests, and a lightweight local demo.
- The repository includes manifest-driven control, operating-profile, and use-case assets intended for reuse across multiple institution and workflow types.
- The lightweight demo generates example lineage output and a summary file in a local ignored output directory.
- The unit tests cover the validator, lineage, and monitoring components.
- The Spark-oriented pipeline code is included as reference code, but full local execution depends on a compatible Spark and PySpark environment.
- Domain-specific examples are included for bank reporting and model-governance use cases, while the generic templates are intended to be adapted to other regulated financial data workflows.
- FDGF is currently intended to be used from a source checkout so that templates, schemas, sample data, examples, and documentation remain available alongside the Python modules.

## Limits

- It does not claim regulator endorsement.
- It does not claim broad production deployment across financial institutions.
- It does not claim that the repository by itself satisfies supervisory or internal control requirements.

## Running the Repository

Install the dependencies:

```bash
python3 -m pip install -r requirements.txt
```

For Spark and orchestration dependencies:

```bash
python3 -m pip install -r requirements-full.txt
```

Run the test suite:

```bash
python3 -m pytest -q
```

Run the lightweight demo:

```bash
python3 examples/run_lightweight_demo.py
```

Run the demo through the CLI:

```bash
python3 -m governance.cli demo
```

Inspect a contract through the CLI:

```bash
python3 -m governance.cli inspect-contract --contract templates/data_contracts/basel3_contract.yaml
```

Convert a JSON payload to Markdown:

```bash
python3 -m governance.cli summarize --input examples/output_samples/validation_summary.json --title "Validation Summary" --output /tmp/validation_summary.md
```

Build the package metadata locally:

```bash
make package-check
```

The demo does not require Spark. Spark-based components require a compatible local Spark and PySpark runtime.

Generate a lineage bundle from the shipped example config:

```bash
python3 -m governance.cli lineage --config examples/lineage_config.json
```

## Containerized Demo

Build and run the demo with Docker:

```bash
docker compose up --build
```

## Adapting the Framework

To adapt the repository for a specific institution:

1. Start with `templates/data_contracts/generic_financial_record_contract.yaml`.
2. Select the closest operating profile under `assets/operating_profiles/`.
3. Select the closest workflow pattern under `assets/use_case_profiles/`.
4. For broader naming, use the files under `assets/control_sets/`.
5. Replace the example fields and thresholds with the institution’s own data elements, reporting cadence, and control requirements.
6. Add domain-specific rule files only where needed.

The current banking-oriented examples are included because they are concrete and testable. They are not meant to limit the framework to banks alone.
