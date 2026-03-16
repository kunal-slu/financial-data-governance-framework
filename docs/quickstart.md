# Quickstart

## 1. Install core dependencies

```bash
python3 -m pip install -r requirements.txt
```

## 2. Run tests

```bash
python3 -m pytest -q
```

## 3. Run the lightweight demo

```bash
python3 examples/run_lightweight_demo.py
```

## 4. Build an artifact bundle

```bash
python3 examples/build_artifact_bundle.py
```

## 5. Try the CLI

Inspect a contract:

```bash
python3 -m governance.cli inspect-contract --contract templates/data_contracts/basel3_contract.yaml
```

Generate a summary:

```bash
python3 -m governance.cli summarize --input examples/output_samples/validation_summary.json --title "Validation Summary" --output /tmp/validation_summary.md
```
