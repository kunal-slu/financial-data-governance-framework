# Control Catalog

This catalog summarizes what the current repository actually implements.

## Data Validation Controls

- `nullability`
  Detects missing values in required fields.
- `range`
  Checks numeric reasonableness against configured bounds.
- `uniqueness`
  Detects duplicate records for configured keys.
- `referential_integrity`
  Checks whether values resolve against a reference set.
- `cross_dataset_reconciliation`
  Compares metrics across datasets within a configured tolerance.
- `regulatory_format`
  Validates formatting patterns such as codes and dates.
- `timeliness`
  Checks data recency against configured lag thresholds.

These controls are executed by [`governance/data_quality/validators.py`](../governance/data_quality/validators.py).

## Lineage and Audit Evidence

- input registration
- transformation registration
- output registration
- run-start and run-complete events
- machine-readable lineage bundle output

These functions are implemented in [`governance/lineage/tracker.py`](../governance/lineage/tracker.py).

## Model-Governance Checks

- PSI-based drift monitoring
- selection-rate fairness checks

These functions are implemented in [`governance/model_governance/drift_detector.py`](../governance/model_governance/drift_detector.py).

## Reusable Asset Layers

- control sets
  Reusable rule-group starting points for common control domains.
- operating profiles
  Institution-type starting points.
- use-case profiles
  Workflow-type starting points.

The current asset inventory is declared in [`assets/framework_manifest.yaml`](../assets/framework_manifest.yaml).
