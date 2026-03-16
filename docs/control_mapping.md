# Control Mapping

This document summarizes how the repository maps its core modules to common supervisory control objectives in regulatory reporting and model-governance workflows.

## Module To Control Objective Mapping

| Module | Primary Objective | Typical Output |
| --- | --- | --- |
| `governance.data_quality.validators` | Completeness, reasonableness, reconciliation, formatting, timeliness | Audit bundle JSON |
| `governance.lineage.tracker` | Traceability of inputs, outputs, and transformations | OpenLineage-style event JSON and lineage bundle |
| `governance.model_governance.drift_detector` | Ongoing monitoring for drift and fairness issues | Model monitoring findings |
| `governance.reporting.compliance_summary` | Reviewer-friendly roll-up reporting | JSON and Markdown summaries |
| `governance.pipelines.basel3_pipeline` | Workflow integration of controls | Reference orchestration pattern |

## Source Mapping File

The machine-readable mapping lives in [`templates/control_mappings/regulatory_control_matrix.yaml`](../templates/control_mappings/regulatory_control_matrix.yaml).

## Example Interpretation

- validation controls support data quality, reconciliation, and reporting control checks
- lineage controls support reconstruction of how reportable data was produced
- model-governance checks support ongoing monitoring of model-driven workflows
- reporting summaries make raw control artifacts usable by reviewers without reading all source code
