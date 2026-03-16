# Failure Modes

This document lists common operational failure modes in regulated data workflows and how FDGF is intended to address them.

## Fragmented Reporting Pipelines

Different teams often maintain extraction, transformation, reconciliation, and review logic separately. This makes it difficult to know which controls actually ran for a given reporting cycle.

FDGF response:

- contract-driven validation
- reusable control sets
- workflow-linked lineage bundles

## Manual Reconciliations

Manual reconciliations are slow to perform, hard to repeat consistently, and difficult to audit after the fact.

FDGF response:

- machine-readable validation outputs
- standardized cross-dataset reconciliation rules
- deterministic summary artifacts

## Undocumented Transformations

Source-to-report mappings often exist in scripts or reviewer memory rather than durable, inspectable artifacts.

FDGF response:

- explicit transformation capture in lineage events
- persisted run-level lineage bundles
- repeatable event generation during execution

## Weak Audit Evidence

Many workflows can produce an output file but cannot quickly reconstruct how that file was generated or what controls were executed.

FDGF response:

- generated lineage bundles
- generated summary artifacts
- review-oriented JSON and Markdown outputs

## Inconsistent Control Execution

When controls are executed differently across teams or reporting periods, it becomes difficult to compare results or rely on the process.

FDGF response:

- declarative YAML contracts
- reusable threshold definitions
- repeatable validation execution paths
