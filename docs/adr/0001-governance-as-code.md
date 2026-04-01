# ADR 0001: Governance-As-Code

## Status

Accepted

## Context

Control logic in regulated workflows is often distributed across spreadsheets, manual review steps, and one-off scripts.

## Decision

Represent validation, lineage, and summary generation as executable artifacts in code and configuration.

## Consequences

- easier reuse across workflows
- clearer output artifacts
- better reproducibility than manual documentation alone

## Alternatives Considered

Considered relying on manual runbooks and spreadsheet-based control tracking. Rejected because they do not provide executable control logic or consistent generated evidence.
