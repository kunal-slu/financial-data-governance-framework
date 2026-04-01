# ADR 0002: Machine-Readable Contracts

## Status

Accepted

## Context

Hard-coded control logic limits reuse and makes it difficult to compare workflows.

## Decision

Use YAML contracts and policy files to externalize rule definitions and thresholds.

## Consequences

- easier template reuse
- clearer control portability
- more explicit review of thresholds and mappings

## Alternatives Considered

Considered embedding rule logic directly in code. Rejected because it reduces reuse and makes contract review less visible to non-developer reviewers.
