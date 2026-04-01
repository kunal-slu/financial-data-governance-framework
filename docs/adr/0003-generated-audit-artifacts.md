# ADR 0003: Generated Audit Artifacts

## Status

Accepted

## Context

Post hoc audit assembly is slow and inconsistent.

## Decision

Generate lineage, validation, and summary artifacts as a direct output of framework execution.

## Consequences

- faster review preparation
- more repeatable evidence generation
- improved comparability across runs

## Alternatives Considered

Considered assembling review evidence after the run from separate logs and notes. Rejected because it makes evidence collection slower and less consistent across workflows.
