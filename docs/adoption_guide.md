# Adoption Guide

FDGF is not limited to one class of financial institution. The repository is designed for adaptation beyond the banking-oriented examples by separating reusable control patterns from domain-specific examples.

## Reusable Patterns

The following patterns are institution-agnostic:
- rule-based data validation
- lineage and run-event capture
- machine-readable audit bundles
- drift and fairness monitoring
- configuration-driven control thresholds
- manifest-backed operating and use-case profiles

## Domain-Specific Examples

The current repository includes banking-oriented examples because they provide a concrete baseline:
- Basel III contract
- CCAR-style contract
- Basel III reference pipeline

These examples can be replaced or supplemented with institution-specific contracts for:
- transaction data
- customer master data
- ledger balances
- positions and valuations
- claims data
- lending and servicing data

## Suggested Rollout

1. Start with `assets/control_sets/universal_record_controls.yaml`.
2. Choose the closest operating profile under `assets/operating_profiles/`.
3. Choose the closest workflow pattern under `assets/use_case_profiles/`.
4. Replace the example fields and thresholds with institution-specific values.
5. Add new control sets for the institution’s highest-risk datasets.
6. Use the broad workflow names where institution-neutral naming is preferred.
7. Keep the lineage and monitoring modules unchanged unless institution-specific logic requires extension.
