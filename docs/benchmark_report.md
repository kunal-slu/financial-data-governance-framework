# Benchmark Report

This benchmark uses synthetic, illustrative workflows to compare a fragmented manual-style process with a framework-based automated process for regulatory reporting data preparation.

It is intended to show the type of operational gap the repository addresses, not to claim measured performance at any specific institution.

## Scenario

The benchmark compares two approaches over the same synthetic reporting workflow:

- manual-style process
  Separate checks, fragmented logic, and manually assembled evidence
- framework-based process
  Contract-driven validation, generated lineage, and summary artifacts

## Illustrative Comparison

| Metric | Manual-Style Process | Framework-Based Process |
| --- | --- | --- |
| Validation coverage | Checklist-style coverage with inconsistent execution across runs | Configured checks executed consistently from contract |
| Failed-record detection | Some seeded issues can be missed or discovered late | Seeded issues are surfaced consistently by the configured checks |
| Lineage completeness | Input-only notes, no transformation trace | Inputs, outputs, transformations, and run events captured |
| Time to generate audit evidence | Manual assembly after the run | Generated artifacts available immediately after the run |
| Reproducibility of outputs | Manual reviewer rework required | Deterministic artifact files and hashes |

## Why It Matters

The main value is not just error detection. The stronger improvement is that the framework turns recurring control work into repeatable artifacts:

- validation summaries
- lineage bundles
- model-monitoring summaries
- review-ready Markdown outputs

This comparison is qualitative. It reflects the kinds of differences the repository is designed to illustrate with synthetic examples and generated artifacts, not institution-level benchmark measurements.

## Illustrative Scenario

This scenario is illustrative and uses synthetic assumptions rather than measured institution-specific results. In an illustrative monthly workflow for a smaller financial institution, the framework-based approach generates artifacts immediately after execution rather than requiring a separate manual assembly step.

See the sample artifacts under [`examples/output_samples/`](../examples/output_samples/).
