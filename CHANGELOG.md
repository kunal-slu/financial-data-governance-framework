# Changelog

## 0.1.4

- aligned package, citation, and artifact version metadata to the 0.1.4 release
- made validation and model-monitoring summaries more explicit around framework version, fairness status, and fingerprint semantics
- tightened lineage schema and sample artifact fields for fingerprint methods and refreshed the generated output samples
- strengthened timeliness, pipeline, and reporting tests around boundary conditions, ordering, and summary consistency

## 0.1.3

- added a restrained white paper draft aligned to the current repository structure and terminology
- introduced a shared framework version constant and propagated it through validation, lineage, and monitoring artifacts
- tightened lineage, fairness, and basel3 pipeline semantics, including deterministic fingerprints, clearer status handling, and safer output behavior
- expanded spark, pipeline, reporting, and sample-artifact test coverage and refreshed the curated example outputs

## 0.1.2

- removed the remaining legacy `submission_ready` compatibility alias from the public code path
- made `build_artifact_bundle.py` produce a fuller, reproducible synthetic review bundle
- added `examples/generate_output_samples.py` to refresh the curated sample artifacts from current code
- regenerated the output samples so they match the current contract and artifact naming
- added citation metadata now that the public repository URL exists

## 0.1.1

- tightened README and architecture wording to match the current implementation
- added explicit explainability status modeling and expanded integration coverage
- added schema-level and cross-field contract support for the Basel III reference contract
- refreshed curated sample artifacts and cleaned release hygiene around local junk files
- clarified policy-template and reference-pipeline wording to better match current implementation scope

## 0.1.0

- added package metadata and build configuration
- added reporting summary utilities
- added CI workflow and split dependency files
- added control mapping metadata
- added sample output artifacts and benchmark/problem documentation
- added reusable liquidity and FDTA contracts
- added a lightweight CLI entry point
