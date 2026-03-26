# Changelog

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
- softened policy-template and reference-pipeline wording where it overstated behavior

## 0.1.0

- added package metadata and build configuration
- added reporting summary utilities
- added CI workflow and split dependency files
- added control mapping metadata
- added sample output artifacts and benchmark/problem documentation
- added reusable liquidity and FDTA contracts
- added a lightweight CLI entry point
