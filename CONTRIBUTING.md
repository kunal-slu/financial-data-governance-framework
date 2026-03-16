# Contributing to FDGF

FDGF is a compact open-source reference implementation for governance-as-code in regulated data workflows.

## Useful Contribution Areas

- additional reusable contracts under `templates/data_contracts/`
- extensions to lineage and summary generation
- improvements to example workflows and documentation
- tests for output artifacts, contracts, and CLI behavior
- clearer control-mapping and extension guidance

## How to Contribute

1. Fork the repository.
2. Create a focused branch for the change.
3. Add tests and docs where the change affects behavior or public usage.
4. Keep examples and sample artifacts coherent with the code.
5. Submit a pull request with a concise explanation of the change and tradeoffs.

## Code Standards

- Python 3.10+ with type hints where practical
- keep public docs technical and implementation-focused
- new validation rules should include `regulatory_ref` metadata
- add or update tests for changed behavior
- prefer small, reviewable changes over broad unfinished scaffolding

## License

By contributing, you agree that your contributions will be licensed under
the Apache 2.0 License.
