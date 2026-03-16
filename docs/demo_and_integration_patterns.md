# Demo And Integration Patterns

FDGF is a reference framework, so its runtime patterns are intentionally lightweight.

## Local Evaluation

- install core dependencies
- run tests and examples
- inspect generated artifacts under `demo_output/`

## Batch Workflow Integration

- embed validation in scheduled reporting workflows
- persist lineage artifacts alongside workflow outputs
- publish summary artifacts for reviewer consumption

## Containerized Demo Pattern

- package the repository in a lightweight Python image
- run tests or the demo as the container entry point
- mount local output directories when inspecting generated artifacts

## Extension Pattern

- keep core primitives stable
- add new contracts, policies, and schemas for new reporting domains
- validate new outputs against schema and sample-artifact tests
