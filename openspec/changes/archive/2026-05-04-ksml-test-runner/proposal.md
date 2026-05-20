## Why

KSML users define pipelines and producers in YAML with Python functions, but testing those pipelines requires writing Java JUnit tests using the `KSMLTestExtension` framework. This forces KSML users to leave the YAML+Python world and work in Java — a language they may not know. A standalone, YAML-driven test runner would let users test their pipelines using the same tools they already use to write them: YAML for structure and Python for logic.

## What Changes

- Add a new Maven module `ksml-test-runner` that provides a standalone test runner
- The runner reads YAML test definitions that specify: which pipeline to test, what data to produce, and what Python assertions to run against the output
- Test data is fed into a `TopologyTestDriver` (no live Kafka broker required)
- Assertions run in the same GraalVM Python sandbox used by KSML pipeline functions, with access to output records and named state stores
- The test runner is packaged as a separate JAR and added to the KSML Docker image as an additional entrypoint
- Output is plain text reporting (pass/fail per test, assertion error details)
- MVP accepts a single test file; the design supports multiple files per run (directory/glob) as a follow-up

## Capabilities

### New Capabilities
- `test-definition-format`: YAML format for defining pipeline tests, including inline test messages with optional timestamps, producer-based data generation, and Python assertion blocks with access to output records and state stores
- `test-runner-execution`: Standalone runner that parses test definitions, sets up TopologyTestDriver, produces test data, runs Python assertions, and reports results as plain text
- `test-runner-packaging`: Separate JAR and Docker entrypoint for running KSML pipeline tests

### Modified Capabilities

## Impact

- **New module**: `ksml-test-runner/` with its own `pom.xml`, depending on `ksml` (core parsing, topology generation, Python context, store proxies) and `kafka-streams-test-utils` (TopologyTestDriver)
- **Docker**: `Dockerfile` updated to copy `ksml-test.jar` and document the second entrypoint
- **Dependencies**: `kafka-streams-test-utils` moves from test-scope to compile-scope in the new module only; no change to existing modules
- **Existing code**: No modifications to `ksml`, `ksml-runner`, or other modules. Reuses `TopologyDefinitionParser`, `TopologyGenerator`, `PythonContext`, `StateStoreProxyFactory`, and notation registration from existing code
