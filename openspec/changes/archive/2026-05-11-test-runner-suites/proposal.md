## Why

The KSML test runner currently requires one YAML file per test, with a top-level `registry:` block that mirrors information already declared in the pipeline's `streams:` block. The five files in `processor-filtering-transforming-complete-test/` illustrate the per-file cost: each repeats the same `definition:`, `registry:`, `schemaDirectory:`, and `moduleDirectory:` configuration to vary only produce data and assertions. Beyond the duplication, the `registry:` block introduces a vocabulary that has no counterpart in pipeline yamls, even though it describes the same concept (named topic + types). With the test runner still in pre-release, this is the right moment to restructure the format so that (a) one file holds many tests for one pipeline and (b) the file's vocabulary mirrors KSML pipeline yamls — `streams:` for named topic+type bindings, referenced by name from elsewhere in the file.

## What Changes

- **BREAKING**: Replace the single-test YAML format with a suite-per-file format. Every test definition file now describes one or more named tests sharing a single pipeline and configuration.
- **BREAKING**: Drop the outer `test:` wrapper element. Pipeline, schema/module directories, the optional suite-level `name:`, the new `streams:` map, and the `tests:` map all live at the file root.
- **BREAKING**: Replace the `registry:` block with a `streams:` map that mirrors the shape used in KSML pipeline yamls. Each stream entry has a logical name as its key (referenced from elsewhere in the file) and contains `topic`, `keyType`, and `valueType` as nested fields.
- **BREAKING**: Replace `produce: - topic: foo, keyType: ..., valueType: ...` with `produce: - to: <stream_key>`. The produce block references a stream by name; types come from the stream's declaration. Inline topic/key/value-type fields on a produce block are not permitted.
- **BREAKING**: Replace `assert: - topic: foo` with `assert: - on: <stream_key>`. Same semantics as `to:` for produce blocks. Inline `topic` on an assert block is not permitted.
- **BREAKING**: Replace the top-level `produce:` and `assert:` keys with a `tests:` map. Each map key is a stable identifier matching `^[a-zA-Z][a-zA-Z0-9_]*$`; the value contains that test's own `produce:`, `assert:`, and optional `description:` (which falls back to the key for display).
- **BREAKING**: Stream keys are subject to the same identifier regex as test keys.
- **BREAKING**: Type strings under `streams:` MUST resolve under KSML's standard type grammar with `allowUnresolved=false` — the same configuration KSML uses for stream type fields in pipeline yamls. Schema-bearing notations (avro, confluent_avro, apicurio_avro, protobuf and its vendor variants, json_schema and its vendor variants, csv) MUST be qualified (e.g., `avro:SensorData`); schemaless notations (`string`, `long`, `json`, `binary`, etc.) MAY be unqualified. `json:SensorData` and similar invalid combinations are rejected by the type parser.
- **BREAKING**: Each test in a suite runs against a fresh `TopologyTestDriver` — tests are hermetic, no state shared between them. The pipeline parse is shared across tests in a file (cheap optimization).
- **BREAKING**: `KSMLTestRunner.runSingleTest(Path) -> TestResult` becomes `KSMLTestRunner.runTestFile(Path) -> List<TestResult>`. The aggregation in `main()` collects the flattened list; exit-code logic is unchanged.
- Reporting label format becomes `<suite> › <test>` where `<suite>` is the file's `name:` (or basename if absent) and `<test>` is the `description:` if set, otherwise the key.
- The build-time JSON schema (`docs/ksml-test-spec.json`) is regenerated to describe the new format, including `patternProperties` for stream keys and test keys.
- The `processor-filtering-transforming-complete-test/` directory collapses into a single `processor-filtering-transforming-complete-test.yaml` file at the resources root. The `filteringTransformingCompleteTestsAllPass` directory-walking unit test in `KSMLTestRunnerTest` is removed (replaced by an entry in the existing `validTestsReturnPass` parameterized test).
- All other existing test definitions are migrated to the new format as 1-test suites.
- The test parser remains decoupled from the KSML pipeline parser. Stream declarations in the test yaml are NOT inherited from the referenced pipeline; the test yaml is the sole source of truth for the test runner's stream/topic information.

## Capabilities

### New Capabilities

(none — this is a restructure of existing capabilities)

### Modified Capabilities
- `test-definition-format`: The YAML structure changes substantially. Outer `test:` wrapper removed; `registry:` replaced by `streams:` (logical-name keyed, mirroring pipeline yaml shape); `produce:` references streams via `to:`; `assert:` references streams via `on:`; tests are organized under a `tests:` map keyed by stable identifier with optional descriptions; type-string parsing rules tightened to disallow unresolved schemas. Identifier regex enforced for both stream keys and test keys.
- `test-runner-execution`: The runner's per-file API and reporting change. `runSingleTest` is replaced by `runTestFile` returning `List<TestResult>`; reporting labels are qualified as `<suite> › <test>`; each test in a file runs against a fresh `TopologyTestDriver`; stream reference resolution becomes part of the lifecycle.

## Impact

- **Code (Java)**:
  - `TestDefinition` record is replaced. New types: `TestSuiteDefinition` (file-level: `name`, `definition`, `schemaDirectory`, `moduleDirectory`, `streams`, `tests`), `StreamDefinition` (per-entry: `topic`, `keyType`, `valueType`), `TestCaseDefinition` (per-test entry: `description`, `produce`, `assertions`).
  - `RegistryEntry` record is removed (its role is now covered by `StreamDefinition`).
  - `TestDefinitionParser` is rewritten to parse the new YAML shape, validate stream-key and test-key regexes, parse type strings via `UserTypeParser` with `allowUnresolved=false`, and detect duplicate keys / duplicate topics / undefined stream references.
  - `ProduceBlock` gains a `to:` field referencing a stream key; `topic`/`keyType`/`valueType` fields are removed.
  - `AssertBlock` gains an `on:` field referencing a stream key; `topic` field is removed.
  - `KSMLTestRunner.runSingleTest` removed; `runTestFile` added returning `List<TestResult>`. `main()` updated.
  - `TestResult` carries suite name and test name as separate fields; the qualified `<suite> › <test>` label is composed at print time.
  - `TestDefinitionSchemaGenerator` regenerates `docs/ksml-test-spec.json` for the new shape.
  - `KSMLTestRunnerTest` parameterized methods updated to assert across `List<TestResult>`. The `filteringTransformingCompleteTestsAllPass` method is removed.
- **Test resources**: Every existing `*.yaml` test definition under `ksml-test-runner/src/test/resources/` is migrated to the new format. The five files in `processor-filtering-transforming-complete-test/` collapse to one file; the subdirectory is removed.
- **Documentation**: `ksml-test-runner/README.md` updated with the new format and examples.
- **No impact**: `ksml/`, `ksml-runner/`, schema files, the Docker image, or any other module. Internal-only refactor of the test runner.
