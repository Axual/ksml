## 1. Java type model

- [x] 1.1 Add `StreamDefinition` record (`topic`, `keyType`, `valueType`) replacing the current `RegistryEntry` record.
- [x] 1.2 Add `TestSuiteDefinition` record (`name`, `definition`, `schemaDirectory`, `moduleDirectory`, `streams: LinkedHashMap<String, StreamDefinition>`, `tests: LinkedHashMap<String, TestCaseDefinition>`).
- [x] 1.3 Add `TestCaseDefinition` record (`description`, `produce`, `assertions`).
- [x] 1.4 Update `ProduceBlock`: replace `topic`/`keyType`/`valueType` with a single `to` field (String, references a `streams` key).
- [x] 1.5 Update `AssertBlock`: replace `topic` with an `on` field (String, optional, references a `streams` key).
- [x] 1.6 Add `suiteName` and `testName` fields to `TestResult`; update construction sites accordingly.
- [x] 1.7 Delete the old `TestDefinition` record and `RegistryEntry` record.
- [x] 1.8 Add a constant for the identifier regex `^[a-zA-Z][a-zA-Z0-9_]*$` (used for both stream keys and test keys). _(Will be added in §2 alongside the parser that uses it.)_

## 2. Parser

- [x] 2.1 Rewrite `TestDefinitionParser` to read top-level fields directly from the YAML root (no outer `test:` wrapper).
- [x] 2.2 Parse `streams:` as a `LinkedHashMap<String, JsonNode>`; validate each key against the identifier regex; produce a clear error identifying any offending key.
- [x] 2.3 For each stream entry, parse `keyType`/`valueType` via `UserTypeParser.parse(t, false)`; on parse error, propagate the parser's message (which already covers `json:Foo`, bare `confluent_avro`, etc.).
- [x] 2.4 Detect duplicate `topic` values across `streams:` entries and reject with an error identifying both stream keys.
- [x] 2.5 Parse `tests:` as a `LinkedHashMap<String, JsonNode>`; validate each key against the identifier regex; reject empty or missing `tests:` map with an explicit "at least one test required" error.
- [x] 2.6 Reject test entries missing `produce` or `assert` with an error identifying the offending test key.
- [x] 2.7 For each `produce` block, validate that `to:` references an existing key in `streams:`. (No migration aid for old `topic`/`keyType`/`valueType` fields — pre-release format, not in production.)
- [x] 2.8 For each `assert` block, validate that `on:` (when present) references an existing key in `streams:`; preserve "at least one of `on:`/`stores:` is required" check.
- [x] 2.9 Reject suite-level fields (`definition`, `streams`, `schemaDirectory`, `moduleDirectory`, `name`) appearing inside individual test entries.
- [x] 2.10 Detect duplicate keys in both the `streams:` map and the `tests:` map (raw-YAML scan or strict reader configuration); reject with a clear error identifying the duplicate key.

## 3. Runner

- [x] 3.1 Replace `KSMLTestRunner.runSingleTest(Path) -> TestResult` with `runTestFile(Path) -> List<TestResult>`.
- [x] 3.2 Inside `runTestFile`: parse suite YAML, parse pipeline YAML once, then iterate over `suite.tests()` in insertion order. For each test, build a fresh `Topology` + `TopologyTestDriver`, run produce, run assertions, capture result, close the driver.
- [x] 3.3 If suite-level parsing or pipeline parsing fails, return a single ERROR result for the whole suite (no per-test results).
- [x] 3.4 If one test fails or errors, continue running remaining tests; never short-circuit the loop.
- [x] 3.5 Update produce-data execution to resolve `to:` against the suite's `streams:` map and use the resolved topic + serdes.
- [x] 3.6 Update assert-data execution to resolve `on:` against the suite's `streams:` map and use the resolved topic + serdes.
- [x] 3.7 Update mock-registry population: walk the `streams:` map, identify schema-bearing types, load schemas from `schemaDirectory`, register them under `<topic>-key`/`<topic>-value`. (This replaces the current behavior driven by `registry:`.)
- [x] 3.8 Set `TestResult.suiteName` from `suite.name()` ?? filename-without-extension; set `TestResult.testName` from `case.description()` ?? key.
- [x] 3.9 Update `main()` to call `runTestFile` and aggregate via `addAll`; verify exit-code logic still computes from `allMatch(PASS)`.
- [x] 3.10 Update `reportResults` to print the qualified `<suite> › <test>` label composed from `TestResult` fields.

## 4. Schema generator

- [x] 4.1 Update `TestDefinitionSchemaGenerator` to emit the new root-level shape (no `test:` wrapper).
- [x] 4.2 Define `StreamSchema` (object with `topic` required, `keyType`/`valueType` optional with defaults).
- [x] 4.3 Set `streams: { additionalProperties: false, patternProperties: { "^[a-zA-Z][a-zA-Z0-9_]*$": StreamSchema } }`.
- [x] 4.4 Define `TestCaseSchema` (object with `description` optional, `produce` required array, `assert` required array).
- [x] 4.5 Set `tests: { additionalProperties: false, patternProperties: { "^[a-zA-Z][a-zA-Z0-9_]*$": TestCaseSchema }, minProperties: 1 }`.
- [x] 4.6 Update produce-block schema: `to` required string, `messages` array OR `generator` object (mutually exclusive).
- [x] 4.7 Update assert-block schema: `on` optional string, `stores` optional array, `code` required string, with `at-least-one-of` constraint between `on` and `stores`.
- [x] 4.8 Run `mvn process-classes -pl ksml-test-runner -am` and verify `docs/ksml-test-spec.json` regenerates without error.

## 5. Migrate existing test resources

- [x] 5.1 Migrate `sample-filter-test.yaml` to the new format.
- [x] 5.2 Migrate `sample-filter-test-confluent-avro.yaml`.
- [x] 5.3 Migrate `sample-filter-test-apicurio-avro.yaml`.
- [x] 5.4 Migrate `sample-filter-test-registry-only-types.yaml` (becomes a standard 1-test suite; the registry-only-types distinction disappears).
- [x] 5.5 Migrate `sample-filter-test-avro-python-produce.yaml`.
- [x] 5.6 Migrate `sample-filter-test-python-produce.yaml`.
- [x] 5.7 Migrate `sample-filter-test-module-import.yaml`.
- [x] 5.8 Migrate `sample-state-store-test.yaml`.
- [x] 5.9 Migrate `sample-timestamp-test.yaml`.
- [x] 5.10 Migrate parser fixture `valid-test-definition.yaml`.
- [x] 5.11 Migrate parser fixture `valid-test-with-stores.yaml`.
- [x] 5.12 Migrate parser fixture `defaults-test.yaml`.
- [x] 5.13 Collapse `processor-filtering-transforming-complete-test/*.yaml` (5 files) into a single multi-test suite at `processor-filtering-transforming-complete-test.yaml`; delete the subdirectory.

## 6. Migrate / add invalid-fixture parser tests

- [x] 6.1 Replace `missing-test-root.yaml` with `missing-tests.yaml` (no `tests:` map).
- [x] 6.2 Delete `missing-name.yaml` (suite-level `name` is now optional).
- [x] 6.3 Update `missing-produce.yaml` to be a 1-test suite where the test entry omits `produce:`.
- [x] 6.4 Update `assert-no-topic-no-stores.yaml` to be a 1-test suite preserving the existing failure mode (assert with neither `on:` nor `stores:`).
- [x] 6.5 Add `invalid-test-key.yaml` (test key violates regex).
- [x] 6.6 Add `invalid-stream-key.yaml` (stream key violates regex).
- [x] 6.7 Add `duplicate-test-key.yaml` (duplicate key in `tests:`).
- [x] 6.8 Add `duplicate-stream-topic.yaml` (two stream entries with the same `topic`).
- [x] 6.9 Add `undefined-stream-reference.yaml` (a `to:` or `on:` referencing a stream key not declared in `streams:`).
- [x] 6.10 ~~Add `inline-topic-on-produce.yaml`~~ — dropped: no migration aid kept for old field names; missing `to:` is rejected by the generic "missing required field" check.
- [x] 6.11 Add `bare-vendor-avro.yaml` (a stream entry with `valueType: confluent_avro` and no schema name).

## 7. Update Java tests

- [x] 7.1 Update `TestDefinitionParserTest` for the new format: top-level fields, `streams:` map, `tests:` map, key validation, duplicate detection, missing-field errors, undefined-reference errors. Type-string rejection moved to runtime (covered by `KSMLTestRunnerTest.invalidDefinitionsReturnNonPass`).
- [x] 7.2 In `KSMLTestRunnerTest`, change the parameterized tests to call `runTestFile` (returning `List<TestResult>`) and assert across the list (`allMatch(PASS)` for valid, `anyMatch(non-PASS)` for invalid).
- [x] 7.3 Update `invalidDefinitionsReturnNonPass`'s `@ValueSource` for the renamed/added invalid fixtures (drop `missing-name.yaml`, rename `missing-test-root.yaml` → `missing-tests.yaml`, add the new ones from §6).
- [x] 7.4 Add `processor-filtering-transforming-complete-test.yaml` to `validTestsReturnPass`'s `@ValueSource`.
- [x] 7.5 Remove the `filteringTransformingCompleteTestsAllPass` directory-walking unit test (no longer needed).
- [x] 7.6 Update `confluentAvroFilterTestPasses` and `apicurioAvroFilterTestPasses` to assert against the new `List<TestResult>` shape.

## 8. Documentation

- [x] 8.1 Update `ksml-test-runner/README.md` test-definition format example to the new shape (no outer `test:`, show `streams:` map, show `tests:` map with two entries, show `description:` fallback, show `to:`/`on:` references).
- [x] 8.2 Update the field tables in `README.md` (root-level fields vs per-stream fields vs per-test fields).
- [x] 8.3 Document test ordering, hermetic isolation, identifier regex (both keys), and the type-string rule (with the explicit subset relationship to KSML's `StreamDefinitionParser`).

## 9. Verification

- [x] 9.1 Run `mvn -pl ksml-test-runner test` and confirm all tests pass under GraalVM. (Result: 84 passed, 0 failed, 0 errors.)
- [x] 9.2 Run the test-runner JAR against the resources directory from the CLI to confirm end-to-end CLI invocation still works. (Verified `sample-filter-test.yaml` and `processor-filtering-transforming-complete-test.yaml`; both pass.)
- [x] 9.3 Inspect a sample output to confirm reporting label format `<suite> › <test>` is rendered correctly. (Verified — see `processor-filtering-transforming-complete-test.yaml` output above.)
- [x] 9.4 Confirm exit code 0 when all tests pass; manually break one test to confirm exit code 1 path. (Verified: passing suite → exit 0; `missing-tests.yaml` invalid fixture → exit 1.)
- [ ] 9.5 Confirm `docs/ksml-test-spec.json` validates a hand-edited valid suite file (positive case) and rejects each new invalid-fixture file (negative cases) when used via a YAML language server. _(Manual editor-based check; deferred to user.)_
