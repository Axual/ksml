## Context

The KSML test runner exists as of the just-archived `ksml-test-runner` change. Its current YAML format is a single-test definition: every file has a `test:` root with one `name`, one `definition`, one optional `registry:` block, one `produce` list, and one `assert` list. The five files in `ksml-test-runner/src/test/resources/processor-filtering-transforming-complete-test/` exemplify two friction points: (a) shared configuration is duplicated across files, and (b) the `registry:` vocabulary has no parallel in KSML pipeline yamls even though it describes the same concept (a named topic with key and value types).

This change is a pre-release format restructure. The test runner has not yet shipped to users, so we accept breaking changes to all existing test definitions. The goals are to land one file per pipeline (with N hermetic tests), and to align the file's vocabulary with KSML pipeline conventions: `streams:` for named topic+type bindings, `to:` and `on:` for references from produce/assert blocks. The `registry:` block disappears — its two functions (declaring topic types for serdes and pre-populating the mock schema registry) become natural side effects of the new `streams:` map.

The earlier explore-mode discussion considered three flavors of "multiple tests per file": (A) hermetic cases with shared infrastructure, (B) sequential cases sharing a `TopologyTestDriver` to test stateful behavior, and (C) parameterized data-driven cases. We are implementing **(A) only** in this change. (B) and (C) remain on the table as future work.

## Goals / Non-Goals

**Goals:**
- One YAML file describes N hermetic tests sharing a single pipeline and configuration.
- File vocabulary mirrors KSML pipeline yamls — `streams:` for named topic+type bindings, referenced from elsewhere by name.
- Each test has a stable, machine-friendly identifier — usable in CLI filters, JUnit-XML reports, and grep — without forcing humans to write identifiers in free-form prose.
- Drop the outer `test:` wrapper. The file's root IS the suite. Reduce one indent level for every line.
- Logical names (stream keys, test keys) are uniformly constrained by the same regex.
- Type strings in `streams:` use exactly the same parser configuration KSML's `StreamDefinitionParser` already uses (`UserTypeParser.parse(t, false)`), so `json:Foo` and bare `confluent_avro` are rejected the same way KSML rejects them in stream definitions.
- The build's JSON schema (`docs/ksml-test-spec.json`) describes the new format precisely enough for editor autocomplete to be useful.
- Migrate all existing test resources to the new format, including collapsing the 5-file `processor-filtering-transforming-complete-test/` directory into one file.

**Non-Goals:**
- Sharing a `TopologyTestDriver` across tests within a suite (flavor B). Each test still gets a fresh driver. State-store testing of accumulation across operations remains a single-test concern.
- Parameterized tests (flavor C). If you want 5 inputs against the same logic, that's 5 entries in `tests:`. No expansion syntax.
- Cross-file test composition / includes / inheritance. Each suite stands alone.
- Reading the referenced pipeline's `streams:` block to inherit type information. The test parser stays decoupled from the KSML parser; the test yaml redeclares whatever streams the test touches.
- Mirroring KSML's `tables:`/`globalTables:` sections in the test yaml. A single `streams:` block covers every topic-like declaration the test runner needs.
- Inline stream declarations under `to:` or `on:`. Every topic the test touches must be declared in `streams:` so the file has one canonical list of involved topics.
- CLI filtering of individual tests within a file (e.g. `--test foo`). The map keys make this trivial to add later, but it's not in scope here.
- Backward compatibility with the previous single-test format. Pre-release; we migrate everything in one pass.

## Decisions

### Shape: streams + tests, no outer wrapper

```yaml
name: "Filtering & transforming pipeline"          # optional, falls back to file basename
definition: pipelines/processor-filtering-transforming-complete.yaml
schemaDirectory: schemas
moduleDirectory: modules

streams:
  sensor_source:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: "avro:SensorData"
  sensor_filtered:
    topic: ksml_sensordata_filtered
    keyType: string
    valueType: "avro:SensorData"

tests:
  valid_sensor_data:
    description: "Valid sensor data is transformed"           # optional
    produce:
      - to: sensor_source
        messages: [...]
    assert:
      - on: sensor_filtered
        code: |
          assert ...

  out_of_range_temperature:
    description: "Out-of-range temperature is filtered out"
    produce: [...]
    assert: [...]
```

### Why `streams:` and not `registry:`

The current `registry:` block had two roles: (1) declare topic+type info so output topic deserialization works in assert blocks, (2) pre-populate the mock schema registry for vendor-avro types. Both are about giving the test runner enough information to set up serdes for a topic. KSML pipeline yamls already have a vocabulary for this concept (`streams:` keyed by logical name with topic+types nested), and pipelines reference those names from `from:`/`to:`/`via:`. The test yaml previously didn't reference any logical name from anywhere — but with `produce: - to:` and `assert: - on:` referring to stream keys, logical names become functional. The registry's role is fully absorbed.

### Why a map and not a list of records with `name:`?

Same reasoning we applied to `tests:` — a map gives every entry a stable identifier, prevents duplicates at parse time, and matches the conventions of every other KSML map-shaped section. For `streams:` this is doubly valuable because the keys are referenced from `to:`/`on:`.

| Concern | List-with-`name` | Map-with-key |
|---|---|---|
| Stable identifier | invent one or grep prose | the YAML key |
| Referenced elsewhere in file | clumsy (string match) | natural (`to: foo`) |
| Duplicate detection | runtime check | YAML/parser enforces |
| Visual consistency with KSML pipeline yamls | poor | strong |

### Why drop the `test:` wrapper

Once the suite *is* the file, an outer `test:` element is misleading. The file path identifies the suite. Dropping the wrapper saves one indent level on every line and makes the file shape self-evident. Optional `name:` at root provides a human label for the suite when needed.

### Identifier constraints (both test keys and stream keys)

Both `tests:` and `streams:` keys must match `^[a-zA-Z][a-zA-Z0-9_]*$`. Rationale:

- Forces machine-friendliness. Free-form strings would defeat the identifier purpose for tests (CLI filters) and for streams (the references in `to:`/`on:`).
- Scan of every stream/table/store/pipeline/function name across ~30 KSML pipeline yamls in the project found zero violations of this regex. All real-world KSML names are snake_case alphanumerics starting with a letter.
- KSML's underlying name validation (via Kafka Streams' `Named.validate`) accepts a slightly looser set including `.` and `-`. The strict regex is therefore a strict subset of what KSML accepts — anything legal in a test yaml is also legal in a pipeline yaml. Authors can copy names verbatim. Tightening later would be breaking; relaxing later is additive.

### Type-string rule for `streams:`

The runner parses every `keyType`/`valueType` value with KSML's standard parser:

```
UserTypeParser.parse(typeString, /*allowUnresolved=*/ false)
```

This is exactly the call configuration KSML's `StreamDefinitionParser` uses. Consequences are inherited from KSML's parser, not invented for the test runner:

- `string`, `long`, `int`, `boolean`, `bytes`, `binary`, `xml` (`SCHEMA_OPTIONAL`), `json` (`SCHEMALESS_ONLY` with `defaultType`), `soap` (`SCHEMALESS_ONLY`) — accepted bare.
- `avro:Foo`, `confluent_avro:Foo`, `apicurio_avro:Foo`, `protobuf:Foo`, `json_schema:Foo`, `csv:Foo` — accepted; the schema is loaded from `schemaDirectory` and registered in the mock registry under `<topic>-key`/`<topic>-value`.
- `confluent_avro` (bare), `apicurio_avro` (bare), `protobuf` (bare), `csv` (bare), `avro` (bare) — rejected. The parser produces an `UnresolvedType` and `allowUnresolved=false` rejects it with the KSML error message.
- `json:Foo` — rejected. `JsonNotation` is `SCHEMALESS_ONLY`, so `UserTypeParser` produces: `Schema "Foo" can not be used for notation json. Use "json" to use the notation schemaless.`
- `list(string)`, `tuple(string, long)`, `map(string)`, `union(string, long)` — accepted; inner types are parsed by the same rules.

This is a **strict subset** of what KSML pipelines accept — KSML's table/topic definition parsers use `allowUnresolved=true` so they accept bare `confluent_avro`, but stream definitions don't. Our test yaml's `streams:` block thus matches stream-definition strictness, which is the correct analog. Authors can copy stream type fields from a pipeline verbatim; they cannot copy table/topic fields that use bare unresolved types (those would need to be qualified for the test).

### Reference resolution for `to:` and `on:`

`produce: - to: <key>` and `assert: - on: <key>` accept exactly one form: a string that is a key in the suite's `streams:` map. No inline shape. The runner resolves the reference at parse time:

- Lookup the key in `streams:`. If absent → parse error identifying the offending reference.
- Use the resolved `StreamDefinition` (topic + types) to pick the serdes for produce/assert.

The constraint "every involved topic must be declared in `streams:`" makes the file's `streams:` block authoritative. A reader can scan the streams: block and know every topic the test touches.

### Topic uniqueness in `streams:`

Two stream entries with the same `topic` are rejected. Rationale: the mock schema registry has at most one schema per `<topic>-key`/`<topic>-value` subject. Allowing multiple stream entries with the same topic would either (a) silently let one overwrite the other, or (b) require complex disambiguation. (a) is dangerous, (b) is over-engineering for a use case that has no precedent in the existing test suite. Reject and document.

### Driver lifecycle: per-test, not per-suite

Each test in a suite runs against a fresh `TopologyTestDriver`. State stores reset between tests, output topics start empty, etc. This preserves the current "single-yaml = single hermetic execution" semantics — the new format is purely a packaging change, not a semantics change.

The pipeline YAML is parsed once per file (cheap optimization). The `Topology` object is rebuilt per test (effectively free) because state-store builders are wired into the topology. Kafka Streams test utils don't have a clean reset path on a single driver, so per-test instantiation is the simplest correct choice.

```
parse suite YAML                            ← once per file
parse pipeline YAML                         ← once per file
resolve stream references for every test    ← once per file (validation)
for each test in suite.tests:
  build StreamsBuilder + new Topology       ← per test
  new TopologyTestDriver(topology)          ← per test (fresh state)
  produce(test.produce, resolved streams)
  run test.assert(resolved streams)
  driver.close()
```

### API change: `runSingleTest` → `runTestFile`

```
KSMLTestRunner.runSingleTest(Path) -> TestResult        // before
KSMLTestRunner.runTestFile(Path)   -> List<TestResult>  // after
```

`main()`:
```java
for (var file : testFiles) {
    results.addAll(runner.runTestFile(file));
}
```

Exit-code logic unchanged (`allPassed = results.allMatch(PASS)`). The list is empty only on parse failure of the suite itself, in which case a single `ERROR` result is returned.

`TestResult` carries `suiteName` and `testName` as separate fields. The qualified label `<suite> › <test>` is composed at print time. Separate fields keep the door open for future JUnit-XML output where the two map to `<testsuite name=...>` and `<testcase name=...>`.

### Reporting

Current reporter prints `PASS/FAIL/ERROR  <testName>`. New format:

```
=== KSML Test Results ===

  PASS  Filtering & transforming pipeline › valid_sensor_data
  PASS  Filtering & transforming pipeline › out_of_range_temperature
  FAIL  Filtering & transforming pipeline › missing_temperature_data
        AssertionError: ...
  PASS  Filter pipeline passes blue sensors › only_test
```

Single-test files (one entry in `tests:`) still get the qualified `<suite> › <test>` label. No special-case for "anonymous" tests; readability is consistent.

### Java type model

Three records replace `TestDefinition` and `RegistryEntry`:

```java
public record TestSuiteDefinition(
    String name,                                   // optional (file basename fallback applied later)
    String pipeline,
    String schemaDirectory,
    String moduleDirectory,
    Map<String, StreamDefinition> streams,         // LinkedHashMap, may be empty if no tests reference any stream
    Map<String, TestCaseDefinition> tests          // LinkedHashMap, order preserved, at least one entry
) {}

public record StreamDefinition(
    String topic,
    String keyType,                                // raw type string; UserType is resolved later
    String valueType                               // raw type string; UserType is resolved later
) {}

public record TestCaseDefinition(
    String description,                            // optional
    List<ProduceBlock> produce,
    List<AssertBlock> assertions
) {}
```

The `streams` and `tests` fields use `LinkedHashMap` (Jackson default for `Map`) so YAML order is preserved.

`ProduceBlock` and `AssertBlock` are updated:
- `ProduceBlock`: `to` (String, required, references `streams` key) replaces `topic`/`keyType`/`valueType`.
- `AssertBlock`: `on` (String, optional, references `streams` key) replaces `topic`. `stores`/`code` unchanged.

### Parser changes

`TestDefinitionParser`:
- Parse top-level fields directly from the YAML root, not from a nested `test:` node.
- Parse `streams:` as a `Map<String, JsonNode>`, iterate entries, validate each key against the identifier regex, parse the value into a `StreamDefinition`, parse `keyType`/`valueType` via `UserTypeParser.parse(t, false)`, surface any parser error.
- Detect duplicate `topic` values across stream entries; reject with a clear error.
- Parse `tests:` as a `Map<String, JsonNode>`, iterate entries, validate each key against the identifier regex, parse the value into a `TestCaseDefinition`.
- For each `produce` block, validate `to:` references an existing stream key; reject inline `topic`/`keyType`/`valueType` fields.
- For each `assert` block, validate `on:` (when present) references an existing stream key; reject inline `topic` field.
- Reject empty/missing `tests:` map.
- Detect duplicate keys in `tests:` and `streams:` (raw-YAML scan or strict reader configuration).

The duplicate-key check is worth verifying during implementation — Jackson's behavior varies by version. If strict-mode is too invasive, document the limitation and rely on the schema generator's constraints, or scan the raw YAML for duplicates.

### Schema generator

`TestDefinitionSchemaGenerator` regenerates `docs/ksml-test-spec.json`. Notable shape:

- Top-level: `name` (string, optional), `definition` (string, required), `schemaDirectory`, `moduleDirectory`, `streams` (object), `tests` (object).
- `streams`: `additionalProperties: false`, `patternProperties: { "^[a-zA-Z][a-zA-Z0-9_]*$": StreamSchema }`.
- `tests`: `additionalProperties: false`, `patternProperties: { "^[a-zA-Z][a-zA-Z0-9_]*$": TestCaseSchema }`, `minProperties: 1`.
- `StreamSchema`: `topic` (required string), `keyType` (optional string, default "string"), `valueType` (optional string, default "string").
- `TestCaseSchema`: `description` (optional string), `produce` (required array), `assert` (required array).
- Produce-block schema: `to` (required string), `messages` (array) or `generator` (object) — but not both.
- Assert-block schema: `on` (optional string), `stores` (optional array), `code` (required string), with the constraint that at least one of `on`/`stores` is present.

### Migration of existing test resources

| Existing file | After |
|---|---|
| `sample-filter-test.yaml` | 1-test suite using `streams:` and `to:`/`on:` |
| `sample-filter-test-confluent-avro.yaml` | 1-test suite |
| `sample-filter-test-apicurio-avro.yaml` | 1-test suite |
| `sample-filter-test-registry-only-types.yaml` | 1-test suite (registry-only-types lose their distinct meaning under streams; merge into the standard form) |
| `sample-filter-test-avro-python-produce.yaml` | 1-test suite |
| `sample-filter-test-python-produce.yaml` | 1-test suite |
| `sample-filter-test-module-import.yaml` | 1-test suite |
| `sample-state-store-test.yaml` | 1-test suite |
| `sample-timestamp-test.yaml` | 1-test suite |
| `valid-test-definition.yaml` (parser fixture) | 1-test suite |
| `valid-test-with-stores.yaml` (parser fixture) | 1-test suite |
| `defaults-test.yaml` (parser fixture) | 1-test suite |
| `processor-filtering-transforming-complete-test/*.yaml` (5 files) | 1 multi-test suite at top level |

The 5-file directory becomes `processor-filtering-transforming-complete-test.yaml` at the resources root. The subdirectory is deleted. `KSMLTestRunnerTest.filteringTransformingCompleteTestsAllPass` is removed; the new file becomes one entry in `validTestsReturnPass`'s `@ValueSource`.

The deliberately-invalid parser fixtures get updated for the new format:
- `missing-test-root.yaml` becomes `missing-tests.yaml` (no `tests:` map).
- `missing-name.yaml` is deleted (suite-level `name` is now optional).
- `missing-produce.yaml` becomes a suite with one test that has no `produce:` block.
- `assert-no-topic-no-stores.yaml` keeps its semantics, just under the new shape (assert with neither `on:` nor `stores:`).

New invalid fixtures cover new failure modes:
- `invalid-test-key.yaml`: a test key that doesn't match the identifier regex.
- `invalid-stream-key.yaml`: a stream key that doesn't match the identifier regex.
- `duplicate-test-key.yaml`: two entries with the same key in `tests:`.
- `duplicate-stream-topic.yaml`: two entries in `streams:` with the same `topic` value.
- `undefined-stream-reference.yaml`: a `to:` or `on:` referencing a stream key not in `streams:`.
- `inline-topic-on-produce.yaml`: a produce block with a `topic` field instead of `to:`.
- `bare-vendor-avro.yaml`: a stream `valueType: confluent_avro` (no schema name).

## Risks / Trade-offs

- **Risk: Jackson YAML behavior on duplicate keys.** YAML technically allows duplicates; Jackson silently picks one. → Add an explicit duplicate-detection pass during parsing (count key occurrences in the raw YAML or use a strict reader configuration). Apply to both `streams:` and `tests:` maps.
- **Risk: Identifier regex turns out too strict.** If a real use case emerges that needs hyphens or dots in stream/test keys, we'd need to relax. → Defer until evidence of need; relaxing is additive and non-breaking.
- **Trade-off: Breaking every existing test resource at once.** Forces every sample, fixture, and integration test to migrate in the same PR. Higher PR diff, but better than a half-migrated codebase. The migration is mechanical (extract topic+types into `streams:`, replace topic-references with `to:`/`on:`).
- **Trade-off: Topic-uniqueness restriction in `streams:`.** A pipeline that genuinely uses two different schemas for the same topic key/value subject is impossible to test under this rule. We don't have such a pipeline today; this is a documented limitation that can be revisited if it ever matters.
- **Trade-off: Strict type rule rejects some KSML-legal forms.** A pipeline yaml's `tables:` block might use bare `confluent_avro` legally; the test yaml must qualify it. Authors copying types from a pipeline must occasionally adjust. The rule mirrors what `StreamDefinitionParser` already enforces, so the inconsistency is between pipeline-internal sections, not between pipelines and tests in any new way.

## Migration Plan

1. Implement the parser/runner changes (new types, new parser, new runner API).
2. Update the schema generator and regenerate `docs/ksml-test-spec.json`.
3. Migrate every YAML in `ksml-test-runner/src/test/resources/` to the new format. Mechanical conversion: extract topic+types into `streams:`, replace `produce[].topic` with `to:`, replace `assert[].topic` with `on:`, wrap the resulting produce/assert in `tests: { only_test: { ... } }`.
4. Migrate the 5-file `processor-filtering-transforming-complete-test/` directory into a single suite file at the resources root.
5. Update `KSMLTestRunnerTest`: remove `filteringTransformingCompleteTestsAllPass`, add the collapsed file to `validTestsReturnPass`, update both parameterized methods to assert across the new `List<TestResult>` return.
6. Update `TestDefinitionParserTest` for the new format and new failure-mode fixtures.
7. Update `ksml-test-runner/README.md` with new examples.
8. Run the full module test suite to verify all migrations work.

No rollback strategy needed (pre-release).

## Open Questions

(none remaining at design time — all earlier ambiguities resolved)
