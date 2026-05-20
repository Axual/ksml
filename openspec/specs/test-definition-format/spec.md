# test-definition-format Specification

## Purpose
TBD - created by archiving change ksml-test-runner. Update Purpose after archive.
## Requirements
### Requirement: Test definition YAML structure
A test definition YAML file SHALL be a flat document with no outer wrapper element. The following fields SHALL appear at the file root: `name` (string, optional, falls back to filename without extension), `definition` (path to a KSML pipeline definition, required), `schemaDirectory` (path to schema files, optional), `moduleDirectory` (path to externalized Python modules, optional), `streams` (map of named topic+type declarations, optional but practically required for any non-trivial test), and `tests` (map of test entries keyed by stable identifier, required, must contain at least one entry).

#### Scenario: Minimal valid test definition
- **WHEN** a YAML file contains a `definition` field, a `streams:` map declaring at least the topics referenced by the tests, and a non-empty `tests:` map with at least one test entry having `produce` and `assert`
- **THEN** the test runner SHALL accept it as a valid test definition

#### Scenario: Missing required fields
- **WHEN** a YAML file is missing `definition` or `tests`
- **THEN** the test runner SHALL reject it with a clear error message identifying the missing field

#### Scenario: Empty tests map
- **WHEN** a YAML file contains an empty `tests: {}` map or omits the `tests` key entirely
- **THEN** the test runner SHALL reject it with an error stating that at least one test is required

#### Scenario: Outer wrapper element rejected
- **WHEN** a YAML file places the test fields under an outer `test:` element rather than at the file root
- **THEN** the test runner SHALL reject it because the expected root-level fields are missing

### Requirement: Multiple produce blocks
Each test entry under `tests:` SHALL support multiple `produce` blocks targeting different streams declared in the suite's `streams:` map, to support pipelines that consume from multiple sources (e.g., joins).

#### Scenario: Stream-table join test data
- **WHEN** a single test entry contains two produce blocks each targeting a different stream key
- **THEN** the test runner SHALL produce messages to both underlying topics before running that test's assertions

### Requirement: Assertion blocks with state stores
Each `assert` block SHALL optionally declare a `stores` list of state store names. The test runner SHALL fetch each store from the TopologyTestDriver, wrap it using `StateStoreProxyFactory.wrap()`, and inject it as a Python global variable with the store's name. State stores are referenced by their plain string names (the names declared in the pipeline's `stores:` block) and do not appear under the suite-level `streams:` map.

#### Scenario: Assert on key-value state store
- **WHEN** an assert block declares `stores: [my_store]` and the pipeline defines a keyValue store named `my_store`
- **THEN** the Python code SHALL have access to `my_store` with the same API as in pipeline Python functions (e.g., `my_store.get(key)`)

#### Scenario: Assert on windowed state store
- **WHEN** an assert block declares a windowed store name in `stores`
- **THEN** the Python code SHALL have access to the store via the appropriate `WindowStoreProxy` with `fetch(key, time)` support

#### Scenario: Assert with both records and stores
- **WHEN** an assert block declares both `on:` and `stores:`
- **THEN** the Python code SHALL have access to both `records` (deserialized output of the stream referenced by `on:`) and the named store variables

#### Scenario: Assert with stores only (no stream reference)
- **WHEN** an assert block declares `stores:` but no `on:`
- **THEN** the Python code SHALL have access to the named store variables without a `records` variable

### Requirement: Python assertion execution
The `code` field in each assert block SHALL be executed as a Python script in a GraalVM sandbox. The script SHALL have access to injected variables (`records`, stores, `log`) and SHALL use Python's `assert` statement for validation.

#### Scenario: Assertion passes
- **WHEN** all `assert` statements in the Python code succeed
- **THEN** the test SHALL be reported as passed

#### Scenario: Assertion fails
- **WHEN** any `assert` statement raises an `AssertionError`
- **THEN** the test SHALL be reported as failed with the assertion error message

#### Scenario: Python runtime error
- **WHEN** the assertion code raises a non-assertion exception (e.g., `KeyError`, `TypeError`)
- **THEN** the test SHALL be reported as errored with the exception details

### Requirement: Test suite with named tests
A test definition file SHALL contain a `tests` map where each entry represents one hermetic test against the suite's shared pipeline. Each map key SHALL be a test identifier matching the regex `^[a-zA-Z][a-zA-Z0-9_]*$`. Each map value SHALL contain a `produce` list (required), an `assert` list (required), and an optional `description` field (string). When `description` is absent, downstream tooling SHALL use the map key as the display label for the test.

#### Scenario: Valid test keys accepted
- **WHEN** a test key matches `^[a-zA-Z][a-zA-Z0-9_]*$` (e.g., `valid_data`, `testCase1`, `t1`)
- **THEN** the parser SHALL accept the key

#### Scenario: Invalid test key rejected
- **WHEN** a test key contains characters outside `[a-zA-Z0-9_]`, contains spaces, starts with a digit, or starts with an underscore (e.g., `"test with spaces"`, `1_test`, `_hidden`, `with-dash`)
- **THEN** the parser SHALL reject the file with an error identifying the offending key and stating the required pattern

#### Scenario: Description fallback to key
- **WHEN** a test entry has no `description` field
- **THEN** reporting and downstream tooling SHALL use the test key as the display label

#### Scenario: Description used for display when set
- **WHEN** a test entry sets `description: "Human readable label"`
- **THEN** reporting SHALL use that description as the display label while preserving the key as the stable identifier

#### Scenario: Test entry missing produce
- **WHEN** a test entry omits the `produce` field
- **THEN** the parser SHALL reject the file with an error identifying the offending test key

#### Scenario: Test entry missing assert
- **WHEN** a test entry omits the `assert` field
- **THEN** the parser SHALL reject the file with an error identifying the offending test key

### Requirement: Suite-level shared configuration
The fields `definition`, `schemaDirectory`, `moduleDirectory`, `streams`, and the optional suite `name` SHALL appear at the file root and apply uniformly to every test in the suite. These fields SHALL NOT be permitted under individual test entries.

#### Scenario: Shared pipeline applies to every test
- **WHEN** a suite has multiple tests and a single root-level `definition:` field
- **THEN** every test in the suite SHALL run against that pipeline

#### Scenario: Shared streams apply to every test
- **WHEN** a suite has multiple tests and a single root-level `streams:` map
- **THEN** every test SHALL resolve its `to:` and `on:` references against that map

#### Scenario: Suite-level field at test level rejected
- **WHEN** a test entry contains a field reserved for the suite level (e.g., `definition:` or `streams:` inside a test entry)
- **THEN** the parser SHALL reject the file with an error identifying the misplaced field

### Requirement: Test order preservation
The order of test entries in the YAML `tests:` map SHALL be preserved when running and reporting tests.

#### Scenario: Tests run and report in YAML order
- **WHEN** a suite defines tests in YAML order `a`, `b`, `c`
- **THEN** the runner SHALL execute and report results in the same order `a`, `b`, `c`

### Requirement: Duplicate test key rejection
The parser SHALL reject a test definition file containing duplicate keys in the `tests:` map.

#### Scenario: Duplicate keys rejected
- **WHEN** a `tests:` map contains the same key twice (e.g., two entries both keyed `t1`)
- **THEN** the parser SHALL reject the file with an error identifying the duplicate key

### Requirement: Streams declarations
A test definition file SHALL declare named topic-and-type bindings under the optional root-level `streams:` map. Each map key is a stream identifier matching the same regex used for test keys (`^[a-zA-Z][a-zA-Z0-9_]*$`). Each map value SHALL contain `topic` (Kafka topic name, required), `keyType` (a type string, optional, defaults to `string`), and `valueType` (a type string, optional, defaults to `string`). The `streams:` map serves two roles: it provides the type information that drives serdes for produce/assert blocks, and for schema-bearing notations it identifies the schema to register in the in-memory mock registry under `<topic>-key`/`<topic>-value` subjects.

#### Scenario: Valid stream keys accepted
- **WHEN** a stream key matches `^[a-zA-Z][a-zA-Z0-9_]*$`
- **THEN** the parser SHALL accept the key

#### Scenario: Invalid stream key rejected
- **WHEN** a stream key violates the identifier regex
- **THEN** the parser SHALL reject the file with an error identifying the offending key

#### Scenario: Schema registration for vendor-avro stream
- **WHEN** a stream entry has `valueType: "avro:SensorData"` and `schemaDirectory` resolves a `SensorData.avsc`
- **THEN** the schema SHALL be loaded and registered in the in-memory mock registry under the subject `<topic>-value`

#### Scenario: Default key/value type
- **WHEN** a stream entry omits `keyType` or `valueType`
- **THEN** the parser SHALL apply the default `string` for the omitted field

#### Scenario: Two streams with the same topic rejected
- **WHEN** the `streams:` map contains two entries with the same `topic:` value
- **THEN** the parser SHALL reject the file because the mock registry can only register one schema per topic per side, and topic-level type declarations must be unambiguous

### Requirement: Type strings parseable without unresolved schemas
Every `keyType` and `valueType` value in the `streams:` map SHALL parse successfully under KSML's standard type grammar (`UserTypeParser`) with `allowUnresolved=false` — the same configuration KSML uses for stream definitions in pipeline files. Schema-bearing notations (`avro`, `confluent_avro`, `apicurio_avro`, `protobuf` and its vendor variants, `json_schema` and its vendor variants, `csv`) MUST be qualified with a schema name (e.g., `avro:SensorData`). Schemaless notations (`string`, `long`, `int`, `boolean`, `bytes`, `json`, `binary`, `xml`, `soap`) MAY be used unqualified. Composite types (`list(...)`, `tuple(...)`, `map(...)`, `union(...)`) MAY be used and their inner element types are subject to the same rules.

#### Scenario: Schemaless notation accepted unqualified
- **WHEN** a stream entry sets `valueType: json`
- **THEN** the parser SHALL accept it

#### Scenario: Schema-bearing notation accepted with schema name
- **WHEN** a stream entry sets `valueType: "avro:SensorData"` and `schemaDirectory` is configured
- **THEN** the parser SHALL accept it and register the schema for the underlying topic

#### Scenario: Bare schema-bearing notation rejected
- **WHEN** a stream entry sets `valueType: confluent_avro` (without a schema name)
- **THEN** the parser SHALL reject it with an error referencing the required `<notation>:<SchemaName>` form

#### Scenario: Schemaless notation with schema name rejected
- **WHEN** a stream entry sets `valueType: "json:SensorData"` (json is schemaless-only)
- **THEN** the parser SHALL reject it with an error stating that the notation does not accept a schema name

### Requirement: Stream-referenced produce blocks
Each entry in a test's `produce` list SHALL identify its target stream via the `to:` field, whose value is a string that MUST be a key in the suite's `streams:` map. The runner SHALL resolve `to:` to the corresponding topic and serdes.

#### Scenario: produce references existing stream
- **WHEN** a produce block sets `to: sensor_source` and `streams.sensor_source` is defined
- **THEN** the runner SHALL produce the block's messages into `streams.sensor_source.topic` using its declared serdes

#### Scenario: produce references undefined stream
- **WHEN** a produce block sets `to: nonexistent_stream` and that key is not in `streams:`
- **THEN** the parser SHALL reject the file with an error identifying the offending reference

#### Scenario: Produce block missing target stream
- **WHEN** a produce block omits the `to:` field
- **THEN** the parser SHALL reject the file with an error stating that `to:` is required

### Requirement: Stream-referenced assertion blocks
Each entry in a test's `assert` list MAY identify its source stream via the `on:` field, whose value is a string that MUST be a key in the suite's `streams:` map. When `on:` is set, the runner SHALL read all output records from the corresponding topic and inject them as a Python `records` list variable in the assertion code, using the stream's declared serdes for deserialization.

#### Scenario: assert references existing stream
- **WHEN** an assert block sets `on: sensor_filtered` and `streams.sensor_filtered` is defined
- **THEN** the runner SHALL inject `records` containing the deserialized output of `streams.sensor_filtered.topic`

#### Scenario: assert references undefined stream
- **WHEN** an assert block sets `on: nonexistent_stream` and that key is not in `streams:`
- **THEN** the parser SHALL reject the file with an error identifying the offending reference

#### Scenario: assert with stores only requires no stream reference
- **WHEN** an assert block declares `stores:` but no `on:`
- **THEN** the parser SHALL accept it; the assertion runs against the named state stores without injecting a `records` variable

