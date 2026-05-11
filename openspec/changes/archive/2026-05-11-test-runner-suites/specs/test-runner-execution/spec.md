## MODIFIED Requirements

### Requirement: Test execution lifecycle
The test runner SHALL execute each test definition file by: (1) parsing the suite YAML and resolving its `streams:` declarations, (2) parsing the referenced pipeline YAML once for the file, (3) iterating over the tests in YAML-defined order, and for each test (4) setting up the execution context with notations and schema/module directories, (5) building the Kafka Streams topology and creating a fresh `TopologyTestDriver`, (6) producing the test's data into the streams referenced by `to:`, (7) collecting output from streams referenced by `on:` and running the test's Python assertions, (8) recording the test result, (9) closing the driver and cleaning up the execution context. After every test in the file is processed, the runner SHALL produce a list of per-test results.

#### Scenario: Successful suite execution
- **WHEN** the runner is given a valid suite file containing N tests against a valid pipeline
- **THEN** it SHALL execute all N tests in YAML order and produce N test results

#### Scenario: One test in a suite fails
- **WHEN** one test in a suite of N tests fails or errors
- **THEN** the remaining tests SHALL still execute, and the result list SHALL contain N entries with appropriate per-test statuses

#### Scenario: Pipeline parse failure
- **WHEN** the referenced pipeline YAML cannot be parsed
- **THEN** the runner SHALL report a single error result for the entire suite and exit with code 1

#### Scenario: Pipeline file not found
- **WHEN** the referenced pipeline YAML file does not exist
- **THEN** the runner SHALL report a single error result identifying the missing file and exit with code 1

#### Scenario: Suite YAML parse failure
- **WHEN** the suite YAML itself is malformed or fails validation
- **THEN** the runner SHALL report a single error result identifying the suite file and the parse error, and exit with code 1

#### Scenario: Stream reference resolution failure
- **WHEN** a test references a stream key via `to:` or `on:` that is not defined in the suite's `streams:` map
- **THEN** the runner SHALL report a single error result identifying the offending reference and exit with code 1

### Requirement: Execution context setup
The test runner SHALL register Binary, JSON, and Avro notations in `ExecutionContext.INSTANCE` before each test, using `MockConfluentSchemaRegistryClient` for Avro serde. If a `schemaDirectory` is specified, it SHALL be registered in the schema library. Schemas referenced by stream entries with schema-bearing types SHALL be loaded from the schema directory and registered in the mock registry under the conventional `<topic>-key`/`<topic>-value` subjects.

#### Scenario: Avro schema resolution
- **WHEN** a test definition specifies `schemaDirectory: schemas` and a stream entry uses `valueType: "avro:SensorData"`
- **THEN** the Avro schema SHALL be resolved from the specified directory and registered under `<topic>-value` in the mock registry

#### Scenario: No schema directory
- **WHEN** a test definition does not specify `schemaDirectory` and no stream uses a schema-bearing notation
- **THEN** the runner SHALL proceed without schema directory registration

### Requirement: Execution context isolation
The test runner SHALL reset and re-register the execution context before every test, whether the test is the only one in its file or one of many in a suite. Each test SHALL run against a fresh `TopologyTestDriver`; state stores, output topics, and time advance independently from prior tests.

#### Scenario: Sequential tests in a suite
- **WHEN** multiple tests are defined in a single suite file
- **THEN** each test SHALL start with a clean execution context and a fresh `TopologyTestDriver`, unaffected by prior tests in the same file

#### Scenario: Sequential test files
- **WHEN** multiple test files are provided to the runner
- **THEN** each file's tests SHALL run independently of prior files' state

### Requirement: Plain text result reporting
The test runner SHALL output results as plain text to stdout. Each result SHALL be labeled `<suite> › <test>` where `<suite>` is the file's `name` field (or filename without extension as fallback) and `<test>` is the per-test `description` (or test key as fallback). For each result the output SHALL include the qualified label and a PASS/FAIL/ERROR status. For FAIL or ERROR results, the output SHALL also include the assertion error message or exception details. The runner SHALL exit with code 0 if every result is PASS and with code 1 if any result is FAIL or ERROR.

#### Scenario: Passing test output
- **WHEN** a test passes
- **THEN** stdout SHALL contain `<suite> › <test>` together with a PASS indicator

#### Scenario: Failing test output
- **WHEN** a test fails due to an assertion error
- **THEN** stdout SHALL contain `<suite> › <test>`, a FAIL indicator, and the assertion error message

#### Scenario: Errored test output
- **WHEN** a test errors due to a runtime exception
- **THEN** stdout SHALL contain `<suite> › <test>`, an ERROR indicator, and the exception details

#### Scenario: Suite without explicit name
- **WHEN** a suite file has no `name` field
- **THEN** the suite portion of the label SHALL be the filename without its extension

#### Scenario: Test without explicit description
- **WHEN** a test entry has no `description` field
- **THEN** the test portion of the label SHALL be the test key
