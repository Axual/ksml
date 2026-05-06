# test-definition-format Specification

## Purpose
TBD - created by archiving change ksml-test-runner. Update Purpose after archive.
## Requirements
### Requirement: Test definition YAML structure
A test definition YAML file SHALL contain a `test` root element with the following fields: `name` (string, required), `pipeline` (classpath-relative path to a KSML pipeline definition, required), `schemaDirectory` (path to Avro schema files, optional), `produce` (list of produce blocks, required), and `assert` (list of assertion blocks, required).

#### Scenario: Minimal valid test definition
- **WHEN** a YAML file contains a `test` element with `name`, `pipeline`, one `produce` block, and one `assert` block
- **THEN** the test runner SHALL accept it as a valid test definition

#### Scenario: Missing required fields
- **WHEN** a YAML file is missing `name`, `pipeline`, `produce`, or `assert`
- **THEN** the test runner SHALL reject it with a clear error message identifying the missing field

### Requirement: Inline test messages
Each `produce` block SHALL support a `messages` list where each entry contains `key` (required), `value` (required), and `timestamp` (optional, epoch milliseconds). The produce block SHALL also declare `topic` (required), `keyType` (required), and `valueType` (required).

#### Scenario: Produce inline messages without timestamps
- **WHEN** a produce block contains messages without `timestamp` fields
- **THEN** the test runner SHALL send each message to the TopologyTestDriver input topic using auto-advancing time

#### Scenario: Produce inline messages with explicit timestamps
- **WHEN** a produce block contains messages with `timestamp` fields
- **THEN** the test runner SHALL send each message with the specified timestamp (epoch millis) to the TopologyTestDriver input topic

#### Scenario: Mixed timestamps
- **WHEN** some messages in a produce block have `timestamp` and others do not
- **THEN** messages with `timestamp` SHALL use the specified time, and messages without SHALL use auto-advancing time

### Requirement: Generator-based test data
Each `produce` block SHALL optionally support a `generator` element (using KSML generator function syntax) with an optional `count` field, as an alternative to inline `messages`.

#### Scenario: Produce data via generator function
- **WHEN** a produce block contains a `generator` definition and `count`
- **THEN** the test runner SHALL invoke the Python generator function `count` times and pipe the resulting key/value pairs into the TopologyTestDriver input topic

### Requirement: Multiple produce blocks
A test definition SHALL support multiple `produce` blocks targeting different input topics, to support pipelines that consume from multiple sources (e.g., joins).

#### Scenario: Stream-table join test data
- **WHEN** a test definition contains two produce blocks targeting different topics
- **THEN** the test runner SHALL produce messages to both input topics before running assertions

### Requirement: Assertion blocks with output records
Each `assert` block SHALL optionally declare a `topic` field. When present, the test runner SHALL read all records from that output topic and inject them as a `records` Python list variable into the assertion code.

#### Scenario: Assert on output topic records
- **WHEN** an assert block declares a `topic` and `code`
- **THEN** the `records` variable in the Python code SHALL contain all records from that output topic, where each record has `key` and `value` attributes

### Requirement: Assertion blocks with state stores
Each `assert` block SHALL optionally declare a `stores` list of state store names. The test runner SHALL fetch each store from the TopologyTestDriver, wrap it using `StateStoreProxyFactory.wrap()`, and inject it as a Python global variable with the store's name.

#### Scenario: Assert on key-value state store
- **WHEN** an assert block declares `stores: [my_store]` and the pipeline defines a keyValue store named `my_store`
- **THEN** the Python code SHALL have access to `my_store` with the same API as in pipeline Python functions (e.g., `my_store.get(key)`)

#### Scenario: Assert on windowed state store
- **WHEN** an assert block declares a windowed store name
- **THEN** the Python code SHALL have access to the store via the appropriate `WindowStoreProxy` with `fetch(key, time)` support

#### Scenario: Assert with both records and stores
- **WHEN** an assert block declares both `topic` and `stores`
- **THEN** the Python code SHALL have access to both `records` and the named store variables

#### Scenario: Assert with stores only (no output topic)
- **WHEN** an assert block declares `stores` but no `topic`
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

