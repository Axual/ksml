# test-runner-execution Specification

## Purpose
TBD - created by archiving change ksml-test-runner. Update Purpose after archive.
## Requirements
### Requirement: Test execution lifecycle
The test runner SHALL execute a test definition through the following phases in order: parse test YAML, set up execution context (notations, schema directory), parse pipeline definition, build topology, create TopologyTestDriver, produce test data, collect output, run Python assertions, report result, clean up.

#### Scenario: Successful test execution
- **WHEN** the runner is given a valid test definition with a valid pipeline
- **THEN** it SHALL execute all phases in order and report the test result

#### Scenario: Pipeline parse failure
- **WHEN** the referenced pipeline YAML cannot be parsed
- **THEN** the runner SHALL report an error with the parse failure details and exit with code 1

#### Scenario: Pipeline file not found
- **WHEN** the referenced pipeline YAML file does not exist
- **THEN** the runner SHALL report an error identifying the missing file and exit with code 1

### Requirement: Execution context setup
The test runner SHALL register Binary, JSON, and Avro notations in `ExecutionContext.INSTANCE` before each test, using `MockConfluentSchemaRegistryClient` for Avro serde. If a `schemaDirectory` is specified, it SHALL be registered in the schema library.

#### Scenario: Avro schema resolution
- **WHEN** a test definition specifies `schemaDirectory: schemas` and the pipeline uses `avro:SensorData`
- **THEN** the Avro schema SHALL be resolved from the specified directory

#### Scenario: No schema directory
- **WHEN** a test definition does not specify `schemaDirectory`
- **THEN** the runner SHALL proceed without schema directory registration (supporting non-Avro pipelines)

### Requirement: Execution context isolation
The test runner SHALL reset and re-register the execution context before each test to ensure test isolation.

#### Scenario: Sequential test execution
- **WHEN** multiple tests are executed sequentially (future suite mode)
- **THEN** each test SHALL start with a clean execution context, unaffected by prior tests

### Requirement: GraalVM availability check
The test runner SHALL verify that it is running on GraalVM before attempting to execute tests. If GraalVM is not available, it SHALL report a clear error and exit with code 1.

#### Scenario: Running on GraalVM
- **WHEN** the runner detects a GraalVM runtime
- **THEN** it SHALL proceed with test execution

#### Scenario: Running on non-GraalVM JVM
- **WHEN** the runner detects a non-GraalVM runtime
- **THEN** it SHALL print an error message stating that GraalVM is required and exit with code 1

### Requirement: Plain text result reporting
The test runner SHALL output results as plain text to stdout. For each test: the test name and pass/fail status. For failures: the assertion error message or exception details. The runner SHALL exit with code 0 if all tests pass and code 1 if any test fails or errors.

#### Scenario: Passing test output
- **WHEN** a test passes
- **THEN** stdout SHALL contain the test name and a PASS indicator

#### Scenario: Failing test output
- **WHEN** a test fails due to an assertion error
- **THEN** stdout SHALL contain the test name, a FAIL indicator, and the assertion error message

#### Scenario: Errored test output
- **WHEN** a test errors due to a runtime exception
- **THEN** stdout SHALL contain the test name, an ERROR indicator, and the exception details

### Requirement: Single-file CLI interface
The test runner SHALL accept one or more file paths as positional arguments. In the MVP, it SHALL process the provided file(s) sequentially.

#### Scenario: Run single test file
- **WHEN** invoked with `java -jar ksml-test.jar test-filter.yaml`
- **THEN** it SHALL execute the test defined in `test-filter.yaml` and report the result

#### Scenario: No arguments
- **WHEN** invoked with no arguments
- **THEN** it SHALL print usage information and exit with code 1

