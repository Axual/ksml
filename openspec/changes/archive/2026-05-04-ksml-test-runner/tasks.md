## 1. Module Setup

- [x] 1.1 Create `ksml-test-runner/` directory with `pom.xml` declaring dependencies on `ksml`, `kafka-streams-test-utils` (compile scope), Picocli, Jackson YAML, SLF4J/Logback
- [x] 1.2 Add `ksml-test-runner` to the root `pom.xml` modules list
- [x] 1.3 Configure `maven-jar-plugin` with Main-Class manifest and `maven-dependency-plugin` to copy dependencies to `libs/` (same pattern as `ksml-runner`)

## 2. Test Definition Parsing

- [x] 2.1 Create `TestDefinition` data class representing the parsed test YAML (name, pipeline, schemaDirectory, produce blocks, assert blocks)
- [x] 2.2 Create `ProduceBlock` data class (topic, keyType, valueType, messages list, optional generator)
- [x] 2.3 Create `TestMessage` data class (key, value, optional timestamp)
- [x] 2.4 Create `AssertBlock` data class (optional topic, optional stores list, code)
- [x] 2.5 Create `TestDefinitionParser` that reads YAML and produces a `TestDefinition`, with validation for required fields and clear error messages

## 3. Execution Context Setup

- [x] 3.1 Create `TestExecutionContext` class that registers Binary, JSON, and Avro notations using `MockConfluentSchemaRegistryClient` (extract pattern from `KSMLTestExtension.beforeAll()` and `beforeEach()`)
- [x] 3.2 Add schema directory registration support
- [x] 3.3 Add context reset/cleanup method for test isolation

## 4. Test Data Production

- [x] 4.1 Create `TestDataProducer` that takes a `ProduceBlock` and pipes inline messages into a `TestInputTopic` via TopologyTestDriver
- [x] 4.2 Implement key/value conversion from YAML maps to typed DataObjects using existing `NativeDataObjectMapper` / notation infrastructure
- [x] 4.3 Implement optional timestamp support: use `pipeInput(key, value, Instant)` when timestamp is present, `pipeInput(key, value)` when absent
- [x] 4.4 Support multiple produce blocks targeting different input topics
- [x] 4.5 Add generator-based production: invoke Python generator function via `PythonContext`, pipe results into TestInputTopic

## 5. Assertion Execution

- [x] 5.1 Create `AssertionRunner` that sets up a `PythonContext` for assertion code execution
- [x] 5.2 Implement output record collection: read records from TopologyTestDriver output topic, convert to Python-accessible objects (PythonDict/PythonList) with `key` and `value` attributes, inject as `records` variable
- [x] 5.3 Implement state store injection: for each store name in the assert block, fetch from TopologyTestDriver, wrap via `StateStoreProxyFactory.wrap()`, inject as Python global variable
- [x] 5.4 Implement `log` variable injection using existing `LoggerBridge`
- [x] 5.5 Execute assertion code as Python script, catch `AssertionError` (test failure) and other exceptions (test error), return structured `TestResult`

## 6. Test Runner Main Class

- [x] 6.1 Create `KSMLTestRunner` main class with Picocli argument parsing (positional file args with `arity = "1..*"`)
- [x] 6.2 Add GraalVM availability check at startup (pattern from `KSMLTestExtension.evaluateExecutionCondition()`)
- [x] 6.3 Implement `runSingleTest(Path) → TestResult` orchestrating: parse → context setup → topology build → produce → assert → cleanup
- [x] 6.4 Implement plain text result reporting to stdout (test name + PASS/FAIL/ERROR, assertion messages on failure)
- [x] 6.5 Implement exit code: 0 for all pass, 1 for any failure/error

## 7. Docker Integration

- [x] 7.1 Update `Dockerfile` to copy `ksml-test-runner` JAR as `/opt/ksml/ksml-test.jar`
- [x] 7.2 Document the test runner entrypoint override in Dockerfile comments or README

## 8. Validation

- [x] 8.1 Create a sample test definition YAML that tests an existing pipeline (e.g., filter pipeline with SensorData)
- [x] 8.2 Create a sample test definition that asserts on state store contents
- [x] 8.3 Create a sample test definition using explicit timestamps for windowed operations
- [x] 8.4 Verify the test runner executes all sample tests successfully
- [x] 8.5 Verify `mvn clean package` builds the full project including `ksml-test-runner`
