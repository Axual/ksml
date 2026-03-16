## 1. Module Setup

- [ ] 1.1 Create `ksml-test-runner/` directory with `pom.xml` declaring dependencies on `ksml`, `kafka-streams-test-utils` (compile scope), Picocli, Jackson YAML, SLF4J/Logback
- [ ] 1.2 Add `ksml-test-runner` to the root `pom.xml` modules list
- [ ] 1.3 Configure `maven-jar-plugin` with Main-Class manifest and `maven-dependency-plugin` to copy dependencies to `libs/` (same pattern as `ksml-runner`)

## 2. Test Definition Parsing

- [ ] 2.1 Create `TestDefinition` data class representing the parsed test YAML (name, pipeline, schemaDirectory, produce blocks, assert blocks)
- [ ] 2.2 Create `ProduceBlock` data class (topic, keyType, valueType, messages list, optional generator)
- [ ] 2.3 Create `TestMessage` data class (key, value, optional timestamp)
- [ ] 2.4 Create `AssertBlock` data class (optional topic, optional stores list, code)
- [ ] 2.5 Create `TestDefinitionParser` that reads YAML and produces a `TestDefinition`, with validation for required fields and clear error messages

## 3. Execution Context Setup

- [ ] 3.1 Create `TestExecutionContext` class that registers Binary, JSON, and Avro notations using `MockConfluentSchemaRegistryClient` (extract pattern from `KSMLTestExtension.beforeAll()` and `beforeEach()`)
- [ ] 3.2 Add schema directory registration support
- [ ] 3.3 Add context reset/cleanup method for test isolation

## 4. Test Data Production

- [ ] 4.1 Create `TestDataProducer` that takes a `ProduceBlock` and pipes inline messages into a `TestInputTopic` via TopologyTestDriver
- [ ] 4.2 Implement key/value conversion from YAML maps to typed DataObjects using existing `NativeDataObjectMapper` / notation infrastructure
- [ ] 4.3 Implement optional timestamp support: use `pipeInput(key, value, Instant)` when timestamp is present, `pipeInput(key, value)` when absent
- [ ] 4.4 Support multiple produce blocks targeting different input topics
- [ ] 4.5 Add generator-based production: invoke Python generator function via `PythonContext`, pipe results into TestInputTopic

## 5. Assertion Execution

- [ ] 5.1 Create `AssertionRunner` that sets up a `PythonContext` for assertion code execution
- [ ] 5.2 Implement output record collection: read records from TopologyTestDriver output topic, convert to Python-accessible objects (PythonDict/PythonList) with `key` and `value` attributes, inject as `records` variable
- [ ] 5.3 Implement state store injection: for each store name in the assert block, fetch from TopologyTestDriver, wrap via `StateStoreProxyFactory.wrap()`, inject as Python global variable
- [ ] 5.4 Implement `log` variable injection using existing `LoggerBridge`
- [ ] 5.5 Execute assertion code as Python script, catch `AssertionError` (test failure) and other exceptions (test error), return structured `TestResult`

## 6. Test Runner Main Class

- [ ] 6.1 Create `KSMLTestRunner` main class with Picocli argument parsing (positional file args with `arity = "1..*"`)
- [ ] 6.2 Add GraalVM availability check at startup (pattern from `KSMLTestExtension.evaluateExecutionCondition()`)
- [ ] 6.3 Implement `runSingleTest(Path) → TestResult` orchestrating: parse → context setup → topology build → produce → assert → cleanup
- [ ] 6.4 Implement plain text result reporting to stdout (test name + PASS/FAIL/ERROR, assertion messages on failure)
- [ ] 6.5 Implement exit code: 0 for all pass, 1 for any failure/error

## 7. Docker Integration

- [ ] 7.1 Update `Dockerfile` to copy `ksml-test-runner` JAR as `/opt/ksml/ksml-test.jar`
- [ ] 7.2 Document the test runner entrypoint override in Dockerfile comments or README

## 8. Validation

- [ ] 8.1 Create a sample test definition YAML that tests an existing pipeline (e.g., filter pipeline with SensorData)
- [ ] 8.2 Create a sample test definition that asserts on state store contents
- [ ] 8.3 Create a sample test definition using explicit timestamps for windowed operations
- [ ] 8.4 Verify the test runner executes all sample tests successfully
- [ ] 8.5 Verify `mvn clean package` builds the full project including `ksml-test-runner`
