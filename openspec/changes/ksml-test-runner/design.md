## Context

KSML provides a YAML+Python interface for defining Kafka Streams topologies. The existing test infrastructure (`KSMLTestExtension`, `@KSMLTest`, `@KSMLTopic`) is Java-centric: tests are JUnit 5 classes with Java assertions. This works well for the KSML project's own development, but KSML end-users who write pipelines in YAML with Python functions have no way to test their work without writing Java.

The codebase already contains all the building blocks needed for a standalone test runner:
- `TopologyDefinitionParser` + `TopologyGenerator` — parse YAML and build Kafka Streams topologies
- `TopologyTestDriver` (from `kafka-streams-test-utils`) — run topologies without a broker
- `PythonContext` + `PythonFunction` — execute Python in a GraalVM sandbox
- `StateStoreProxyFactory` — wrap state stores for safe Python access via `@HostAccess.Export`
- `MockConfluentSchemaRegistryClient` — Avro serde without a real registry
- Notation registration patterns — from `KSMLTestExtension.beforeAll()`

The gap is an orchestrator that ties these together, driven by a YAML test definition instead of Java annotations.

## Goals / Non-Goals

**Goals:**
- Enable KSML users to test pipelines using only YAML and Python (no Java)
- Provide a standalone executable that can run in Docker alongside the existing KSML runner
- Reuse existing KSML infrastructure (parsers, topology generation, Python context, store proxies)
- Support inline test messages with optional timestamps for windowed operations
- Support Python assertions on both output topic records and state store contents
- Design for future multi-file test suite execution from day one

**Non-Goals:**
- Replacing the existing Java test framework (`KSMLTestExtension`) — it serves KSML development well
- JUnit or Maven integration — this is a standalone tool
- Live Kafka broker testing — TopologyTestDriver only
- Performance/load testing
- Modifying any existing KSML modules

## Decisions

### Separate Maven module and JAR
**Rationale:** `kafka-streams-test-utils` (which provides `TopologyTestDriver`) is conventionally a test-scoped dependency. Putting it in a dedicated `ksml-test-runner` module keeps it out of the production `ksml-runner` classpath. This follows the project's existing module-per-concern pattern.
**Alternative considered:** Adding a `--test` subcommand to the existing `ksml-runner` JAR. Rejected because it would pull test-scoped dependencies into production and mix concerns.

### Composable single-test execution
**Rationale:** The core execution unit is `runSingleTest(path) → TestResult`. This is a pure function: parse YAML, set up context, run topology, assert, return result. This design naturally extends to suite mode later (`testFiles.stream().map(this::runSingleTest)`) and keeps isolation between tests clean. The MVP processes one file; the CLI argument parsing (`arity = "1..*"`) already accepts multiple.
**Alternative considered:** Building suite support from the start. Deferred to keep MVP focused while the composable design ensures it's easy to add.

### Inline messages as primary test data format
**Rationale:** For tests, you want to know exactly what goes in to reason about what comes out. Explicit key/value pairs in YAML are the most natural and readable format. Generator functions (reusing producer definition syntax) are supported as an advanced option for volume/randomized testing.
**Alternative considered:** Reusing `ExecutableProducer` from `ksml-runner` directly. Rejected because it's coupled to `KafkaProducer.send()` (real broker API). Instead, we reuse the generator function invocation but pipe results into `TestInputTopic.pipeInput()`.

### Optional timestamps on test messages
**Rationale:** Without explicit timestamps, `TopologyTestDriver` uses auto-advancing wall-clock time, making windowed operations non-deterministic. An optional `timestamp` field (epoch millis) on each message is trivial to implement and essential for reliable windowed/timestamped store tests.

### Python assertions via PythonContext (not PythonFunction)
**Rationale:** Assertion code is a script block, not a typed function with parameters and return types. It's closer to how `globalCode` works in KSML function definitions — a block of Python that runs in context with injected variables (`records`, store proxies, `log`). We use `PythonContext` to set up the sandbox and inject variables, then execute the assertion code as a script. `AssertionError` exceptions are caught and reported as test failures.
**Alternative considered:** Wrapping assertions as `PythonFunction` instances with a boolean return type. Rejected because it's more complex and less natural — Python's `assert` statement is idiomatic for testing.

### State stores injected by name in assert block
**Rationale:** The assert block declares `stores: [store_name]`, mirroring how pipeline functions declare store access (`stores:` list). The runner fetches each store from `TopologyTestDriver.getKeyValueStore()` (etc.), wraps it via `StateStoreProxyFactory.wrap()`, and injects it as a Python global variable. This means assertion code uses the exact same store API as pipeline code — zero new API to learn.

### ExecutionContext lifecycle management
**Rationale:** `ExecutionContext.INSTANCE` is a global singleton holding notation and schema registries. Each test must get a clean context. The runner resets and re-registers notations before each test, following the pattern in `KSMLTestExtension.beforeAll()`. This ensures test isolation and prepares for suite mode where tests run sequentially.

### Plain text reporting
**Rationale:** For MVP and standalone use, plain text to stdout is the simplest and most universal format. Exit code 0 for all-pass, 1 for any failure. Structured formats (JSON, JUnit XML) can be added later behind a `--format` flag for CI integration.

### Picocli for CLI
**Rationale:** The existing `ksml-runner` already uses Picocli. Using it in the test runner maintains consistency and gives us argument parsing, help text, and future subcommand support for free.

## Risks / Trade-offs

- **[Risk] ExecutionContext singleton makes parallelization hard** → Mitigation: sequential execution is fine for MVP and likely for v2. If parallel execution is needed later, ExecutionContext would need refactoring (a larger project affecting all of KSML).
- **[Risk] WindowStoreProxy.fetch(key, timeFrom, timeTo) throws UnsupportedOperationException** → Mitigation: single-point `fetch(key, time)` works today. Range queries require a small separate enhancement to the proxy layer in `ksml/` core, not part of this change.
- **[Risk] GraalVM dependency** → Mitigation: same constraint as the existing KSML runner. The Docker image already includes GraalVM. The runner checks for GraalVM availability at startup (pattern from `KSMLTestExtension.evaluateExecutionCondition()`).
- **[Trade-off] TopologyTestDriver only, no broker** → Acceptable: this matches the existing test approach and is explicitly a non-goal to test against live Kafka.
- **[Trade-off] Schema registry is mocked** → Acceptable: uses `MockConfluentSchemaRegistryClient` like the existing test framework. Sufficient for serde validation.

## Open Questions

- Should the test runner support a shared configuration file (`--config`) for suite mode, or should each test YAML be self-contained?
- Should the Dockerfile use a single entrypoint with a subcommand (`ksml test ...`) or two separate JARs with distinct entrypoints?
