# KSML Test Runner

A standalone test runner for KSML pipeline definitions. It lets you test your pipelines using
YAML test definitions with Python assertions — no Java required.

The test runner uses Kafka's `TopologyTestDriver` under the hood, so no running Kafka broker is needed.

## How it works

You write a YAML file that describes:
1. Which pipeline to test
2. What data to produce into input topics
3. What to assert on output topics and/or state stores

The runner parses the pipeline, builds the Kafka Streams topology, pipes in test data,
and executes your Python assertions.

## Test definition format

A test definition file is a flat YAML document — no outer wrapper element — describing one
test suite (one pipeline plus one or more named tests). Each test runs against a fresh
`TopologyTestDriver`, so tests in the same suite are hermetic and do not share state.

```yaml
name: "Filter pipeline passes blue sensors"     # optional; falls back to filename without extension
definition: pipelines/test-filter.yaml          # path to the KSML pipeline definition YAML
schemaDirectory: schemas

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
  blue_sensors_pass:
    description: "Blue sensors pass through, red ones are filtered out"
    produce:
      - to: sensor_source
        messages:
          - key: "sensor-1"
            value:
              name: "sensor-1"
              timestamp: 1000
              value: "25.0"
              type: "TEMPERATURE"
              unit: "celsius"
              color: "blue"
    assert:
      - on: sensor_filtered
        code: |
          assert len(records) == 1, f"Expected 1 record, got {len(records)}"
```

### Suite-level fields

| Field | Required | Description |
|---|---|---|
| `name` | no | Human-readable suite name shown in reports. Falls back to filename without extension. |
| `definition` | yes | Path to the KSML pipeline definition YAML (relative to the test file, on the classpath, or absolute). |
| `schemaDirectory` | no | Path to schema files (relative to the test file or absolute). Required when any stream uses a schema-bearing notation like `avro:Foo`. |
| `moduleDirectory` | no | Path to externalized Python modules accessible to the pipeline. |
| `streams` | no | Map of named topic+type bindings, referenced by `to:` and `on:`. See below. |
| `tests` | yes | Map of named tests. Must contain at least one entry. See below. |

### Streams

The `streams:` map declares every Kafka topic the test suite produces to or asserts on.
Each entry is keyed by a logical stream identifier (matching `^[a-zA-Z][a-zA-Z0-9_]*$`)
and binds it to a topic plus key/value types.

| Field | Required | Description |
|---|---|---|
| `topic` | yes | Kafka topic name. Each topic may appear in at most one stream entry. |
| `keyType` | no | Key type string (default `string`). |
| `valueType` | no | Value type string (default `string`). |

Type strings follow KSML's standard type grammar (the same one `StreamDefinitionParser` uses
in pipeline yamls):

- Schema-less notations (`string`, `long`, `int`, `boolean`, `bytes`, `json`, `binary`, `xml`, etc.)
  may appear unqualified.
- Schema-bearing notations (`avro`, `confluent_avro`, `apicurio_avro`, `protobuf`, `json_schema`,
  `csv`) **must** be qualified with a schema name (e.g., `avro:SensorData`). The named schema is
  loaded from `schemaDirectory` and registered in the in-memory mock registry under
  `<topic>-key` / `<topic>-value`.
- Bare schema-bearing notations (e.g., `confluent_avro` without `:Schema`) are rejected — the
  test runner has no real registry to resolve them against.

### Tests

The `tests:` map carries one or more test entries. Each key is a stable test identifier
(matching `^[a-zA-Z][a-zA-Z0-9_]*$`); the value contains that test's produce data and assertions.
Tests run in YAML-defined order; failure in one test does not stop later tests.

| Field | Required | Description |
|---|---|---|
| `description` | no | Human-readable label. Falls back to the test key. |
| `produce` | yes | List of produce blocks. |
| `assert` | yes | List of assertion blocks. |

### Produce block

Each produce block targets one stream by reference:

| Field | Required | Description |
|---|---|---|
| `to` | yes | Stream key (must exist in `streams:`). |
| `messages` | * | List of messages with `key`, `value`, and optional `timestamp` (epoch millis). |
| `generator` | * | Generator function (KSML generator syntax). Mutually exclusive with `messages`. |
| `count` | no | Number of times to invoke the generator (default `1`). |

\* Exactly one of `messages` or `generator` must be present.

Inline `topic`, `keyType`, or `valueType` fields on a produce block are not permitted —
declare the stream under `streams:` and reference it via `to:`.

### Assert block

Each assert block runs Python code with injected variables:

| Field | Required | Description |
|---|---|---|
| `on` | no* | Stream key (must exist in `streams:`). The runner reads all records from the underlying topic and injects them as `records`. |
| `stores` | no* | List of state store names (the names declared in the pipeline's `stores:` block). Each store is injected as a Python variable. |
| `code` | yes | Python assertion code. Use `assert` statements. |

\* At least one of `on` or `stores` must be specified.

When `on:` is set, `records` is a list of dicts with `key`, `value`, and `timestamp` fields,
deserialized using the referenced stream's serdes. When `stores:` is set, each store is
available as a Python variable with the same API as in pipeline code (e.g. `store.get(key)`,
`store.put(key, value)`).

Inline `topic` fields on an assert block are not permitted — declare the stream under
`streams:` and reference it via `on:`.

### Reporting

Each test result is labeled `<suite> › <test>` where `<suite>` is the file's `name:` (or
filename without extension) and `<test>` is the test entry's `description:` (or the test key
if absent). Suite-level parse failures (missing `definition:`, malformed YAML, undefined stream
references, etc.) are reported as a single ERROR result for the whole suite.

## Running from the command line

Build the project first:

```bash
mvn clean package
```

Then run one or more test files, directories, or a mix of both. When a directory is given,
the runner walks it recursively and picks up every `*.yaml` / `*.yml` file inside:

```bash
java -Djava.security.manager=allow \
  -cp "ksml-test-runner/target/ksml-test-runner-1.2.0-SNAPSHOT.jar:ksml-test-runner/target/libs/*" \
  io.axual.ksml.testrunner.KSMLTestRunner \
  path/to/test1.yaml path/to/tests/
```

Or using the JAR directly (the manifest classpath expects `libs/` next to the JAR):

```bash
java -Djava.security.manager=allow \
  -jar ksml-test-runner/target/ksml-test-runner-1.2.0-SNAPSHOT.jar \
  path/to/test1.yaml path/to/tests/
```

The exit code is `0` if all tests pass, `1` otherwise.

## Running from IntelliJ

1. Open the project in IntelliJ.
2. Create a new **Run Configuration** (Run > Edit Configurations > + > Application).
3. Set the following:
   - **Module:** `ksml-test-runner`
   - **Main class:** `io.axual.ksml.testrunner.KSMLTestRunner`
   - **Program arguments:** path(s) to your test YAML file(s), e.g. `src/test/resources/sample-filter-test.yaml`
   - **VM options:** `-Djava.security.manager=allow`
   - **Use classpath of module:** `ksml-test-runner`
4. Click **Run**.

> **Note:** The test runner requires GraalVM. Make sure IntelliJ is configured to use a GraalVM JDK
> (File > Project Structure > SDKs).

## Running from Docker

The KSML Docker image includes the test runner JAR at `/opt/ksml/ksml-test.jar`.
Override the entrypoint to run tests, passing one or more files, directories, or a mix:

```bash
# Run a single test file
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:local \
  -Djava.security.manager=allow -jar /opt/ksml/ksml-test.jar /tests/my-test.yaml

# Run every *.yaml / *.yml test inside a directory (walked recursively)
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:local \
  -Djava.security.manager=allow -jar /opt/ksml/ksml-test.jar /tests/
```

Letting the runner expand the directory inside the container avoids shell glob
expansion problems on the host (e.g. `fish: No matches for wildcard …`), since
the pattern would otherwise be evaluated against the host filesystem before
`docker run` ever starts.

## Sample tests

The `src/test/resources/` directory contains sample test definitions:

- `sample-filter-test.yaml` — filters SensorData by color, asserts on output topic records
- `sample-filter-test-confluent-avro.yaml` — same filter logic, but pipeline uses `confluent_avro` with a `registry` block
- `sample-filter-test-registry-only-types.yaml` — types declared only in `registry`, not on produce block
- `sample-state-store-test.yaml` — stores sensor data per key, asserts on state store contents
- `sample-timestamp-test.yaml` — uses explicit timestamps, asserts timestamps are preserved

## Editor support (YAML schema)

A JSON Schema for test definition files is generated at build time to
`docs/ksml-test-spec.json` (analogous to `docs/ksml-language-spec.json` for pipelines).
It provides auto-completion, validation, and field descriptions in editors that support
YAML schemas.

### Per-file activation

Add this comment as the first line of your test YAML:

```yaml
# yaml-language-server: $schema=./path/to/test-definition.schema.json
```

### VS Code

Install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml),
then add to your workspace `settings.json`:

```json
{
  "yaml.schemas": {
    "./docs/ksml-test-spec.json": [
      "*-test.yaml",
      "test-*.yaml"
    ]
  }
}
```

### IntelliJ

Go to **Settings > Languages & Frameworks > Schemas and DTDs > JSON Schema Mappings**,
add a new mapping pointing to `docs/ksml-test-spec.json`, and configure the file pattern
(e.g. `*-test.yaml`).

### Regenerating the schema

The schema is regenerated automatically during `mvn compile` (at the `process-classes` phase).
To regenerate it manually:

```bash
mvn process-classes -pl ksml-test-runner -am
```

This runs `TestDefinitionSchemaGenerator` and writes the schema to `docs/ksml-test-spec.json`.

## Logging

The test runner ships with a default `logback.xml` that keeps output quiet: `WARN`
for everything, `INFO` for `io.axual.ksml.testrunner` to show the
`Running test: ...` progress lines and the final results table.

To get verbose output for one run, point Logback at a custom config file at
invocation time:

```bash
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:local \
  -Dlogback.configurationFile=/tests/logback-debug.xml \
  -jar /opt/ksml/ksml-test.jar /tests/
```
