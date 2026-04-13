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

```yaml
test:
  name: "Filter pipeline passes blue sensors"
  pipeline: pipelines/test-filter.yaml
  schemaDirectory: schemas

  produce:
    - topic: ksml_sensordata_avro
      keyType: string
      valueType: "avro:SensorData"
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
    - topic: ksml_sensordata_filtered
      code: |
        assert len(records) == 2, f"Expected 2 records, got {len(records)}"
```

### Fields

| Field | Required | Description |
|---|---|---|
| `test.name` | yes | Human-readable test name, shown in the output |
| `test.pipeline` | yes | Path to the KSML pipeline YAML (relative to the test file or absolute) |
| `test.schemaDirectory` | no | Path to Avro/JSON schema files (relative to the test file or absolute) |
| `test.produce` | yes | List of produce blocks |
| `test.assert` | yes | List of assertion blocks |

### Produce block

Each produce block targets one input topic:

| Field | Required | Description |
|---|---|---|
| `topic` | yes | Kafka topic name to produce to |
| `keyType` | no | Key type (default: `string`) |
| `valueType` | no | Value type, e.g. `string` or `avro:SensorData` (default: `string`) |
| `messages` | yes | List of messages with `key`, `value`, and optional `timestamp` (epoch millis) |

### Assert block

Each assert block runs Python code with injected variables:

| Field | Required | Description |
|---|---|---|
| `topic` | no* | Output topic to read records from. Injects a `records` variable |
| `stores` | no* | List of state store names to inject as Python variables |
| `code` | yes | Python assertion code. Use `assert` statements |

\* At least one of `topic` or `stores` must be specified.

When `topic` is set, the `records` variable is a list of dicts with `key`, `value`, and `timestamp` fields.
When `stores` is set, each store is available as a Python variable with the same API as in pipeline code
(e.g. `store.get(key)`, `store.put(key, value)`).

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
