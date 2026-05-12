# Testing Your Pipeline

KSML includes a test runner that lets you verify your pipeline logic without a running Kafka broker. You write a YAML test definition that describes what data to send and what to assert, and the test runner handles the rest using Kafka's `TopologyTestDriver`.

## How It Works

The test runner:

1. Parses your KSML pipeline definition and builds a Kafka Streams topology
2. Sends test messages into the topology's input topics
3. Runs Python assertions against output topics and/or state stores
4. Reports pass/fail results

Since the test runner is using Kafka's topology test driver, there is no infrastructure needed: no Kafka broker, no Schema Registry. Tests can be run on any machine.

A test definition file is a *suite* — it describes one pipeline plus one or more named tests that share the suite's configuration. Each test runs against a fresh `TopologyTestDriver`, so tests in the same suite are hermetic and do not share state.

## Test Definition Format

A test definition is a flat YAML document referencing one KSML definition, the streams it touches, and one or more named tests:

```yaml
name: "Filtering pipeline tests"          # optional; falls back to filename without extension
definition: path/to/pipeline.yaml         # path to the KSML pipeline definition YAML
schemaDirectory: path/to/schemas          # optional, for schemas
moduleDirectory: path/to/modules          # optional, for externalized Python modules

streams:
  sensor_source:                          # logical stream name (referenced by to: / on:)
    topic: input-topic-name
    keyType: string                       # optional, defaults to "string"
    valueType: "avro:SensorData"          # optional, defaults to "string"
  sensor_filtered:
    topic: output-topic-name
    keyType: string
    valueType: "avro:SensorData"

tests:
  blue_sensors_pass:                      # test identifier (referenced in reports)
    description: "Blue sensors pass through"   # optional, falls back to the test key
    produce:
      - to: sensor_source                 # references streams: key, not a topic name
        messages:
          - key: "my-key"
            value: { field: "value" }
            timestamp: 1709200000000      # optional, epoch millis

    assert:
      - on: sensor_filtered               # references streams: key, not a topic name
        code: |
          assert len(records) == 1
          assert records[0]["key"] == "my-key"
```

### Suite-level fields

| Field | Required | Description |
|---|---|---|
| `name` | no | Human-readable suite name shown in reports. Falls back to filename without extension. |
| `definition` | yes | Path to the KSML pipeline definition YAML (relative to the test file, on the classpath, or absolute). |
| `schemaDirectory` | no | Path to schema files. Required when any stream uses a schema-bearing notation like `avro:Foo`. |
| `moduleDirectory` | no | Path to externalized Python modules accessible to the pipeline. |
| `streams` | no | Map of named topic+type bindings, referenced by `to:` and `on:`. See below. |
| `tests` | yes | Map of named tests. Must contain at least one entry. See below. |

### Streams

The `streams:` map declares every Kafka topic the test suite produces to or asserts on. Each entry is keyed by a logical stream identifier (matching `^[a-zA-Z][a-zA-Z0-9_]*$`) and binds it to a topic plus key/value types. 

| Field | Required | Default | Description |
|---|---|---|---|
| `topic` | yes | | Kafka topic name. Each topic may appear in at most one stream entry. |
| `keyType` | no | `string` | Key serialization type (e.g. `string`, `avro:MyKey`). |
| `valueType` | no | `string` | Value serialization type (e.g. `string`, `avro:SensorData`). |

Type strings follow KSML's standard type grammar — the same one `StreamDefinitionParser` uses in pipeline yamls:

- **Schema-less notations** (`string`, `long`, `int`, `boolean`, `bytes`, `json`, `binary`, `xml`, etc.) may appear unqualified.
- **Schema-bearing notations** (`avro`, `confluent_avro`, `apicurio_avro`, `protobuf`, `json_schema`, `csv`) **must** be qualified with a schema name (e.g., `avro:SensorData`). The named schema is loaded from `schemaDirectory` and registered in an in-memory mock registry under `<topic>-key` / `<topic>-value`. 
- **Bare schema-bearing notations** (e.g., `confluent_avro` without `:Schema`) are rejected — the test runner has no real registry to resolve them against.

### Tests

The `tests:` map carries one or more test entries. Each key is a stable test identifier matching the same regex as stream keys; the value contains that test's produce data and assertions. Tests run in YAML-defined order; failure in one test does not stop later tests.

| Field | Required | Description |
|---|---|---|
| `description` | no | Human-readable label. Falls back to the test key. |
| `produce` | yes | List of produce blocks. |
| `assert` | yes | List of assertion blocks. |

### Produce Blocks

Each produce block targets one stream by reference. You can define multiple produce blocks to feed data into different streams (e.g. for join tests).

| Field | Required | Default | Description |
|---|---|---|---|
| `to` | yes | | Stream key (must exist in `streams:`). |
| `messages` | * | | List of messages with `key`, `value`, and optional `timestamp` (epoch millis). |
| `generator` | * | | Generator function (KSML generator syntax). Mutually exclusive with `messages`. |
| `count` | no | `1` | Number of times to invoke the generator. |

\* Exactly one of `messages` or `generator` must be present.

Inline `topic`, `keyType`, or `valueType` fields on a produce block are not permitted — declare the stream under `streams:` and reference it via `to:`.

### Assert Blocks

Each assert block runs Python code with injected variables. At least one of `on` or `stores` must be specified.

| Field | Required | Description |
|---|---|---|
| `on` | no* | Stream key (must exist in `streams:`). The runner reads all records from the underlying topic and injects them as a `records` list variable, deserialized using the stream's serdes. |
| `stores` | no* | List of state store names (the names declared in the pipeline's `stores:` block). Each store is injected as a Python variable. |
| `code` | yes | Python assertion code using `assert` statements. |

When `on:` is set, `records` is a list of dicts with `key`, `value`, and `timestamp` fields. When `stores:` is set, each store is available as a Python variable with the same API as in pipeline functions (e.g. `store.get(key)`, `store.put(key, value)`).

### Reporting

Each test result is labeled `<suite> › <test>` where `<suite>` is the file's `name:` (or the filename without extension) and `<test>` is the test entry's `description:` (or the test key if absent). Suite-level parse failures (missing `definition:`, malformed YAML, undefined stream references, etc.) are reported as a single ERROR result for the whole suite.

## Example: Testing a Filter Pipeline

Let's walk through testing a pipeline that filters sensor data, keeping only sensors with color "blue".

### The Pipeline

??? info "Pipeline definition: `test-filter.yaml` (click to expand)"

    ```yaml
    --8<-- "pipelines/test-filter.yaml"
    ```

This pipeline reads from `ksml_sensordata_avro`, filters messages where the sensor color is "blue", and writes the matching messages to `ksml_sensordata_filtered`.

### The Test

??? info "Test definition: `sample-filter-test.yaml` (click to expand)"

    ```yaml
    --8<-- "sample-filter-test.yaml"
    ```

The suite declares two logical streams (`sensor_source` and `sensor_filtered`) bound to the two Kafka topics, then defines one test that produces three sensor messages (two blue, one red) into `sensor_source` and asserts that only the two blue sensors appear on `sensor_filtered`.

## Example: Testing a Pipeline Using `confluent_avro`

If your pipeline uses `confluent_avro` (or `apicurio_avro`) — notations that would normally fetch schemas from a real registry at runtime — the suite's `streams:` block tells the test runner which schemas to register in the mock registry. Each stream entry's `valueType: "avro:SensorData"` causes `SensorData.avsc` to be loaded from `schemaDirectory` and registered under the `<topic>-key` / `<topic>-value` subjects. The KSML definition's `confluent_avro` types resolve from the mock registry, and the assertion can deserialize the Avro output records correctly.

### The Pipeline

??? info "Pipeline definition: `test-filter-confluent-avro.yaml` (click to expand)"

    ```yaml
    --8<-- "pipelines/test-filter-confluent-avro.yaml"
    ```

### The Test

??? info "Test definition: `sample-filter-test-confluent-avro.yaml` (click to expand)"

    ```yaml
    --8<-- "sample-filter-test-confluent-avro.yaml"
    ```

The same suite shape works for both `avro:SensorData` (where the schema name is part of the type) and `confluent_avro:SensorData` (where it would normally come from a real registry) — the test runner handles them uniformly through the `streams:` block.

## Running Tests with Docker

The KSML Docker image includes the test runner at `/opt/ksml/ksml-test.jar`. Mount your test files and override the entrypoint:

```bash
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:latest \
  -Djava.security.manager=allow -jar /opt/ksml/ksml-test.jar \
  /tests/my-test.yaml
```

You can pass multiple test files or directories:

```bash
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:latest \
  -Djava.security.manager=allow -jar /opt/ksml/ksml-test.jar \
  /tests/
```

### Example Output

Output for a single multi-test suite that exercises five cases against the same pipeline:

```
=== KSML Test Results ===

  PASS  Filtering & transforming pipeline › Valid sensor data is filtered through and transformed
  PASS  Filtering & transforming pipeline › Out-of-range temperature is filtered out
  PASS  Filtering & transforming pipeline › Sensor data without temperature is filtered out
  PASS  Filtering & transforming pipeline › Sensor data without humidity is filtered out
  PASS  Filtering & transforming pipeline › Malformed sensor data is routed to alerts_stream when transformation fails

5 passed, 0 failed, 0 errors
```

When a test fails or errors, the offending result also includes the assertion message or exception detail on the line below:

```
=== KSML Test Results ===

  PASS  Filtering & transforming pipeline › Valid sensor data is filtered through and transformed
  FAIL  Filtering & transforming pipeline › Out-of-range temperature is filtered out
        AssertionError: Expected no records on filtered_data, got 1
  PASS  Filtering & transforming pipeline › Sensor data without temperature is filtered out
  PASS  Filtering & transforming pipeline › Sensor data without humidity is filtered out
  PASS  Filtering & transforming pipeline › Malformed sensor data is routed to alerts_stream when transformation fails

4 passed, 1 failed, 0 errors
```

A failure in one test does not stop later tests in the same suite from running. The exit code of the runner is `0` when every result is `PASS` and `1` otherwise. This makes it easy to integrate into CI/CD pipelines.

## Writing Assertions

Assertions use Python's `assert` statement. Some common patterns:

### Check record count

```python
assert len(records) == 3, f"Expected 3 records, got {len(records)}"
```

### Check specific record values

```python
assert records[0]["key"] == "sensor-1"
assert records[0]["value"]["color"] == "blue"
```

### Check timestamps

```python
assert records[0]["timestamp"] == 1709200000000
```

### Check state store contents

```python
# With stores: [my_store] in the assert block
value = my_store.get("sensor-1")
assert value is not None, "Expected sensor-1 in store"
assert value["temperature"] == "25.0"
```

### Combine output records and state stores in one block

```yaml
assert:
  - on: enriched_output
    stores:
      - last_seen_store
    code: |
      assert len(records) == 1
      assert last_seen_store.get(records[0]["key"]) is not None
```

## Schema Validation for Test Files

A JSON Schema is available for test definition files at `docs/ksml-test-spec.json`. See the [Schema Validation](schema-validation.md) page for instructions on setting up editor auto-completion and validation. The schema enforces the identifier regex on stream and test keys via `patternProperties`, so editors will flag invalid keys as you type.

## Logging

The test runner ships with a default Logback configuration that keeps output quiet: `WARN` for everything, `INFO` for the test runner itself so you still see the `Running suite: ...` progress lines and the final results table.

To get verbose output for one run, point Logback at a custom logback configuration file at invocation time:

```bash
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:latest \
  -Dlogback.configurationFile=/tests/logback-debug.xml \
  -jar /opt/ksml/ksml-test.jar /tests/my-test.yaml
```
