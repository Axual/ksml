# Testing Your Pipeline

KSML includes a test runner that lets you verify your pipeline logic without a running Kafka broker. You write a YAML test definition that describes what data to send and what to assert, and the test runner handles the rest using Kafka's `TopologyTestDriver`.

## How It Works

The test runner:

1. Parses your KSML pipeline definition and builds a Kafka Streams topology
2. Sends test messages into the topology's input topics
3. Runs Python assertions against output topics and/or state stores
4. Reports pass/fail results

No Kafka broker, no Schema Registry, no infrastructure required.

## Test Definition Format

A test definition is a YAML file with a `test` root element:

```yaml
test:
  name: "Human-readable test name"
  pipeline: path/to/pipeline.yaml
  schemaDirectory: path/to/schemas    # optional, for Avro schemas

  produce:
    - topic: input-topic-name
      keyType: string                 # optional, defaults to "string"
      valueType: "avro:SensorData"    # optional, defaults to "string"
      messages:
        - key: "my-key"
          value: { field: "value" }
          timestamp: 1709200000000    # optional, epoch millis

  assert:
    - topic: output-topic-name
      code: |
        assert len(records) == 1
        assert records[0]["key"] == "my-key"
```

### Produce Blocks

Each produce block targets one input topic. You can define multiple produce blocks to feed data into different topics (e.g. for join tests).

| Field | Required | Default | Description |
|---|---|---|---|
| `topic` | yes | | Kafka topic name |
| `keyType` | no | `string` | Key serialization type (e.g. `string`, `avro:MySchema`) |
| `valueType` | no | `string` | Value serialization type |
| `messages` | yes | | List of messages with `key`, `value`, and optional `timestamp` |

### Assert Blocks

Each assert block runs Python code with injected variables. At least one of `topic` or `stores` must be specified.

| Field | Required | Description |
|---|---|---|
| `topic` | no | Output topic to read records from. Injects a `records` list variable |
| `stores` | no | List of state store names to inject as Python variables |
| `code` | yes | Python assertion code using `assert` statements |

When `topic` is set, `records` is a list of dicts with `key`, `value`, and `timestamp` fields.
When `stores` is set, each store is available as a Python variable with the same API as in pipeline functions (e.g. `store.get(key)`).

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

The test sends three sensor messages (two blue, one red) and asserts that only the two blue sensors appear in the output topic.

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

You can pass multiple test files:

```bash
docker run --rm \
  -v ./my-tests:/tests \
  --entrypoint java \
  axual/ksml:latest \
  -Djava.security.manager=allow -jar /opt/ksml/ksml-test.jar \
  /tests/filter-test.yaml /tests/join-test.yaml /tests/store-test.yaml
```

### Example Output

```
=== KSML Test Results ===

  PASS  Filter pipeline passes blue sensors

1 passed, 0 failed, 0 errors
```

The exit code is `0` when all tests pass, `1` otherwise. This makes it easy to integrate into CI/CD pipelines.

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

## Schema Validation for Test Files

A JSON Schema is available for test definition files at `docs/ksml-test-spec.json`. See the [Schema Validation](schema-validation.md) page for instructions on setting up editor auto-completion and validation.
