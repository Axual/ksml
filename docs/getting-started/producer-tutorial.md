# Producer Tutorial

KSML producers let you quickly send test messages to Kafka in different formats like JSON, Avro, Protobuf, XML, CSV, and Binary. Write simple Python code to generate your data, and KSML handles the rest - no building or compiling needed.

In this tutorial, you'll learn all the properties and features of KSML producers.

## Prerequisites

Before starting this tutorial:

- Complete the [Quick Start Guide](quick-start.md)
- Have the [Docker Compose environment running](quick-start.md#step-1-set-up-your-environment)
- Add the following topic to your `kafka-setup` service in `docker-compose.yml` to run the examples:

??? info "Topic creation commands (click to expand)"

    ```yaml
    # Producer Tutorial Topics
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic my_topic \
    ```

## Basic Producer Structure

A KSML producer definition consists of two main parts:

1. **Functions**: Define the generator function that creates messages
2. **Producers**: Configure how and when messages are produced

Here's the simplest possible producer:

??? info "Simplest Producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/simplest-producer.yaml" %}
    ```

This producer will generate **one JSON message** and stop, because no `interval`, `count`, or `until` condition is specified.

## Producer Properties Reference

Let's explore all available properties for producer definitions:

### Required Properties

#### `generator` (required)

The function that generates messages. Must be a function of type `generator`.

```yaml
producers:
  my_producer:
    generator: my_generator_function  # References a function defined in the functions section
    to: my_topic
```

The generator function must return:

- A tuple `(key, value)` for a single message, or
- A list of tuples `[(key1, value1), (key2, value2), ...]` for multiple messages

For detailed information about generator functions, see the [Generator Function Reference](../reference/function-reference.md#generator).

#### `to` (required)

The target topic where messages will be produced. Can be specified as a simple topic name or with detailed configuration.

Simple format:
```yaml
producers:
  my_producer:
    generator: my_generator
    to: my_topic 
```

Detailed format:
```yaml
producers:
  my_producer:
    generator: my_generator
    to:
      topic: sensor_data
      keyType: string
      valueType: json
```

### Optional Properties

#### `interval`

The time to wait between generator calls. Supports various duration formats based on KSML's duration specification:

**Duration Format**: `<number><unit>` where unit is optional

- `100` - 100 milliseconds (default unit)
- `500ms` - 500 milliseconds
- `3s` - 3 seconds
- `5m` - 5 minutes
- `2h` - 2 hours
- `1d` - 1 day
- `4w` - 4 weeks

**Default**: If not specified, and no `count` or `until` is provided, the producer enters "once mode" and generates **exactly one message** then stops.

**Important**: Specifying `interval` (even as `0` or `1ms`) is different from omitting it entirely. See the behavior table below.

??? info "Interval Examples (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/interval-examples.yaml" %}
    ```

#### `count`

The total number of messages to produce before stopping.

**Default**: If not specified (and `until` is also not specified), produces indefinitely.

??? info "Count Example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/count-example.yaml" %}
    ```

**Note**: The producer will stop when either `count` is reached OR the `until` condition becomes true, whichever comes first.

#### `batchSize`

The number of messages to generate in each call to the generator. This is useful for performance optimization.

- **Default**: `1` (one message per generator call)
- **Range**: Must be between 1 and 1000
- **Validation**: Values outside this range will trigger a warning and default to 1

??? info "Batch Size Example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/batch-example.yaml" %}
    ```

**Performance Note**: Using batching can significantly improve throughput when producing large volumes of data.

#### `condition`

A predicate function that validates whether a generated message should be produced. If the condition returns `false`, the message is discarded and the generator is called again.

??? info "Condition Example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/condition-example.yaml" %}
    ```

**Use Cases**:

- Filtering out invalid or unwanted messages
- Implementing probabilistic message generation

#### `until`

A predicate function that determines when to stop producing. When this function returns `true`, the producer stops immediately.

??? info "Until Example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/until-example.yaml" %}
    ```

**Alternative with global state**:

??? info "Until with Global State Example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/producer-tutorial/until-global-state-example.yaml" %}
    ```

**Use Cases**:

- Producing a predetermined dataset exactly once
- Implementing time-based or condition-based stopping logic

## Producer Behavior Summary

Understanding when a producer starts and stops is crucial. Here's the complete behavior table:

| interval | count | until | Behavior |
|----------|-------|-------|----------|
| **Not specified** | Not specified | Not specified | Produces **1 message** then stops ("once mode") |
| **Specified** (any value) | Not specified | Not specified | Produces **indefinitely** at the specified interval |
| **Specified** | **Specified** | Not specified | Produces `count` messages at the specified interval, then stops |
| **Specified** | Not specified | **Specified** | Produces indefinitely until `until` returns true, checking at each interval |
| **Specified** | **Specified** | **Specified** | Produces until `count` is reached **OR** `until` returns true (whichever comes first) |

**Key Points:**

- **"Not specified"** means the property is completely omitted from the YAML
- **"Specified"** means the property is included, even if set to a minimal value like `interval: 1ms`
- Using `interval` without `count` or `until` will produce messages indefinitely.
- When both `count` and `until` are specified, the producer stops when **either** condition is met first

## Working with Different Data Formats

To learn about producing messages in different formats like JSON, Avro, Protobuf, XML, CSV, and Binary, see the [Different Data Formats](../tutorials/beginner/data-formats.md) tutorial.