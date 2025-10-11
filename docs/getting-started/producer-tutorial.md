# Producer Tutorial

KSML, besides being a powerful Kafka Streams wrapper, is also a great way to write business logic for producing messages to Kafka topics. This approach beats the alternative of writing long Java applications to do the same - you can define complex message generation logic in a simple YAML file with Python functions.

In this tutorial, you'll learn everything about producer definitions in KSML, including all available properties and how to use them effectively.

## What You'll Learn

By the end of this tutorial, you'll understand:

- The basic structure of a producer definition
- All available producer properties and when to use them
- How to control message generation with intervals and batching
- How to work with different data formats
- Advanced producer patterns using conditions and termination logic

## Prerequisites

Before you begin, make sure you have:

- Completed the [KSML Basics Tutorial](basics-tutorial.md)
- Basic understanding of YAML and Python
- Your KSML environment running

## Basic Producer Structure

A KSML producer definition consists of two main parts:

1. **Functions**: Define the generator function that creates messages
2. **Producers**: Configure how and when messages are produced

Here's the simplest possible producer:

```yaml
functions:
  simple_generator:
    type: generator
    expression: ("key1", {"message": "Hello, Kafka!"})
    resultType: (string, json)

producers:
  my_producer:
    generator: simple_generator
    to:
      topic: my_topic
      keyType: string
      valueType: json
```

This producer will generate **one message** and stop, because no interval, count, or until condition is specified.

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

#### `to` (required)

The target topic where messages will be produced. Can be specified as a simple topic name or with detailed configuration.

Simple format:
```yaml
producers:
  my_producer:
    generator: my_generator
    to: my_topic  # Topic must be defined in streams section
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

```yaml
producers:
  # Produce every 3 seconds (indefinitely)
  slow_producer:
    generator: my_generator
    interval: 3s
    to: my_topic

  # Produce every 500 milliseconds
  fast_producer:
    generator: my_generator
    interval: 500ms
    to: my_topic

  # Produce as fast as possible for 100 messages
  rapid_producer:
    generator: my_generator
    interval: 1ms   # Very fast (not 0, to be explicit)
    count: 100      # Must specify count or until to avoid infinite loop
    to: my_topic
```

#### `count`

The total number of messages to produce before stopping.

**Default**: If not specified (and `until` is also not specified), produces indefinitely.

```yaml
producers:
  # Produce exactly 100 messages
  limited_producer:
    generator: my_generator
    interval: 1s
    count: 100
    to: my_topic
```

**Note**: The producer will stop when either `count` is reached OR the `until` condition becomes true, whichever comes first.

#### `batchSize`

The number of messages to generate in each call to the generator. This is useful for performance optimization.

**Default**: `1` (one message per generator call)
**Range**: Must be between 1 and 1000
**Validation**: Values outside this range will trigger a warning and default to 1

```yaml
functions:
  batch_generator:
    type: generator
    globalCode: |
      counter = 0
    code: |
      global counter
      # Generate multiple messages in one call
      messages = []
      for i in range(10):  # Generate 10 messages
        key = f"sensor{counter}"
        value = {"id": counter, "reading": counter * 10}
        messages.append((key, value))
        counter += 1
      return messages  # Return list of tuples
    resultType: list(tuple(string, json))

producers:
  batch_producer:
    generator: batch_generator
    interval: 5s
    batchSize: 10  # Matches the generator's batch size
    to:
      topic: sensor_data
      keyType: string
      valueType: json
```

**Performance Note**: Using batching can significantly improve throughput when producing large volumes of data.

#### `condition`

A predicate function that validates whether a generated message should be produced. If the condition returns `false`, the message is discarded and the generator is called again.

```yaml
functions:
  generate_random_value:
    type: generator
    globalCode: |
      import random
    code: |
      value = random.randint(0, 100)
      return ("sensor1", {"value": value})
    resultType: (string, json)

  only_high_values:
    type: predicate
    expression: value.get("value", 0) > 50

producers:
  filtered_producer:
    generator: generate_random_value
    condition: only_high_values  # Only produce if value > 50
    interval: 1s
    to:
      topic: high_values
      keyType: string
      valueType: json
```

**Use Cases**:
- Filtering out invalid or unwanted messages
- Implementing probabilistic message generation
- Ensuring data quality before producing

#### `until`

A predicate function that determines when to stop producing. When this function returns `true`, the producer stops immediately.

```yaml
functions:
  generate_sequence:
    type: generator
    globalCode: |
      counter = 0
    code: |
      global counter
      counter += 1
      return (f"key{counter}", {"count": counter})
    resultType: (string, json)

  stop_at_10:
    type: predicate
    expression: value.get("count", 0) >= 10

producers:
  sequence_producer:
    generator: generate_sequence
    until: stop_at_10  # Stop when count reaches 10
    interval: 1s
    to:
      topic: sequence_data
      keyType: string
      valueType: json
```

**Alternative with global state**:
```yaml
functions:
  generate_with_stop:
    type: generic
    resultType: boolean
    globalCode: |
      counter = 0
      done = False

      def next_message():
        global counter, done
        counter += 1
        if counter >= 10:
          done = True
        return (f"key{counter}", {"count": counter})

producers:
  smart_producer:
    generator:
      code: |
        return next_message()
      resultType: (string, json)
    until:
      expression: done  # Access global variable
    interval: 1s
    to:
      topic: smart_data
      keyType: string
      valueType: json
```

**Use Cases**:
- Producing a predetermined dataset exactly once
- Implementing time-based or condition-based stopping logic
- Creating test data with specific characteristics

## Working with Different Data Formats

KSML supports multiple data formats for message keys and values. Here are examples for common formats:

### JSON Format

```yaml
functions:
  generate_json:
    type: generator
    globalCode: |
      import random
      counter = 0
    code: |
      global counter
      key = f"sensor{counter}"
      counter = (counter + 1) % 10
      value = {
        "id": counter,
        "temperature": random.randint(0, 100),
        "humidity": random.randint(0, 100)
      }
      return (key, value)
    resultType: (string, json)

producers:
  json_producer:
    generator: generate_json
    interval: 2s
    to:
      topic: sensor_data_json
      keyType: string
      valueType: json
```

### Avro Format

```yaml
functions:
  generate_avro_data:
    type: generator
    globalCode: |
      import time
      import random
    code: |
      # Generate as JSON, KSML will convert to Avro using schema
      data = {
        "name": "sensor1",
        "timestamp": str(round(time.time() * 1000)),
        "value": str(random.randint(0, 100))
      }
      return ("sensor1", data)
    resultType: (string, json)  # Generate as JSON

producers:
  avro_producer:
    generator: generate_avro_data
    interval: 3s
    to:
      topic: sensor_data_avro
      keyType: string
      valueType: avro:SensorData  # Output as Avro with schema name
```

### Binary Format

```yaml
functions:
  generate_binary:
    type: generator
    globalCode: |
      from random import randbytes, randrange
      counter = 0
    code: |
      global counter
      key = f"sensor{counter}"
      counter = (counter + 1) % 10
      # Generate random bytes
      value = list(randbytes(randrange(100, 1000)))
      return (key, value)
    resultType: (string, bytes)

producers:
  binary_producer:
    generator: generate_binary
    interval: 3s
    to:
      topic: binary_data
      keyType: string
      valueType: bytes
```

### Protobuf Format

```yaml
functions:
  generate_protobuf_data:
    type: generator
    globalCode: |
      import time
      import random
    code: |
      # Generate as JSON, KSML will convert to Protobuf
      data = {
        "name": "sensor1",
        "timestamp": str(round(time.time() * 1000)),
        "value": str(random.randint(0, 100))
      }
      return ("sensor1", data)
    resultType: (string, json)

producers:
  protobuf_producer:
    generator: generate_protobuf_data
    interval: 3s
    to:
      topic: sensor_data_protobuf
      keyType: string
      valueType: protobuf:sensor_data  # Protobuf message type
```

### XML and CSV Formats

```yaml
producers:
  xml_producer:
    generator: my_generator
    interval: 5s
    to:
      topic: xml_data
      keyType: string
      valueType: xml

  csv_producer:
    generator: my_generator
    interval: 5s
    to:
      topic: csv_data
      keyType: string
      valueType: csv
```

### Multiple Producers with Different Formats

You can define multiple producers in the same file, each targeting different topics with different formats:

```yaml
functions:
  generate_device_config:
    type: generator
    code: |
      config = {"device_id": "dev1", "threshold": 50}
      return ("dev1", config)
    resultType: (string, json)

  generate_sensor_reading:
    type: generator
    code: |
      reading = {"sensor": "temp1", "value": 25}
      return ("temp1", reading)
    resultType: (string, json)

producers:
  # JSON configuration every 10 seconds
  config_producer:
    generator: generate_device_config
    interval: 10s
    to:
      topic: device_config
      keyType: string
      valueType: json

  # Avro readings every 3 seconds
  reading_producer:
    generator: generate_sensor_reading
    interval: 3s
    to:
      topic: sensor_readings
      keyType: string
      valueType: avro:SensorData
```

## Complete Examples

### Example 1: Simple Periodic Producer

Produce a temperature reading every 3 seconds:

??? info "Simple Periodic Producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/basics-tutorial/producer.yaml" %}
    ```

### Example 2: Batch Producer with Count

Produce 1000 messages in batches of 100 with minimal delay:

??? info "Batch Producer (click to expand)"

    ```yaml
    --8<-- "examples/00-example-generate-sensordata-avro-batch.yaml:39:49"
    ```

This producer uses:
- `interval: 1` (1 millisecond) - very fast generation
- `count: 1000` - produces exactly 1000 messages then stops
- `batchSize: 100` - generates 100 messages per call

### Example 3: Producer with Until Condition

Produce messages until a specific condition is met:

??? info "Producer with Until (click to expand)"

    ```yaml
    --8<-- "docs-examples/reference/data-types/enum-producer.yaml:37:43"
    ```

This producer loops through a predefined list and stops when the `done` flag becomes true.

### Example 4: Combining Interval, Count, and Until

Here's an example that uses all three properties together:

??? info "Combined Interval, Count, and Until (click to expand)"

    ```yaml
    functions:
      generate_sensor_reading:
        type: generator
        globalCode: |
          import random
          counter = 0
          anomaly_detected = False

          def check_anomaly(value):
            # Simulate anomaly detection
            return value > 95
        code: |
          global counter, anomaly_detected
          counter += 1
          reading = random.randint(0, 100)

          if check_anomaly(reading):
            anomaly_detected = True

          return (f"sensor{counter}", {"reading": reading, "count": counter})
        resultType: (string, json)

    producers:
      sensor_with_limits:
        generator: generate_sensor_reading
        interval: 2s          # Check every 2 seconds
        count: 50             # Maximum 50 readings
        until:                # Stop early if anomaly detected
          expression: anomaly_detected
        to:
          topic: sensor_readings
          keyType: string
          valueType: json
    ```

**This producer will:**
- Generate a sensor reading every 2 seconds
- Stop after 50 readings (count limit)
- **OR** stop immediately if an anomaly is detected (reading > 95)
- Whichever condition is met first will stop the producer

## Producer Behavior Summary

Understanding when a producer starts and stops is crucial. Here's the complete behavior table:

| interval | count | until | Behavior |
|----------|-------|-------|----------|
| **Not specified** | Not specified | Not specified | Produces **1 message** then stops ("once mode") |
| **Specified** (any value) | Not specified | Not specified | Produces **indefinitely** at the specified interval ⚠️ |
| **Specified** | **Specified** | Not specified | Produces `count` messages at the specified interval, then stops |
| **Specified** | Not specified | **Specified** | Produces indefinitely until `until` returns true, checking at each interval |
| **Specified** | **Specified** | **Specified** | Produces until `count` is reached **OR** `until` returns true (whichever comes first) |

**Key Points:**

- **"Not specified"** means the property is completely omitted from the YAML
- **"Specified"** means the property is included, even if set to a minimal value like `interval: 1ms`
- ⚠️ Using `interval` without `count` or `until` will produce messages indefinitely - make sure this is intentional!
- When both `count` and `until` are specified, the producer stops when **either** condition is met first

## Performance Considerations

### Batching for High Throughput

When producing large volumes of data, use batching:

```yaml
producers:
  high_volume_producer:
    generator: batch_generator  # Should return list of tuples
    interval: 100  # 100ms between batches
    batchSize: 100  # 100 messages per batch
    count: 10000  # Total 10,000 messages = 100 batches
    to: high_volume_topic
```

This produces 100 messages every 100ms, achieving 1000 messages/second.

### Interval Selection

- **Very fast producers** (`1ms` - `100ms`): Use for high-throughput testing or real-time simulations
- **Fast producers** (`100ms` - `1s`): Use for typical sensor data or event streams
- **Medium producers** (`1s` - `1m`): Use for periodic updates or monitoring
- **Slow producers** (`> 1m`): Use for configuration updates or low-frequency events

**Note:** Avoid setting interval to very low values (like `1ms`) without specifying `count` or `until`, as this will produce messages as fast as possible indefinitely.

## Testing Your Producers

### Using Docker Compose

After defining your producer, add it to `ksml-runner.yaml`:

```yaml
ksml:
  definitions:
    my_producer: producer.yaml
```

Restart the KSML runner:

```bash
docker compose restart ksml
```

View the logs:

```bash
docker compose logs ksml -f
```

### Verifying Messages

Consume from the topic to verify messages:

```bash
docker compose exec broker kafka-console-consumer.sh \
  --bootstrap-server broker:9093 \
  --topic my_topic \
  --from-beginning \
  --property print.key=true
```

Or use the Kafka UI at [http://localhost:8080](http://localhost:8080).

## Common Patterns and Use Cases

### 1. Test Data Generation

Generate a fixed dataset for testing:

```yaml
producers:
  test_data:
    generator: test_data_generator
    count: 100  # Exactly 100 test records
    interval: 10  # Fast generation
    to: test_topic
```

### 2. Continuous Simulation

Simulate continuous sensor data:

```yaml
producers:
  sensor_simulator:
    generator: sensor_data_generator
    interval: 1s  # One reading per second
    # No count or until = runs indefinitely
    to: sensor_stream
```

### 3. Controlled Dataset

Produce a specific dataset and stop:

```yaml
producers:
  dataset_loader:
    generator: load_from_list
    until: all_data_loaded  # Stop when list is exhausted
    interval: 100
    to: dataset_topic
```

### 4. Filtered Production

Only produce messages that meet criteria:

```yaml
producers:
  filtered_events:
    generator: event_generator
    condition: is_valid_event  # Only produce valid events
    interval: 500
    to: valid_events
```

## Additional Documentation Ideas

Based on the producer capabilities, consider documenting:

1. **Integration with Schema Registry**: How producers work with Confluent and Apicurio Schema Registry for Avro/Protobuf
2. **Error Handling**: What happens when a generator throws an exception
3. **Python Best Practices**: Guidelines for writing efficient generator functions
4. **State Management**: Patterns for managing state in generator functions
5. **Performance Tuning**: Detailed guide on optimizing producer performance
6. **Testing Strategies**: How to test producer definitions before deployment
7. **Monitoring**: How to monitor producer metrics and health

## Troubleshooting

### Producer Not Starting

**Issue**: Producer doesn't generate messages
- Check KSML logs for errors: `docker compose logs ksml`
- Verify the generator function is properly defined
- Ensure the topic exists or is properly defined in streams section

### Messages Not Appearing

**Issue**: Producer runs but no messages appear
- Check if `condition` is filtering all messages
- Verify topic configuration matches producer output types
- Check `until` predicate - it might be stopping immediately

### Performance Issues

**Issue**: Producer is too slow or too fast
- Adjust `interval` to control message rate
- Use `batchSize` for higher throughput
- Consider system resources and Kafka broker capacity

## Next Steps

Now that you understand producers, you can:

1. Combine producers with processors from the [KSML Basics Tutorial](basics-tutorial.md)
2. Explore [Different Data Formats](../tutorials/beginner/data-formats.md)
3. Learn about [State Stores](../tutorials/intermediate/state-stores.md)
4. Check the [Function Reference](../reference/function-reference.md) for advanced generator patterns

## Summary

In this tutorial, you learned:

- ✅ The structure of producer definitions
- ✅ All producer properties (`generator`, `to`, `interval`, `count`, `batchSize`, `condition`, `until`)
- ✅ How to work with different data formats (JSON, Avro, Binary, Protobuf, XML, CSV)
- ✅ Producer behavior with different property combinations
- ✅ Performance optimization with batching
- ✅ Common patterns and use cases

KSML producers provide a powerful, declarative way to generate Kafka messages without writing complex Java code. Experiment with different configurations to find what works best for your use case!
