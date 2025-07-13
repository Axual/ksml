# KSML Basics Tutorial

This tutorial will guide you through creating your first KSML data pipeline. By the end, you'll understand the basic
components of KSML and how to create a simple but functional data processing application.

## What You'll Build

In this tutorial, you'll build a simple data pipeline that:

1. Reads temperature sensor data from a Kafka topic
2. Filters out readings below a certain threshold
3. Transforms the data by converting Fahrenheit to Celsius
4. Writes the processed data to another Kafka topic
5. Logs information about the processed messages

Here's a visual representation of what we'll build:

```
Input Topic → Filter → Transform → Output Topic
                ↓
              Logging
```

## Prerequisites

Before you begin, make sure you have:

- Completed the [Installation and Setup](installation.md) guide
- A running KSML environment with Kafka
- Basic understanding of YAML syntax

## Understanding the KSML File Structure

A KSML definition file consists of three main sections:

1. **Streams**: Define the input and output Kafka topics
2. **Functions**: Define reusable code snippets
3. **Pipelines**: Define the data processing flow

Let's create each section step by step.

## Step 1: Define Your Streams

First, let's define the input and output streams for our pipeline:

```yaml
streams:
  input_stream:
    topic: tutorial_input
    keyType: string
    valueType: json
  output_stream:
    topic: tutorial_output
    keyType: string
    valueType: json
```

This defines:

- An input stream reading from the `tutorial_input` topic with string keys and JSON values
- An output stream writing to the `tutorial_output` topic with the same data types

### Understanding Stream Definitions

Each stream definition includes:

- A unique name (e.g., `input_stream`)
- The Kafka topic it connects to
- The data types for keys and values

KSML supports various data types including:

- `string`: For text data
- `json`: For JSON-formatted data
- `avro`: For Avro-formatted data (requires schema)
- `binary`: For raw binary data
- And more

## Step 2: Create a Simple Function

Next, let's create a function to log messages as they flow through our pipeline:

```yaml
functions:
  log_message:
    type: forEach
    parameters:
      - name: message_type
        type: string
    code: |
      log.info("{} message - key={}, value={}", message_type, key, value)
```

This function:

- Is named `log_message`
- Is of type `forEach`, which means it [always gets](../reference/function-reference.md)  two parameters `key` and
  `value`, and does not return a value
- Takes one additional parameter called `message_type`
- Uses the built-in `log` object to output information about each message

### Understanding Functions in KSML

Functions in KSML:

- Can be reused across multiple operations in your pipelines
- Are written in Python
- Have access to pre-defined parameters based on function type
- Can take additional parameters for more flexibility
- Must return a value if so pre-defined by the function type

## Step 3: Build Your Pipeline

Now, let's create the pipeline that processes our data:

```yaml
pipelines:
  tutorial_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: value.get('temperature') > 70
      - type: mapValues
        mapper:
          expression: |
            {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
      - type: peek
        forEach:
          code: log_message(key, value, message_type="Processed")
    to: output_stream
```

This pipeline:

1. Reads from `input_stream`
2. Filters out messages where the temperature is 70°F or lower
3. Transforms the values to include both Fahrenheit and Celsius temperatures
4. Logs each processed message
5. Writes the results to `output_stream`

### Understanding Pipeline Operations

Let's break down each operation:

#### Filter Operation

```yaml
- type: filter
  if:
    expression: value.get('temperature') > 70
```

The filter operation:

- Evaluates the expression for each message
- Only passes messages where the expression returns `True`
- Discards messages where the expression returns `False`

#### Map Values Operation

```yaml
- type: mapValues
  mapper:
    expression: |
      {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
```

The mapValues operation:

- Transforms the value of each message
- Keeps the original key unchanged
- Creates a new value based on the expression
- In this case, creates a new JSON object with the original temperature and a calculated Celsius value

Note that we put the expression on a new line in this example to force the KSML YAML parser to interpret the expression
as a literal string for Python, instead of parsing it as part of the YAML syntax. Another way of achieving the same
would be to surround the '{...}' with quotes, but in that case, be aware of consistent single/double quoting to not
confuse the KSML parser and/or the Python interpreter. We generally recommend using the above notation for readability
purposes.

#### Peek Operation

```yaml
- type: peek
  forEach:
    code: log_message(key, value, message_type="Processed")
```

The peek operation:

- Executes the provided code for each message
- Doesn't modify the message
- Allows the message to continue through the pipeline
- Is useful for logging, metrics, or other side effects

## Step 4: Put It All Together

Let's combine all the sections into a complete KSML definition file:

```yaml
streams:
  input_stream:
    topic: tutorial_input
    keyType: string
    valueType: json
  output_stream:
    topic: tutorial_output
    keyType: string
    valueType: json

functions:
  log_message:
    type: forEach
    parameters:
      - name: message_type
        type: string
    code: |
      log.info("{} message - key={}, value={}", message_type, key, value)

pipelines:
  tutorial_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: value.get('temperature') > 70
      - type: mapValues
        mapper:
          expression: |
            {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
      - type: peek
        forEach:
          code: log_message(key, value, message_type="Processed")
    to: output_stream
```

Save this file as `tutorial.yaml`.

## Step 5: Run Your Pipeline

Now let's run the pipeline and see it in action:

1. Make sure your Kafka environment is running
2. Create the input and output topics:
   ```bash
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic tutorial_input --partitions 1 --replication-factor 1
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic tutorial_output --partitions 1 --replication-factor 1
   ```
3. Run the KSML runner with your definition file:
   ```bash
   docker run -v $(pwd):/app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 axual/ksml-runner:latest --definitions /app/tutorial.yaml
   ```
4. In another terminal, produce some test messages to the input topic:
   ```bash
   docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic tutorial_input --property "parse.key=true" --property "key.separator=:"
   ```
   Then enter messages in the format `key:value`, for example:
   ```
   sensor1:{"temperature": 75}
   sensor2:{"temperature": 65}
   sensor3:{"temperature": 80}
   ```
5. In a third terminal, consume messages from the output topic to see the results:
   ```bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic tutorial_output --from-beginning
   ```

You should see messages like:

```
{"sensor":"sensor1","temp_fahrenheit":75,"temp_celsius":23.88888888888889}
{"sensor":"sensor3","temp_fahrenheit":80,"temp_celsius":26.666666666666668}
```

Notice that the message with temperature 65°F was filtered out, and the remaining messages have been transformed to
include the Celsius temperature.

## Understanding What's Happening

When you run your KSML definition:

1. The KSML runner parses your YAML file
2. It creates a Kafka Streams topology based on your pipeline definition
3. The topology starts consuming from the input topic
4. Each message flows through the operations you defined:
    - The filter operation drops messages with temperatures ≤ 70°F
    - The mapValues operation transforms the remaining messages
    - The peek operation logs each message
    - The messages are written to the output topic

## Using KSML to produce messages

While you can manually produce the above messages, KSML can also generate messages for you. See below for a KSML
definition that would randomly generate test messages every three seconds.

```yaml
functions:
  generate_temperature_message:
    type: generator
    globalCode: |
      import random
      sensorCounter = 0
    code: |
      global sensorCounter

      key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
      sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

      value = {"temperature": random.randrange(150)}
    expression: (key, value)                      # Return a message tuple with the key and value
    resultType: (string, json)                    # Indicate the type of key and value

producers:
  # Produce a temperature message every 3 seconds
  tutorial_producer:
    generator: generate_temperature_message
    interval: 3s
    to:
        topic: tutorial_input
        keyType: string
        valueType: json
```

## Next Steps

Congratulations! You've built your first KSML data pipeline. Here are some ways to expand on what you've learned:

### Try These Modifications:

1. Add another filter condition (e.g., filter by sensor name)
2. Add more fields to the transformed output
3. Create a second pipeline that processes the data differently

### Explore More Advanced Concepts:

- Learn about [stateful operations](../../core-concepts/operations.md#stateful-operations) like aggregations and joins
- Explore [windowing operations](../../core-concepts/operations.md#windowing-operations) for time-based processing
- Try working with different [data formats](../../reference/stream-types-reference.md)

### Continue Your Learning Journey:

- Check out the [beginner tutorials](../beginner/index.md) for more guided examples
- Explore the [examples library](../../resources/examples-library.md) for inspiration
- Dive into the [reference documentation](../../reference/operations-reference.md) to learn about all available operations