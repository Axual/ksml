# Building a Simple Data Pipeline

This tutorial guides you through building a simple KSML data pipeline. By the end, you'll understand the basic components of KSML and how to create a functional data processing application.

## Prerequisites

- Basic understanding of Kafka concepts (topics, messages)
- Docker installed for running the example environment
- Completed the [KSML Basics Tutorial](/docs/tutorials/getting-started/basics-tutorial.md)

## What You'll Build

In this tutorial, you'll build a simple data pipeline that:
1. Reads messages from an input topic
2. Filters messages based on a condition
3. Transforms the remaining messages
4. Writes the results to an output topic

Here's a visual representation of what we'll build:

```
Input Topic → Filter → Transform → Output Topic
```

## Setting Up Your Environment

### Start a Local Kafka Cluster

1. Create a new directory for your project
2. Create a `docker-compose.yml` file with the following content:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

3. Start the services:

```bash
docker-compose up -d
```

4. Verify that the services are running:

```bash
docker-compose ps
```

### Create Kafka Topics

Create the input and output topics:

```bash
docker-compose exec kafka kafka-topics --create --topic tutorial-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic tutorial-output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Creating Your KSML Pipeline

### Step 1: Define Your Streams

Create a file named `simple-pipeline.yaml` with the following content:

```yaml
streams:
  input_stream:
    topic: tutorial-input
    keyType: string
    valueType: json
  output_stream:
    topic: tutorial-output
    keyType: string
    valueType: json
```

This defines:
- An input stream connected to the `tutorial-input` topic
- An output stream connected to the `tutorial-output` topic
- Both streams use string keys and JSON values

### Step 2: Create a Simple Function

Add a function to log messages as they flow through the pipeline:

```yaml
functions:
  log_message:
    type: forEach
    parameters:
      - name: message_type
        type: string
    code: log.info("{} message - key={}, value={}", message_type, key, value)
```

This function:
- Takes a parameter called `message_type`
- Logs the message type, key, and value
- Uses the built-in `log` object to write to the application logs

### Step 3: Build Your Pipeline

Now, add the pipeline definition:

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
          expression: {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
      - type: peek
        forEach:
          functionRef: log_message
          args:
            message_type: "Processed"
    to: output_stream
```

This pipeline:
1. Starts from the `input_stream`
2. Filters messages to keep only those with a temperature greater than 70°F
3. Transforms each message to include both Fahrenheit and Celsius temperatures
4. Logs each processed message
5. Sends the results to the `output_stream`

## Running Your Pipeline

### Start the KSML Runner

1. Install the KSML Runner (if not already installed)
2. Run your pipeline:

```bash
ksml-runner --config simple-pipeline.yaml
```

### Produce Test Messages

In a new terminal, produce some test messages:

```bash
docker-compose exec kafka kafka-console-producer --topic tutorial-input --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"
```

Enter messages in the format `key:value`, for example:

```
sensor1:{"temperature": 75, "humidity": 45}
sensor2:{"temperature": 65, "humidity": 50}
sensor3:{"temperature": 80, "humidity": 40}
```

### Observe the Output

In another terminal, consume messages from the output topic:

```bash
docker-compose exec kafka kafka-console-consumer --topic tutorial-output --bootstrap-server localhost:9092 --from-beginning
```

You should see only messages with temperatures above 70°F, transformed to include Celsius values:

```
{"sensor":"sensor1","temp_fahrenheit":75,"temp_celsius":23.88888888888889}
{"sensor":"sensor3","temp_fahrenheit":80,"temp_celsius":26.666666666666668}
```

## Understanding the Pipeline

### The Filter Operation

```yaml
- type: filter
  if:
    expression: value.get('temperature') > 70
```

This operation:
- Examines each message and decides whether to keep it or discard it
- Evaluates the Python expression `value.get('temperature') > 70`
- Only keeps messages where the temperature is greater than 70°F
- Discards messages that don't meet this condition

### The MapValues Operation

```yaml
- type: mapValues
  mapper:
    expression: {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
```

This operation:
- Transforms the value of each message
- Creates a new JSON object with:
  - The sensor ID (from the key)
  - The original temperature in Fahrenheit
  - The temperature converted to Celsius
- Preserves the original key

### The Peek Operation

```yaml
- type: peek
  forEach:
    functionRef: log_message
    args:
      message_type: "Processed"
```

This operation:
- Calls the `log_message` function for each message
- Passes "Processed" as the `message_type` parameter
- Doesn't modify the message
- Useful for debugging and monitoring

## Exploring and Modifying

Try these modifications to enhance your pipeline:

1. Add a temperature unit to the output:
```yaml
- type: mapValues
  mapper:
    expression: {"sensor": key, "temp_fahrenheit": str(value.get('temperature')) + "°F", "temp_celsius": str(round((value.get('temperature') - 32) * 5/9, 1)) + "°C"}
```

2. Filter based on multiple conditions:
```yaml
- type: filter
  if:
    expression: value.get('temperature') > 70 and value.get('humidity') < 50
```

3. Add a timestamp to each message:
```yaml
- type: mapValues
  mapper:
    expression: dict(list(value.items()) + [("timestamp", time.time())])
```

## Next Steps

Now that you've built a simple pipeline, you can:

- Learn about [filtering and transforming data](/docs/tutorials/beginner/filtering-transforming.md) in more detail
- Explore [logging and monitoring](/docs/tutorials/beginner/logging-monitoring.md) techniques
- Dive into [intermediate tutorials](/docs/tutorials/intermediate/index.md) to learn about aggregations and joins

## Conclusion

In this tutorial, you've learned how to:
- Set up a local Kafka environment
- Define streams in KSML
- Create a simple function
- Build a pipeline with filter, transform, and logging operations
- Run your pipeline and observe the results

These fundamental skills form the building blocks for more complex KSML applications.
