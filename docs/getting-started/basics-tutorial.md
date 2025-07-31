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

- Completed the [Installation and Setup](installation.md) guide with Docker Compose running
- Basic understanding of YAML syntax
- Your KSML environment running (`docker compose ps` should show all services as "Up")

## Understanding the KSML File Structure

A KSML definition file consists of three main sections:

1. **Streams**: Define the input and output Kafka topics
2. **Functions**: Define reusable code snippets
3. **Pipelines**: Define the data processing flow

Let's create each section step by step.

## Step 1: Define Your Streams

First, let's create a new file `tutorial.yaml` and start by defining the input and output streams for our pipeline:

```yaml
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Streams"
  end="## End of Streams"
%}
```

This defines:

- An input stream reading from the `temperature_data` topic with string keys and JSON values
- An output stream writing to the `temperature_data_converted` topic with the same data types

### Understanding Stream Definitions

Each KSML stream has:

- A unique name (e.g., `input_stream`)
- The Kafka topic it connects to
- The data types for keys and values

KSML supports various data types and notations including:

- `string`: For text data
- `json`: For JSON-formatted data
- `avro`: For Avro-formatted data (requires schema)
- `binary`: For raw binary data
- And [more](../reference/data-type-reference.md)

## Step 2: Create a Simple Function

Next, let's add functions to filter, transform and log messages as they flow through our pipeline:

```yaml
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Functions"
  end="## End of Functions"
%}
```

We defined three uniquely named functions:

**`temperature_above_threshold`** function:

- Is of type `predicate`, which means it [always gets](../reference/function-reference.md#predicate) a `key` and
  `value` as its parameters and needs to return a `boolean` value
- Uses the `expression` tag to return a `True` if the `temperature` field in the value (zero if it does not exist) is
  above 70, `False` otherwise.

**`fahrenheit_to_celsius`** function:

- Is of type `valueTransformer`, which means it [always gets](../reference/function-reference.md#valuetransformer)
  two parameters `key` and `value`, and does not return a value
- Uses the `expression` tag to define the value to return, in this case renaming `temperature` to `temp_fahrenheit` and
  adding a field called `temp_celsius`

**`log_message`** function:

- Is of type `forEach`, which means it [always gets](../reference/function-reference.md#foreach)  two parameters
  `key` and `value`, and does not return a value
- Takes an extra parameter `prefix` of type `string`
- Checks if the `prefix` variable is of the correct type, and not empty, and if so, uses it to compose a description for
  the log message
- Uses the built-in `log` object to output information at `info` level

### Understanding Functions in KSML

Functions in KSML:

- Can be reused across multiple operations in your pipelines
- Are written in Python
- Have access to pre-defined parameters based on [function type](../reference/function-reference.md)
- Can take additional parameters for more flexibility
- Must return a value if required by the function type

## Step 3: Build Your Pipeline

Now, let's add the pipeline that processes our data:

```yaml
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Pipelines"
  end="## End of Pipelines"
%}
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
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Filter Operation"
  end="## End of Filter Operation"
%}
```

The filter operation:

- Evaluates the expression for each message
- Only passes messages where the expression returns `True`
- Discards messages where the expression returns `False`

#### Transform Value Operation

```yaml
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Transform Value Operation"
  end="## End of Transform Value Operation"
%}
```

The transformValue operation:

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
{%
  include "../definitions/basics-tutorial/tutorial.yaml"
  start="## Peek Operation"
  end="## End of Peek Operation"
%}
```

The peek operation:

- Executes the provided code for each message
- Doesn't modify the message
- Allows the message to continue through the pipeline
- Is useful for logging, metrics, or other side effects

## Step 4: Put It All Together

Let's combine all the sections into a complete KSML definition file. In the [Quick Start guide](installation.md) you created a directory
structure containing an `examples/` directory; in this directory create a file called `tutorial.yaml` and copy
the following content:

```yaml
{% include "../definitions/basics-tutorial/tutorial.yaml" %}
```

Save the file.
We also need to make the KSML Runner aware of the new pipeline. In the `ksml-runner.yaml` you created before, there is
a section containing the definitions; modify this part so that it looks like this:

```yaml
ksml:
  definitions:
    # format is: <namespace>: <filename> 
    helloworld: hello-world.yaml 
    tutorial: tutorial.yaml
```

You can either replace the line containing `helloworld` or add the tutorial, in the latter case both pipelines will be run.

## Step 5: Run Your Pipeline definition(s)

Now let's run the pipeline using our Docker Compose setup. If the compose was still running, you can just restart the KSML runner to make it
aware of the new definition:

```bash
docker compose restart ksml
```

Or, if you stopped the setup, you can start the complete compose as before:

```bash
docker compose up -d 
```

Check the logs to verify your pipeline(s) started:

```bash
docker compose logs ksml
```

### Step 5.1: Test with Sample Data

Produce some test messages to the input topic:

```bash
docker compose exec broker kafka-console-producer.sh --bootstrap-server broker:9093 --topic temperature_data --property "parse.key=true" --property "key.separator=:"
```

Then enter messages in the format `key:value`:
```
sensor1:{"temperature": 75}
sensor2:{"temperature": 65}
sensor3:{"temperature": 80}
```

Press <Enter> after each record, and press Ctrl+C to exit the producer.

### Step 5.2: View the Results

Consume messages from the output topic to see the results:

```bash
docker compose exec broker kafka-console-consumer.sh --bootstrap-server broker:9093 --topic temperature_data_converted --from-beginning
```

You can also view the topics and messages in the Kafka UI at http://localhost:8080.

You should see messages like:

```
{"temp_fahrenheit":75,"temp_celsius":23.88888888888889}
{"temp_fahrenheit":80,"temp_celsius":26.666666666666668}
```

Notice that:

- The message with temperature 65°F was filtered out (below our 70°F threshold)
- The remaining messages have been transformed to include both Fahrenheit and Celsius temperatures
- You can see processing logs in the KSML container logs: `docker compose logs ksml`

## Understanding What's Happening

When you run your KSML definition:

1. The KSML runner parses your `tutorial.yaml` definition
2. It creates a Kafka Streams topology based on your pipeline definition
3. The topology starts consuming from the input topic
4. Each message flows through the operations you defined:
    - The filter operation drops messages with temperatures ≤ 70°F
    - The mapValues operation transforms the remaining messages
    - The peek operation logs each message
    - The messages are written to the output topic

## Using KSML to produce messages

While you can manually produce the above messages, KSML can also generate messages for you. 
To randomly generate test messages every three seconds, do the following:

In the `functions:` section of your KSML file, add the following function definition:

```yaml
  generate_temperature_message:
    type: generator
    globalCode: |
      import random
      sensorCounter = 0
    code: |
      global sensorCounter

      key = "sensor" + str(sensorCounter)         # Simulate 10 sensors "sensor0" to "sensor9"
      sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

      value = {"temperature": random.randrange(150)}
    expression: (key, value)                      # Return a message tuple with the key and value
    resultType: (string, json)                    # Indicate the type of key and value
```

Next, _before_ the section `pipelines:` in your defintion file, add a new section `producers:` as follows:

```yaml
producers:
  # Produce a temperature message every 3 seconds
  tutorial_producer:
    generator: generate_temperature_message
    interval: 3s
    to:
      topic: temperature_data
      keyType: string
      valueType: json
```

Restart the KSML Runner to make it aware of the new definitions:

```bash
docker compose restart ksml
```

You can check the runner logs (`docker compose logs ksml`) or go to the Kafka UI at [http://localhost:8080](http://localhost:8080)
to verify that new messages are generated in `temperature_data` topic and filtered and converted
messages will appear on topic `temperature_data_converted`.

## Next Steps

Congratulations! You've built your first KSML data pipeline. Here are some ways to expand on what you've learned:

### Try These Modifications:

1. Add another filter condition (e.g., filter by sensor name)
2. Add more fields to the transformed output
3. Create a second pipeline that processes the data differently

### Explore More Advanced Concepts:

- Learn about [stateful operations](../core-concepts/operations.md#stateful-operations) like aggregations and joins
- Explore [windowing operations](../core-concepts/operations.md#windowing-operations) for time-based processing
- Try working with different [data formats](../reference/stream-type-reference.md)

### Continue Your Learning Journey:

- Check out the [beginner tutorials](../tutorials/beginner/index.md) for more guided examples
- Explore the [examples library](../resources/examples-library.md) for inspiration
- Dive into the [reference documentation](../reference/operation-reference.md) to learn about all available
  operations
