[<< Back to index](index.md)

# KSML: Kafka Streams for Low Code Environments

## Abstract
Kafka Streams has captured the hearts and minds of many developers that want to develop streaming applications on top of Kafka. But as powerful as the framework is, Kafka Streams has had a hard time getting around the requirement of writing Java code and setting up build pipelines. There were some attempts to rebuild Kafka Streams, but up until now popular languages like Python did not receive equally powerful (and maintained) stream processing frameworks. In this article we will present a new declarative approach to unlock Kafka Streams, called KSML. By the time you finish reading this document, you will be able to write streaming applications yourself, using only a few simple basic rules and Python snippets.

* [Setting up a test environment](#setting-up-a-test-environment)
* [KSML in practice](#ksml-in-practice)
  * [Example 1. Inspect data on a topic](#example-1-inspect-data-on-a-topic)
  * [Example 2. Copying data to another topic](#example-2-copying-data-to-another-topic)
  * [Example 3. Filtering data](#example-3-filtering-data)
  * [Example 4. Branching messages](#example-4-branching-messages)
  * [Example 5. Dynamic routing](#example-5-dynamic-routing)
  * [Example 6. Multiple pipelines](#example-6-multiple-pipelines)


## Setting up a test environment

To demonstrate KSML's capabilities, you will need a working Kafka cluster, or an Axual Platform/Cloud environment. Check out the [Runners](runners.md) page to configure KSML
we set up a test topic, called `ksml_sensordata_avro` with key/value types of `String`/`SensorData`. The [SensorData](../examples/SensorData.avsc) schema was created for demo purposes only and contains several fields to demonstratie KSML capabilities:

```json
{
  "namespace": "io.axual.ksml.example",
  "doc": "Emulated sensor data with a few additional attributes",
  "name": "SensorData",
  "type": "record",
  "fields": [
    {
      "doc": "The name of the sensor",
      "name": "name",
      "type": "string"
    },
    {
      "doc": "The timestamp of the sensor reading",
      "name": "timestamp",
      "type": "long"
    },
    {
      "doc": "The value of the sensor, represented as string",
      "name": "value",
      "type": "string"
    },
    {
      "doc": "The type of the sensor",
      "name": "type",
      "type": {
        "name": "SensorType",
        "type": "enum",
        "doc": "The type of a sensor",
        "symbols": [
          "AREA",
          "HUMIDITY",
          "LENGTH",
          "STATE",
          "TEMPERATURE"
        ]
      }
    },
    {
      "doc": "The unit of the sensor",
      "name": "unit",
      "type": "string"
    },
    {
      "doc": "The color of the sensor",
      "name": "color",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "doc": "The city of the sensor",
      "name": "city",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "doc": "The owner of the sensor",
      "name": "owner",
      "type": [
        "null",
        "string"
      ],
      "default": null
    }
  ]
}
```

For the rest of this document, we assume you have set up the `ksml_sensordata_avro` topic and populated it with some random data.

So without any further delays, let's see how KSML allows us to process this data.

## KSML in practice

### Example 1. Inspect data on a topic

The first example is one where we inspect data on a specific topic. The definition is as follows:

```yaml
streams:
  sensor_source_avro:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData

functions:
  # Log the message using the built-in log variable that is passed in from Java
  log_message:
    type: forEach
    parameters:
      - name: format
        type: string
    code: log.info("Consumed {} message - key={}, value={}", format, key, value)

pipelines:
  # Multiple pipelines can be created in a single KSML definition
  consume_avro:
    from: sensor_source_avro
    forEach:
      code: log_message(key, value, format="AVRO")

```

Let's disect this definition one element at a time. Before defining processing logic, we first define the streams used by the definition. In this case we define a stream named `sensor_source_avro` which reads from the topic `ksml_sensordata_avro`. The stream defines a `string` key and Avro `SensorData` values.

Next is a list of functions that can be used by the processing logic. Here we define just one, `log_message`, which simply uses the provided logger to write the key, value and format of a message to the console.

The third element `pipelines` defines the real processing logic. We define a pipeline called `consume_avro`, which takes messages from `ksml_sensordata_avro` and passes them to `print_message`.

The definition file is parsed by KSML and translated into a Kafka Streams topology, which is [described](https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/Topology.html#describe--) as follows:

```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> inspect_inspect_pipelines_consume_avro
    Processor: inspect_inspect_pipelines_consume_avro (stores: [])
      --> none
      <-- ksml_sensordata_avro
```

And the output of the generated topology looks like this:

```
2024-03-06T18:31:57,196Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Alkmaar', 'color': 'yellow', 'name': 'sensor9', 'owner': 'Bob', 'timestamp': 1709749917190, 'type': 'LENGTH', 'unit': 'm', 'value': '562', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:57,631Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor3, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor3', 'owner': 'Bob', 'timestamp': 1709749917628, 'type': 'HUMIDITY', 'unit': 'g/m3', 'value': '23', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,082Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor6, value={'city': 'Amsterdam', 'color': 'white', 'name': 'sensor6', 'owner': 'Bob', 'timestamp': 1709749918078, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '64', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,528Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor9', 'owner': 'Evan', 'timestamp': 1709749918524, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '87', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,970Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor1, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709749918964, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:59,412Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor5, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor5', 'owner': 'Bob', 'timestamp': 1709749919409, 'type': 'LENGTH', 'unit': 'm', 'value': '658', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
```

As you can see, the output of the application is exactly that what we defined it to be in the `log_message` function, namely a dump of all data found on the topic.

### Example 2. Copying data to another topic

Now that we can see what data is on a topic, we will start to manipulate its routing. In this example we are copying unmodified data to a secondary topic:

```yaml
streams:
  - topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_copy
    keyType: string
    valueType: avro:SensorData

functions:
  # Log the message using the built-in log variable that is passed in from Java
  log_message:
    type: forEach
    parameters:
      - name: format
        type: string
    code: log.info("Consumed {} message - key={}, value={}", format, key, value)

pipelines:
  # Every pipeline logs its own message, passing in the format parameter to log_message above
  consume_avro:
    from: sensor_source_avro
    via:
      - type: peek
        forEach:
          code: log_message(key, value, format="AVRO")
    to: sensor_copy
```

You can see that we specified a second stream named `sensor_copy`  in this example, which is backed by the topic `ksml_sensordata_copy` target topic. The `log_message` function is unchanged, but the pipeline did undergo some changes. Two new elements are introduced here, namely `via` and `to`.

The `via` tag allows users to define a series of operations executed on the data. In this case there is only one, namely a `peek` operation which does not modify any data, but simply outputs the data on stdout as a side-effect.

The `to` operation is a so-called "sink operation". Sink operations are always last in a pipeline. Processing of the pipeline does not continue after it was delivered to a sink operation. Note that in the first example above `forEach` is also a sink operation, whereas in this example we achieve the same result by passing the `log_message` function as a parameter to the `peek` operation.

When this definition is translated by KSML, the following Kafka Streams topology is created:

```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> inspect_inspect_pipelines_consume_avro_via_1
    Processor: inspect_inspect_pipelines_consume_avro_via_1 (stores: [])
      --> inspect_inspect_ToOperationParser_001
      <-- ksml_sensordata_avro
    Sink: inspect_inspect_ToOperationParser_001 (topic: ksml_sensordata_copy)
      <-- inspect_inspect_pipelines_consume_avro_via_1

```

The output is similar to that of example 1, but the same data can also be found on the `ksml_sensordata_copy` topic now.

### Example 3. Filtering data

Now that we can read and write data, let's see if we can apply some logic to the processing as well. In this example we will be filtering data based on the contents of the value:

```yaml
# This example shows how to read from four simple streams and log all messages

streams:
  sensor_source_avro:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  sensor_filtered:
    topic: ksml_sensordata_filtered
    keyType: string
    valueType: avro:SensorData


functions:
  # Log the message using the built-in log variable that is passed in from Java
  log_message:
    type: forEach
    parameters:
      - name: format
        type: string
    code: log.info("Consumed {} message - key={}, value={}", format, key, value)

  sensor_is_blue:
    type: predicate
    code: |
      if value["color"] == "blue":
        return True
    expression: False

pipelines:
  # Every pipeline logs its own message, passing in the format parameter to log_message above
  consume_avro:
    from: sensor_source_avro
    via:
      - type: filter
        if: sensor_is_blue
      - type: peek
        forEach:
          code: log_message(key, value, format="AVRO")
    to: sensor_filtered
```

Again, first we define the streams and the functions involved in the processing. You can see we added a new function called `filter_message` which returns `true` or `false` based on the `color` field in the value of the message. This function is used below in the pipeline.

The pipeline is extended to include a `filter` operation, which takes a `predicate` function as parameter. That function is called for every input message. Only messages for which the function returns `true` are propagated. All other messages are discarded.

Using this definition, KSML generates the following Kafka Streams topology:

```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> inspect_inspect_pipelines_consume_avro_via_1
    Processor: inspect_inspect_pipelines_consume_avro_via_1 (stores: [])
      --> inspect_inspect_pipelines_consume_avro_via_2
      <-- ksml_sensordata_avro
    Processor: inspect_inspect_pipelines_consume_avro_via_2 (stores: [])
      --> inspect_inspect_ToOperationParser_001
      <-- inspect_inspect_pipelines_consume_avro_via_1
    Sink: inspect_inspect_ToOperationParser_001 (topic: ksml_sensordata_filtered)
      <-- inspect_inspect_pipelines_consume_avro_via_2

```

When it executes, we see the following output:

```
2024-03-06T18:45:10,401Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Alkmaar', 'color': 'blue', 'name': 'sensor9', 'owner': 'Bob', 'timestamp': 1709749917190, 'type': 'LENGTH', 'unit': 'm', 'value': '562', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:45:10,735Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor3, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor3', 'owner': 'Bob', 'timestamp': 1709749917628, 'type': 'HUMIDITY', 'unit': 'g/m3', 'value': '23', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:45:11,215Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor6, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor6', 'owner': 'Bob', 'timestamp': 1709749918078, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '64', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:45:11,484Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor9', 'owner': 'Evan', 'timestamp': 1709749918524, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '87', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:45:11,893Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor1, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709749918964, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:45:12,008Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor5, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor5', 'owner': 'Bob', 'timestamp': 1709749919409, 'type': 'LENGTH', 'unit': 'm', 'value': '658', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
```

As you can see, the filter operation did its work. Only messages with field `color` set to `blue` are passed on to the `peek` operation, while other messages are discarded.

### Example 4. Branching messages

Another way to filter messages is to use a `branch` operation. This is also a sink operation, which closes the processing of a pipeline. It is similar to `forEach` and `to` in that respect, but has a different definition and behaviour.

```yaml
streams:
  sensor_source:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  sensor_blue:
    topic: ksml_sensordata_blue
    keyType: string
    valueType: avro:SensorData
  sensor_red:
    topic: ksml_sensordata_red
    keyType: string
    valueType: avro:SensorData

pipelines:
  main:
    from: sensor_source
    via:
      - type: peek
        forEach:
          code: log.info("SOURCE MESSAGE - key={}, value={}", key, value)
    branch:
      - if:
          expression: value != None and value["color"] == "blue"
        to: sensor_blue
      - if:
          expression: value != None and value["color"] == "red"
        to: sensor_red
      - forEach:
          code: log.warn("UNKNOWN COLOR - {}", value["color"])
```

The `branch` operation takes a list of branches as its parameters, which each specifies a processing pipeline of its own. Branches contain the keyword `if`, which take a predicate function that determines if a message will flow into that particular branch, or if it will be passed to the next branch(es). Every message will only end up in one branch, namely the first one in order where the `if` predcate function returns `true`.

In the example we see that the first branch will be populated only with messages with `color` field set to `blue`. Once there, these messages will be written to `ksml_sensordata_blue`. The second branch will only contain messages with `color`=`red` and these messages will be written to `ksml_sensordata_red`. Finally, the last branch outputs a message that the color is unknown and ends any further processing.

When translated by KSML the following Kafka Streams topology is set up:

```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> branch_branch_pipelines_main_via_1
    Processor: branch_branch_pipelines_main_via_1 (stores: [])
      --> branch_branch_branch_001
      <-- ksml_sensordata_avro
    Processor: branch_branch_branch_001 (stores: [])
      --> branch_branch_branch_001-predicate-0, branch_branch_branch_001-predicate-1, branch_branch_branch_001-predicate-2
      <-- branch_branch_pipelines_main_via_1
    Processor: branch_branch_branch_001-predicate-0 (stores: [])
      --> branch_branch_ToOperationParser_001
      <-- branch_branch_branch_001
    Processor: branch_branch_branch_001-predicate-1 (stores: [])
      --> branch_branch_ToOperationParser_002
      <-- branch_branch_branch_001
    Processor: branch_branch_branch_001-predicate-2 (stores: [])
      --> branch_branch_pipelines_main_branch_3
      <-- branch_branch_branch_001
    Sink: branch_branch_ToOperationParser_001 (topic: ksml_sensordata_blue)
      <-- branch_branch_branch_001-predicate-0
    Sink: branch_branch_ToOperationParser_002 (topic: ksml_sensordata_red)
      <-- branch_branch_branch_001-predicate-1
    Processor: branch_branch_pipelines_main_branch_3 (stores: [])
      --> none
      <-- branch_branch_branch_001-predicate-2

```

It is clear that the branch operation is integrated in this topology. Its output looks like this:

```
2024-03-06T18:31:57,196Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor9, value={'city': 'Alkmaar', 'color': 'yellow', 'name': 'sensor9', 'owner': 'Bob', 'timestamp': 1709749917190, 'type': 'LENGTH', 'unit': 'm', 'value': '562', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:57,631Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor3, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor3', 'owner': 'Bob', 'timestamp': 1709749917628, 'type': 'HUMIDITY', 'unit': 'g/m3', 'value': '23', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,082Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor6, value={'city': 'Amsterdam', 'color': 'white', 'name': 'sensor6', 'owner': 'Bob', 'timestamp': 1709749918078, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '64', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,528Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor9, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor9', 'owner': 'Evan', 'timestamp': 1709749918524, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '87', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,529Z WARN  k.f.branch_pipelines_main_branch_3_forEach UNKNOWN COLOR - black
2024-03-06T18:31:58,970Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor1, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709749918964, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T18:31:58,972Z WARN  k.f.branch_pipelines_main_branch_3_forEach UNKNOWN COLOR - black
2024-03-06T18:31:59,412Z INFO  k.f.branch_pipelines_main_via_1_forEach  SOURCE MESSAGE - key=sensor5, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor5', 'owner': 'Bob', 'timestamp': 1709749919409, 'type': 'LENGTH', 'unit': 'm', 'value': '658', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
```

We see that every message processed by the pipeline is logged through the `k.f.branch_pipelines_main_via_1_forEach` logger. But the branch operation sorts the messages and sends messages with colors `blue` and `red` into their own branches. The only colors that show up as `UNKNOWN COLOR -` messages are non-blue and non-red and send through the `branch_pipelines_main_branch_3_forEach` logger.

### Example 5. Dynamic routing

Sometimes it is necessary to route a message to one stream or another based on the content of a message. This example shows how to route messages dynamically using a TopicNameExtractor.

```yaml
streams:
  sensor_source:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData


pipelines:
  main:
    from: sensor_source
    via:
      - type: peek
        forEach:
          code: log.info("SOURCE MESSAGE - key={}, value={}", key, value)
    to:
      topicNameExtractor:
        code: |
          if key == 'sensor1':
            return 'ksml_sensordata_sensor1'
          if key == 'sensor2':
            return 'ksml_sensordata_sensor2'
          return 'ksml_sensordata_sensor0'
```

The `topicNameExtractor` operation takes a function, which determines the routing of every message by returning a topic name string. In this case, when the key of a message is `sensor1` then the message will be sent to `ksml_sensordata_sensor1`. When it contains `sensor2` the message is sent to `ksml_sensordata_sensor2`. All other messages are sent to `ksml_sensordata_sensor0`.

The equivalent Kafka Streams topology looks like this:

```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> route_route_pipelines_main_via_1
    Processor: route_route_pipelines_main_via_1 (stores: [])
      --> route_route_ToOperationParser_001
      <-- ksml_sensordata_avro
    Sink: route_route_ToOperationParser_001 (extractor class: io.axual.ksml.user.UserTopicNameExtractor@5d28e108)
      <-- route_route_pipelines_main_via_1
```

The output does not show anything special compared to previous examples, since all messages are simply written by the logger.

### Example 6. Multiple pipelines

In the previous examples there was always a single pipeline definition for processing data. KSML allows us to define multiple pipelines in a single file.

In this example we combine the filtering example with the routing example. We will also define new pipelines with the sole purpose of logging the routed messages.

```yaml
# This example shows how to route messages to a dynamic topic. The target topic is the result of an executed function.

streams:
  sensor_source_avro:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  sensor_filtered:
    topic: ksml_sensordata_filtered
    keyType: string
    valueType: avro:SensorData
  sensor_0:
    topic: ksml_sensordata_sensor0
    keyType: string
    valueType: avro:SensorData
  sensor_1:
    topic: ksml_sensordata_sensor1
    keyType: string
    valueType: avro:SensorData
  sensor_2:
    topic: ksml_sensordata_sensor2
    keyType: string
    valueType: avro:SensorData

functions:
  # Only pass the message to the next step in the pipeline if the color is blue
  sensor_is_blue:
    type: predicate
    code: |
      if value["color"] == "blue":
        return True
    expression: False

pipelines:
  filtering:
    from: sensor_source_avro
    via:
      - type: filter
        if: sensor_is_blue
    to: sensor_filtered

  routing:
    from: sensor_filtered
    via:
      - type: peek
        forEach:
          code: log.info("Routing Blue sensor - key={}, value={}", key, value)
    to:
      topicNameExtractor:
        code: |
          if key == 'sensor1':
            return 'ksml_sensordata_sensor1'
          if key == 'sensor2':
            return 'ksml_sensordata_sensor2'
          return 'ksml_sensordata_sensor0'

  sensor0_peek:
    from: sensor_0
    forEach:
      code: log.info("SENSOR0 - key={}, value={}", key, value)

  sensor1_peek:
    from: sensor_1
    forEach:
      code: log.info("SENSOR1 - key={}, value={}", key, value)

  sensor2_peek:
    from: sensor_2
    forEach:
      code: log.info("SENSOR2 - key={}, value={}", key, value)
```

In this definition we defined five pipelines:
1. `filtering` which filters out all sensor messages that don't have the color blue and sends it to the `sensor_filtered` stream.
2. `routing` which routes the data on the `sensor_filtered` stream to one of three target topics
3. `sensor0_peek` which writes the content of the `sensor_0` stream to the console
4. `sensor1_peek` which writes the content of the `sensor_1` stream to the console
5. `sensor2_peek` which writes the content of the `sensor_2` stream to the console

The equivalent Kafka Streams topology looks like this:
```
Topologies:
   Sub-topology: 0
    Source: ksml_sensordata_avro (topics: [ksml_sensordata_avro])
      --> multiple_multiple_pipelines_filtering_via_1
    Processor: multiple_multiple_pipelines_filtering_via_1 (stores: [])
      --> multiple_multiple_ToOperationParser_001
      <-- ksml_sensordata_avro
    Sink: multiple_multiple_ToOperationParser_001 (topic: ksml_sensordata_filtered)
      <-- multiple_multiple_pipelines_filtering_via_1

  Sub-topology: 1
    Source: ksml_sensordata_filtered (topics: [ksml_sensordata_filtered])
      --> multiple_multiple_pipelines_routing_via_1
    Processor: multiple_multiple_pipelines_routing_via_1 (stores: [])
      --> multiple_multiple_ToOperationParser_002
      <-- ksml_sensordata_filtered
    Sink: multiple_multiple_ToOperationParser_002 (extractor class: io.axual.ksml.user.UserTopicNameExtractor@2700f556)
      <-- multiple_multiple_pipelines_routing_via_1

  Sub-topology: 2
    Source: ksml_sensordata_sensor0 (topics: [ksml_sensordata_sensor0])
      --> multiple_multiple_pipelines_sensor0_peek
    Processor: multiple_multiple_pipelines_sensor0_peek (stores: [])
      --> none
      <-- ksml_sensordata_sensor0

  Sub-topology: 3
    Source: ksml_sensordata_sensor1 (topics: [ksml_sensordata_sensor1])
      --> multiple_multiple_pipelines_sensor1_peek
    Processor: multiple_multiple_pipelines_sensor1_peek (stores: [])
      --> none
      <-- ksml_sensordata_sensor1

  Sub-topology: 4
    Source: ksml_sensordata_sensor2 (topics: [ksml_sensordata_sensor2])
      --> multiple_multiple_pipelines_sensor2_peek
    Processor: multiple_multiple_pipelines_sensor2_peek (stores: [])
      --> none
      <-- ksml_sensordata_sensor2
```

And this is what the output would look something like this. The sensor peeks messages will not always be shown immediately after the Routing messages. This is because the pipelines are running in separate sub processes.
```
2024-03-06T20:11:39,520Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor6, value={'city': 'Utrecht', 'color': 'blue', 'name': 'sensor6', 'owner': 'Charlie', 'timestamp': 1709755877401, 'type': 'LENGTH', 'unit': 'ft', 'value': '507', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,523Z INFO  k.f.route2_pipelines_sensor0_peek_forEach SENSOR0 - key=sensor6, value={'city': 'Utrecht', 'color': 'blue', 'name': 'sensor6', 'owner': 'Charlie', 'timestamp': 1709755877401, 'type': 'LENGTH', 'unit': 'ft', 'value': '507', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,533Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor1, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor1', 'owner': 'Evan', 'timestamp': 1709755889834, 'type': 'LENGTH', 'unit': 'm', 'value': '609', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,535Z INFO  k.f.route2_pipelines_sensor1_peek_forEach SENSOR1 - key=sensor1, value={'city': 'Xanten', 'color': 'blue', 'name': 'sensor1', 'owner': 'Evan', 'timestamp': 1709755817913, 'type': 'STATE', 'unit': 'state', 'value': 'on', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,539Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor7, value={'city': 'Utrecht', 'color': 'blue', 'name': 'sensor7', 'owner': 'Evan', 'timestamp': 1709755892051, 'type': 'HUMIDITY', 'unit': '%', 'value': '77', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,5419Z INFO  k.f.route2_pipelines_sensor0_peek_forEach SENSOR0 - key=sensor7, value={'city': 'Utrecht', 'color': 'blue', 'name': 'sensor7', 'owner': 'Evan', 'timestamp': 1709755892051, 'type': 'HUMIDITY', 'unit': '%', 'value': '77', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,546Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor2, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor2', 'owner': 'Bob', 'timestamp': 1709755893390, 'type': 'HUMIDITY', 'unit': '%', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,549Z INFO  k.f.route2_pipelines_sensor1_peek_forEach SENSOR2 - key=sensor2, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor2', 'owner': 'Bob', 'timestamp': 1709755893390, 'type': 'HUMIDITY', 'unit': '%', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,552Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor1, value={'city': 'Xanten', 'color': 'blue', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709755894717, 'type': 'HUMIDITY', 'unit': '%', 'value': '76', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,555Z INFO  k.f.route2_pipelines_sensor1_peek_forEach SENSOR1 - key=sensor1, value={'city': 'Xanten', 'color': 'blue', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709755894717, 'type': 'HUMIDITY', 'unit': '%', 'value': '76', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,558Z INFO  k.f.route2_pipelines_routing_via_1_forEach Routing Blue sensor - key=sensor9, value={'city': 'Alkmaar', 'color': 'blue', 'name': 'sensor9', 'owner': 'Alice', 'timestamp': 1709755896937, 'type': 'HUMIDITY', 'unit': '%', 'value': '65', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:11:39,562Z INFO  k.f.route2_pipelines_sensor0_peek_forEach SENSOR0 - key=sensor9, value={'city': 'Alkmaar', 'color': 'blue', 'name': 'sensor9', 'owner': 'Alice', 'timestamp': 1709755896937, 'type': 'HUMIDITY', 'unit': '%', 'value': '65', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
```