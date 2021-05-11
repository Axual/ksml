# Introducing KSML: Kafka Streams for Low Code Environments

## Abstract
Kafka Streams has captured the hearts and minds of many developers that want to develop streaming applications on top of Kafka. But as powerful as the framework is, Kafka Streams has had a hard time getting around the requirement of writing Java code and setting up build pipelines. There were some attempts to rebuild Kafka Streams, but up until now popular languages like Python did not receive equally powerful (and maintained) stream processing frameworks. In this article we will present a new declarative approach to unlock Kafka Streams, called KSML. By the time you finish reading, you will be able to write streaming applications yourself, using only a few simple basic rules and Python snippets.

## Background
Axual is a vendor of Kafka-based streaming platforms to enterprise customers. Under the names Axual Platform and Axual Cloud, we sell our solution primarily to enterprise customers. Our overall mission is Streaming Made Simple, meaning that our products try to hide the complexities of Kafka for customers. We do this through delivery of a standard approach, standardized architecture and easy installation. For more information on Axual, visit https://axual.com.

One of the key features of Axual's products is self-service for DevOps teams. An intuitive UI allows customer teams to configure their piece of the common Kafka platform. Teams define topics, schemas and applications themselves. Strict ownership and approval processes provide the necessary data governance for enterprises.

Users of self-service are typically not very Kafka-savvy. They know about Kafka, may like it even, but their primary responsibility is to build a business application that happens to require interaction with Kafka. Self-service allows them to define their application, either as a custom application or as a Kafka Connector. But up until recently we had no way of easily defining "simple transformation" apps. Kafka Streams offers the basis for stream processing, but requires Java developers and (often complex) build pipelines.

To allow easier definition of streaming apps, we started to investigate if we could unlock the power of Kafka Streams without requiring Java.

## Inspiration from Home Assistant
During the Covid-19 lockdown I started to experiment with [Home Assistant](https://home-assistant.io), a home automation platform that integrates with everything in your house _and_ the [kitchen sink](https://www.youtube.com/watch?v=5Y5kGnZN4CQ). If you are into home automation, I highly recommend you check out this tremendous piece of software.

To allow any kind of device from any vendor to be integrated and controlled, Home Assistant defines its own YAML structures for automations, scripts and sensors amongst other things. The really nice thing about YAML is that it is interpreted and does not require compilation.

## Hypothesis
The Home Assistant way of defining custom logic got us thinking: could we use a similar structure to define Kafka Streams topologies? It would be easy to imagine a topology in the following way:

```yaml
pipelines:
  main:
    from: some_topic
    to: some_other_topic
```

This example defines a topology, which simply copies messages from a source topic to a target topic. But anyone familiar with Kafka Streams will immediately recognize that this can only be part of the overall solution. Kafka Streams requires a lot of custom user functions in Java, such as Predicates, Key/Value transformers and ValueJoiners. If we want to define everything in YAML, we need a compiler-free expression language.

We then turned to [Jython](https://www.jython.org), which is a complete Python implementation that runs in the Java JVM. Running Python code is as easy as creating an instance of PythonInterpreter and asking it to execute code provided as strings.

```Java
import org.python.util.PythonInterpreter;

public class JythonHelloWorld {
  public static void main(String[] args) {
    try(PythonInterpreter pyInterp = new PythonInterpreter()) {
      pyInterp.exec("print('Hello Python World!')");
    }
  }
}
```

Using a few days of tinkering, we were able to combine the two into a generic interpreter that we envisioned. Before diving into the examples, let's look at the test environment we used.

## Setting up a test environment

To illustrate KSML's capabilities, we set up a test topic, called `ksml_sensordata_avro` with key/value types of `String`/`SensorData`. The `SensorData` schema contains the following fields:

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

Next we created a producer, which generates random test data and produces it on the topic. We will leave out the specific of this producer, but you can assume it simply fills out all fields with random values picked from a list of possible values.

So without any further delays, let's see how KSML allows us to process this data.

## KSML in practice

### Example 1. Inspect data on a topic

The first example is one where we inspect data on a specific topic. The definition is as follows:

```yaml
streams:
  - topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData

functions:
  print_message:
    type: forEach
    code: "print('key='+(key if isinstance(key,str) else str(key))+', value='+(value if isinstance(value,str) else str(value)))"

pipelines:
  main:
    from: ksml_sensordata_avro
    forEach: print_message
```

Let's disect this definition one element at a time. Before defining processing logic, we first define the streams used by the definition. In this case we define the `ksml_sensordata_avro`, which as explained above has `string` key and `SensorData` values.

Next is a list of functions that can be used by the processing logic. Here we define just one, `print_message`, which simply prints the key and value of a message to stdout.

The third element `pipelines` defines the real processing logic. We define a pipeline called `main`, which takes messages from `ksml_sensordata_avro` and passes them to `print_message`.

The definition file is parsed by KSML and translated into a Kafka Streams topology, which is [described](https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/Topology.html#describe--) as follows:

```
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [ksml_sensordata_avro])
      --> KSTREAM-FOREACH-0000000001
    Processor: KSTREAM-FOREACH-0000000001 (stores: [])
      --> none
      <-- KSTREAM-SOURCE-0000000000
```

And the output of the generated topology looks like this:

```
key=sensor0, value={'owner': 'Evan', 'color': 'red', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'AREA', 'unit': 'ft2', 'name': 'sensor0', 'value': '225', 'timestamp': 1620217832071L}
key=sensor1, value={'owner': 'Charlie', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor1', 'value': '86', 'timestamp': 1620217833268L}
key=sensor2, value={'owner': 'Dave', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor2', 'value': '89', 'timestamp': 1620217833269L}
key=sensor3, value={'owner': 'Charlie', 'color': 'white', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor3', 'value': '392', 'timestamp': 1620217833269L}
key=sensor4, value={'owner': 'Dave', 'color': 'red', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'ft', 'name': 'sensor4', 'value': '459', 'timestamp': 1620217833270L}
key=sensor5, value={'owner': 'Bob', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'C', 'name': 'sensor5', 'value': '466', 'timestamp': 1620217833270L}
key=sensor6, value={'owner': 'Dave', 'color': 'red', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor6', 'value': '37', 'timestamp': 1620217833270L}
key=sensor7, value={'owner': 'Evan', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor7', 'value': '704', 'timestamp': 1620217833271L}
key=sensor8, value={'owner': 'Dave', 'color': 'red', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor8', 'value': 'on', 'timestamp': 1620217833271L}
key=sensor9, value={'owner': 'Dave', 'color': 'black', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor9', 'value': '67', 'timestamp': 1620217833272L}
key=sensor0, value={'owner': 'Evan', 'color': 'blue', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor0', 'value': '2', 'timestamp': 1620217833272L}
key=sensor1, value={'owner': 'Alice', 'color': 'black', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor1', 'value': '126', 'timestamp': 1620217833272L}
key=sensor2, value={'owner': 'Charlie', 'color': 'white', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor2', 'value': '58', 'timestamp': 1620217833273L}
```

As you can see, the output of the application is exactly that what we defined it to be in the `print_message` function, namely a dump of all data found on the topic.

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
  print_message:
    type: forEach
    code: "print('key='+(key if isinstance(key,str) else str(key))+', value='+(value if isinstance(value,str) else str(value)))"

pipelines:
  main:
    from: ksml_sensordata_avro
    via:
      - type: peek
        forEach: print_message
    to: ksml_sensordata_copy
```

You can see that we specified a second topic in this example, being the target topic that all messages are copied to. The `print_message` function is unchanged, but the pipeline did undergo some changes. Two new elements are introduced here, namely `via` and `to`.

The `via` tag allows users to define a series of operations executed on the data. In this case there is only one, namely a `peek` operation which does not modify any data, but simply outputs the data on stdout as a side-effect.

The `to` operation is a so-called "sink operation". Sink operations are always last in a pipeline. Processing of the pipeline does not continue after it was delivered to a sink operation. Note that in the first example above `forEach` is also a sink operation, whereas in this example we achieve the same result by passing the `print_message` function as a parameter to the `peek` operation.

When this definition is translated by KSML, the following Kafka Streams topology is created:

```
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [ksml_sensordata_copy])
      --> none

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000001 (topics: [ksml_sensordata_avro])
      --> KSTREAM-PEEK-0000000002
    Processor: KSTREAM-PEEK-0000000002 (stores: [])
      --> KSTREAM-SINK-0000000003
      <-- KSTREAM-SOURCE-0000000001
    Sink: KSTREAM-SINK-0000000003 (topic: ksml_sensordata_copy)
      <-- KSTREAM-PEEK-0000000002
```

The output is similar to that of example 1, but the same data can also be found on the `ksml_sensordata_copy` topic now.

### Example 3. Filtering data

Now that we can read and write data, let's see if we can apply some logic to the processing as well. In this example we will be filtering data based on the contents of the value:

```yaml
streams:
  - topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_filtered
    keyType: string
    valueType: avro:SensorData

functions:
  print_message:
    type: forEach
    code: "print('key='+(key if isinstance(key,str) else str(key))+', value='+(value if isinstance(value,str) else str(value)))"

  filter_message:
    type: predicate
    expression: value['color'] == 'blue'

pipelines:
  main:
    from: ksml_sensordata_avro
    via:
      - type: filter
        predicate: filter_message
      - type: peek
        forEach: print_message
    to: ksml_sensordata_filtered
```

Again, first we define the streams and the functions involved in the processing. You can see we added a new function called `filter_message` which returns `true` or `false` based on the `color` field in the value of the message. This function is used below in the pipeline.

The pipeline is extended to include a `filter` operation, which takes a `predicate` function as parameter. That function is called for every input message. Only messages for which the function returns `true` are propagated. All other messages are discarded.

Using this definition, KSML generates the following Kafka Streams topology:

```
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [ksml_sensordata_avro])
      --> KSTREAM-FILTER-0000000002
    Processor: KSTREAM-FILTER-0000000002 (stores: [])
      --> KSTREAM-PEEK-0000000003
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-PEEK-0000000003 (stores: [])
      --> KSTREAM-SINK-0000000004
      <-- KSTREAM-FILTER-0000000002
    Sink: KSTREAM-SINK-0000000004 (topic: ksml_sensordata_filtered)
      <-- KSTREAM-PEEK-0000000003

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000001 (topics: [ksml_sensordata_filtered])
      --> none
```

When it executes, we see the following output:

```
key=sensor0, value={'owner': 'Evan', 'color': 'blue', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor0', 'value': '2', 'timestamp': 1620217833272L}
key=sensor4, value={'owner': 'Bob', 'color': 'blue', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor4', 'value': 'on', 'timestamp': 1620217833273L}
key=sensor5, value={'owner': 'Bob', 'color': 'blue', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor5', 'value': '14', 'timestamp': 1620217833277L}
key=sensor6, value={'owner': 'Charlie', 'color': 'blue', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor6', 'value': 'off', 'timestamp': 1620217833278L}
key=sensor7, value={'owner': 'Bob', 'color': 'blue', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'AREA', 'unit': 'ft2', 'name': 'sensor7', 'value': '292', 'timestamp': 1620217833278L}
key=sensor4, value={'owner': 'Charlie', 'color': 'blue', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor4', 'value': '72', 'timestamp': 1620217833280L}
key=sensor5, value={'owner': 'Evan', 'color': 'blue', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor5', 'value': '876', 'timestamp': 1620217833281L}
key=sensor8, value={'owner': 'Alice', 'color': 'blue', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor8', 'value': 'off', 'timestamp': 1620217833282L}
key=sensor1, value={'owner': 'Evan', 'color': 'blue', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor1', 'value': '952', 'timestamp': 1620217833282L}
key=sensor2, value={'owner': 'Bob', 'color': 'blue', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor2', 'value': '602', 'timestamp': 1620217833286L}
```

As you can see, the filter operation did its work. Only messages with field `color` set to `blue` are passed on to the `peek` operation, while other messages are discarded.

### Example 4. Branching messages

Another way to filter messages is to use a `branch` operation. This is also a sink operation, which closes the processing of a pipeline. It is similar to `forEach` and `to` in that respect, but has a different definition and behaviour.

```yaml
streams:
  - topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_blue
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_red
    keyType: string
    valueType: avro:SensorData

functions:
  print_message:
    type: forEach
    code: "print('key='+(key if isinstance(key,str) else str(key))+', value='+(value if isinstance(value,str) else str(value)))"

pipelines:
  main:
    from: ksml_sensordata_avro
    via:
      - type: peek
        forEach: print_message
    branch:
      - if:
          expression: value['color'] == 'blue'
        to: ksml_sensordata_blue
      - if:
          expression: value['color'] == 'red'
        to: ksml_sensordata_red
      - forEach:
          code: |
            print('Unknown color sensor: '+str(value))
```

The `branch` operation takes a list of branches as its parameters, which each specifies a processing pipeline of its own. Branches contain the keyword `if`, which take a predicate function that determines if a message will flow into that particular branch, or if it will be passed to the next branch(es). Every message will only end up in one branch, namely the first one in order where the `if` predcate function returns `true`.

In the example we see that the first branch will be populated only with messages with `color` field set to `blue`. Once there, these messages will be written to `ksml_sensordata_blue`. The second branch will only contain messages with `color`=`red` and these messages will be written to `ksml_sensordata_red`. Finally, the last branch outputs a message that the color is unknown and ends any further processing.

When translated by KSML the following Kafka Streams topology is set up:

```
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [ksml_sensordata_blue])
      --> none

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000001 (topics: [ksml_sensordata_red])
      --> none

  Sub-topology: 2
    Source: KSTREAM-SOURCE-0000000002 (topics: [ksml_sensordata_avro])
      --> KSTREAM-PEEK-0000000003
    Processor: KSTREAM-PEEK-0000000003 (stores: [])
      --> KSTREAM-BRANCH-0000000004
      <-- KSTREAM-SOURCE-0000000002
    Processor: KSTREAM-BRANCH-0000000004 (stores: [])
      --> KSTREAM-BRANCHCHILD-0000000005, KSTREAM-BRANCHCHILD-0000000006, KSTREAM-BRANCHCHILD-0000000007
      <-- KSTREAM-PEEK-0000000003
    Processor: KSTREAM-BRANCHCHILD-0000000005 (stores: [])
      --> KSTREAM-SINK-0000000008
      <-- KSTREAM-BRANCH-0000000004
    Processor: KSTREAM-BRANCHCHILD-0000000006 (stores: [])
      --> KSTREAM-SINK-0000000009
      <-- KSTREAM-BRANCH-0000000004
    Processor: KSTREAM-BRANCHCHILD-0000000007 (stores: [])
      --> KSTREAM-FOREACH-0000000010
      <-- KSTREAM-BRANCH-0000000004
    Processor: KSTREAM-FOREACH-0000000010 (stores: [])
      --> none
      <-- KSTREAM-BRANCHCHILD-0000000007
    Sink: KSTREAM-SINK-0000000008 (topic: ksml_sensordata_blue)
      <-- KSTREAM-BRANCHCHILD-0000000005
    Sink: KSTREAM-SINK-0000000009 (topic: ksml_sensordata_red)
      <-- KSTREAM-BRANCHCHILD-0000000006
```

It is clear that the branch operation is integrated in this topology. Its output looks like this:

```
key=sensor0, value={'owner': 'Evan', 'color': 'red', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'AREA', 'unit': 'ft2', 'name': 'sensor0', 'value': '225', 'timestamp': 1620217832071L}
key=sensor1, value={'owner': 'Charlie', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor1', 'value': '86', 'timestamp': 1620217833268L}
key=sensor2, value={'owner': 'Dave', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor2', 'value': '89', 'timestamp': 1620217833269L}
key=sensor3, value={'owner': 'Charlie', 'color': 'white', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor3', 'value': '392', 'timestamp': 1620217833269L}
Unknown color sensor: {'owner': 'Charlie', 'color': 'white', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor3', 'value': '392', 'timestamp': 1620217833269L}
key=sensor4, value={'owner': 'Dave', 'color': 'red', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'ft', 'name': 'sensor4', 'value': '459', 'timestamp': 1620217833270L}
key=sensor5, value={'owner': 'Bob', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'C', 'name': 'sensor5', 'value': '466', 'timestamp': 1620217833270L}
key=sensor6, value={'owner': 'Dave', 'color': 'red', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor6', 'value': '37', 'timestamp': 1620217833270L}
key=sensor7, value={'owner': 'Evan', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor7', 'value': '704', 'timestamp': 1620217833271L}
key=sensor8, value={'owner': 'Dave', 'color': 'red', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor8', 'value': 'on', 'timestamp': 1620217833271L}
key=sensor9, value={'owner': 'Dave', 'color': 'black', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor9', 'value': '67', 'timestamp': 1620217833272L}
Unknown color sensor: {'owner': 'Dave', 'color': 'black', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': 'g/m3', 'name': 'sensor9', 'value': '67', 'timestamp': 1620217833272L}
key=sensor0, value={'owner': 'Evan', 'color': 'blue', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor0', 'value': '2', 'timestamp': 1620217833272L}
key=sensor1, value={'owner': 'Alice', 'color': 'black', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor1', 'value': '126', 'timestamp': 1620217833272L}
Unknown color sensor: {'owner': 'Alice', 'color': 'black', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor1', 'value': '126', 'timestamp': 1620217833272L}
key=sensor2, value={'owner': 'Charlie', 'color': 'white', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor2', 'value': '58', 'timestamp': 1620217833273L}
```

We see that every message processed by the pipeline is sent through the `print_message` function. But the branch operation sorts the messages and sends messages with colors `blue` and `red` into their own branches. The only colors that show up as `Unknown color sensor` messages are non-blue and non-red.

### 5. Dynamic routing

As the last example in this article, we will route messages dynamically using Kafka Streams' [TopicNameExtractor](https://kafka.apache.org/27/javadoc/index.html?org/apache/kafka/streams/processor/TopicNameExtractor.html).

```yaml
streams:
  - topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_sensor0
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_sensor1
    keyType: string
    valueType: avro:SensorData
  - topic: ksml_sensordata_sensor2
    keyType: string
    valueType: avro:SensorData

functions:
  print_message:
    type: forEach
    code: "print('key='+(key if isinstance(key,str) else str(key))+', value='+(value if isinstance(value,str) else str(value)))"

pipelines:
  main:
    from: ksml_sensordata_avro
    via:
      - type: peek
        forEach: print_message
    toExtractor:
      code: |
        if key == 'sensor1':
          return 'ksml_sensordata_sensor1'
        elif key == 'sensor2':
          return 'ksml_sensordata_sensor2'
        else:
          return 'ksml_sensordata_sensor0'
```

The `toExtractor` operation takes a function, which determines the routing of every message by returning a topic name string. In this case, when the key of a message is `sensor1` then the message will be sent to `ksml_sensordata_sensor1`. When it contains `sensor2` the message is sent to `ksml_sensordata_sensor2`. All other messages are sent to `ksml_sensordata_sensor0`.

The equivalent Kafka Streams topology looks like this:

```
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [ksml_sensordata_sensor0])
      --> none

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000001 (topics: [ksml_sensordata_avro])
      --> KSTREAM-PEEK-0000000004
    Processor: KSTREAM-PEEK-0000000004 (stores: [])
      --> KSTREAM-SINK-0000000005
      <-- KSTREAM-SOURCE-0000000001
    Sink: KSTREAM-SINK-0000000005 (extractor class: io.axual.ksml.user.UserTopicNameExtractor@713529c2)
      <-- KSTREAM-PEEK-0000000004

  Sub-topology: 2
    Source: KSTREAM-SOURCE-0000000002 (topics: [ksml_sensordata_sensor2])
      --> none

  Sub-topology: 3
    Source: KSTREAM-SOURCE-0000000003 (topics: [ksml_sensordata_sensor1])
      --> none
```

The output does not show anything special, since all messages in this example are simply outputed to stdout.

## Current status

So after having seen these examples, I hope I was able to spark your interest. So it is good to understand the current status of the project.

KSML was recently released as version 0.0.1, indicating that people should treat this as a technology preview (or alpha) software and not use it in production. The reason for writing this article is that we think there is potential for a new community to be formed around KSML. We expect input and perspectives of others to greatly benefit the state of the language as well as the implementation, so we want to tap into your collective enthusiasm early.

The state of KSML is already much more advanced than what we have shown here. In this article we only touched upon the simplest operations currently supported. In fact, almost all Kafka Streams operations are already implemented, though not fully tested. In terms of data types, we support all primitive types (strings, integers, booleans etcetera), basic Avro handling (through Python dicts) and will be introducing Json support soon. This makes KSML already quite capable to handle simple stream processing logic.

## Next steps

For the near future we foresee three main activities around KSML:

* __Test, test, test__: we have tested quite a lot of processing logic, but internal refactoring to reach a first alpha release has probably introduced some bugs that we overlooked at this stage. Therefore we welcome everyone to give it a go and to come up with findings, suggestions and improvements.
* __Build community__: using this article and several presentations, we want to get your honest feedback on this idea. Would it be of value to you and your use cases? How would you want to run these kinds of processing pipelines? As a library, a Docker image or perhaps on a generic processing container like Kafka Connect provides?
* __Work towards a beta release__: we know that the current code is very much alpha. Unit tests and integration tests are severely lacking, but we did not focus on these at this stage, because finding out about usefulness of the concept is more important than elegant code at this stage. But of course, as we evolve the KSML language, its typing model and its implementation, code quality needs serious attention.

If this article has sparked your interest, feel free to take a test drive and let us hear your experiences. You can find us at https://ksml.io.

## About the author
Jeroen van Disseldorp is CEO and Founder of Axual, a company that provides a ready-to-use distribution of Apache Kafka for enterprises. He has a broad background in systems development and architecture. Before Axual, Jeroen worked as a principal consultant for Capgemini and was global thought leader for open source software.
