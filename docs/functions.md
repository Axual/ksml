# Functions

### Table of Contents

* [Introduction](#introduction)
* [Data types in Python](#data-types-in-python)
    * [Data type mapping](#data-type-mapping)
    * [Automatic conversion](#automatic-conversion)
* [Function Types](#function-types)
* [Function parameters](#function-parameters)
    * [Logger](#logger)
    * [Metrics](#metrics)
    * [State stores](#state-stores)

## Introduction

Functions can be specified in the `functions` section of a KSML definition file. The layout typically looks like this:

```yaml
functions:
  my_first_predicate:
    type: predicate
    expression: key=='Some string'

  compare_params:
    type: generic
    parameters:
      - name: firstParam
        type: string
      - name: secondParam
        type: int
    globalCode: |
      import something from package
      globalVar = 3
    code: |
      print('Hello there!')
    expression: firstParam == str(secondParam)
```

Functions are defined by the following tags:

| Parameter    | Value Type                    | Default      | Description                                                                                                                                                                                                            |
|:-------------|:------------------------------|:-------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`       | `string`                      | `generic`    | The [type](#function-types) of the function defined                                                                                                                                                                    |
| `parameters` | List of parameter definitions | _empty list_ | A list of parameters, each of which contains the mandatory fields `name` and `type`. See example above.                                                                                                                |
| `globalCode` | `string`                      | _empty_      | Snippet of Python code that is executed once upon creation of the Kafka Streams topology. This section can contain statements like `import` to import function libraries used in the `code` and `expression` sections. |
| `code`       | `string`                      | _empty_      | Python source code, which will be included in the called function.                                                                                                                                                     |Bla
| `expression` | `string`                      | _empty_      | Python expression that contains the returned function result.                                                                                                                                                          |Typically the code `"return expression"` is generated for the Python interpreter. For example `expression: key` would generate the Python code `return key` for the function.

See below for the list of supported function types.

## Data types in Python

Internally, KSML uses an abstraction to deal with all kinds of data types.
See [types](types.md) for more information on data types.

### Data type mapping

Data types are automatically converted to/from Python in the following manner:

| Data type          | Python type  | Example                                                                         |
|--------------------|--------------|---------------------------------------------------------------------------------|
| boolean            | bool         | True, False                                                                     |
| bytes              | bytearray    |                                                                                 |
| double             | float        | 3.145                                                                           |
| float              | float        | 1.23456                                                                         |
| byte               | int          | between -128 and 127                                                            |
| short              | int          | between -65,536 and 65,535                                                      |
| int                | int          | between -2,147,483,648 and 2,147,483,647                                        |
| long               | int          | between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807                |
| string             | str          | "text"                                                                          |
| enum               | str          | enum string literal, eg. "BLUE", "EUROPE"                                       |
| list               | array        | [ "key1", "key2" ]                                                              |
| struct             | dict         | { "key1": "value1", "key2": "value2" }                                          |
| struct with schema | dict         | { "key1": "value1", "key2": "value2", "@type": "SensorData", "@schema": "..." } |
| tuple              | tuple        | (1, "text", 3.14, { "key": "value" })                                           |
| union              | <value type> | Real value is translated as specified in this table                             |

### Automatic conversion

KSML is able to automatically convert between types. Examples are:

- To/from string conversion is handled automatically for almost all data types, including string-notations such as CSV,
  JSON and XML.
- When a string is expected, but a struct is passed in, the struct is automatically converted to string.
- When a struct is expected, but a string is passed in, the string is parsed according to the notation specified.
- Field matching and field type conversion is done automatically. For instance, if a struct contains an integer field,
  but the target schema expects a string, the integer is automatically converted to string.

## Function Types

Functions in KSML always have a `type`. When no type is specified, the function type is inferred from the context, or it
defaults back to `generic`. This section discusses the purpose of every function type, and what fixed arguments every
call gets passed in.

### Aggregator

An `aggregator` incrementally integrates a new keu/value into an aggregatedValue. It is called for every new message
that
becomes part of the aggregated result.

The following highlights which calls are made to which function type during a regular aggregation, in this case for
counting the number of messages:

```
# Aggregation starts
initializer() -> 0
msg1: aggregator(msg1.key, msg1.value, 0) -> 1
msg2: aggregator(msg2.key, msg2.value, 1) -> 1
msg3: aggregator(msg3.key, msg3.value, 2) -> 3
```

The result in this example is 3.

Aggregators get the following fixed arguments:

| Parameter         | Value Type | Description                                                                |
|:------------------|:-----------|:---------------------------------------------------------------------------|
| `key`             | _any_      | The key of the message to be included in the aggregated result thus far.   |
| `value`           | _any_      | The value of the message to be included in the aggregated result thus far. |
| `aggregatedValue` | _any_      | The aggregated value thus far.                                             |
| returns           | _any_      | The new aggregated result, which includes the latest message.              |

### ForEach

A `forEach` function is called for every message in a stream. When part of a `forEach` operation at the end of a
pipeline, the function is the last one called for every message. When this function is called during `peek` operations,
it may look at the messages and cause side effects (e.g. printing the message to stdout), and the pipeline will continue
with the unmodified message after doing so.

ForEach functions get the following fixed arguments:

| Parameter | Value Type | Description               |
|:----------|:-----------|:--------------------------|
| `key`     | _any_      | The key of the message.   |
| `value`   | _any_      | The value of the message. |
| returns   | _none_     | Nothing is returned.      |

### ForeignKeyExtractor

A `foreignKeyExtractor` is a function used during (left) joins of two tables. The function translates a value from "this
table" and translates it into a key of the "other table" that is joined with.

ForEach functions get the following fixed arguments:

| Parameter | Value Type | Description                                 |
|:----------|:-----------|:--------------------------------------------|
| `value`   | _any_      | The value of the message.                   |
| returns   | _any_      | The key looked up in the table joined with. |

### Initializer

An `initializer` is called upon the start of every (part of an) aggregation. It takes no arguments and should return an
initial value for the aggregation.

| Parameter | Value Type | Description                                                                         |
|:----------|:-----------|:------------------------------------------------------------------------------------|
| returns   | _any_      | An initial value for the aggregation. In a counting aggregation, this would be `0`. |

### KeyTransformer

A `keyTransformer` is able to transform a key/value into a new key, which then gets combined with the original value as
a new message on the output stream.

KeyTransformers get the following fixed arguments:

| Parameter | Value Type | Description                    |
|:----------|:-----------|:-------------------------------|
| `key`     | _any_      | The key of the message.        |
| `value`   | _any_      | The value of the message.      |
| returns   | _any_      | The key of the output message. |

### KeyValuePrinter

A `keyValuePrinter` takes a message and converts it to `string` before outputting it to a file or printing it to stdout.

KeyValuePrinters get the following fixed arguments:

| Parameter | Value Type | Description                                 |
|:----------|:-----------|:--------------------------------------------|
| `key`     | _any_      | The key of the message.                     |
| `value`   | _any_      | The value of the message.                   |
| returns   | `string`   | The string to be written to file or stdout. |

### KeyValueToKeyValueListTransformer

A `keyValueToKeyValueListTransformer` takes one message and converts it into a list of output messages, which then get
sent to the output stream. An example for this type of function would be a message, which contains a list of items in
its `value` (e.g. `(k, [item])`. Using a `transformKeyValueToKeyValueList` operation, this message can be converted into
individual messages `(k,item1), (k,item2), ...` on the output stream.

KeyValueToKeyValueListTransformers get the following fixed arguments:

| Parameter | Value Type         | Description                               |
|:----------|:-------------------|:------------------------------------------|
| `key`     | _any_              | The key of the message.                   |
| `value`   | _any_              | The value of the message.                 |
| returns   | `[(_any_, _any_)]` | A list of messages for the output stream. |

### KeyValueToValueListTransformer

A `keyValueToValueListTransformer` takes one message and converts it into a list of output values, which then get
combined with the original key and sent to the output stream. An example for this type of function would be a message,
which contains a list of items in its `value` (e.g. `(k, [item])`. Using a `transformKeyValueToValueList` operation,
this message can be converted into a list of values `[item1, item2, ...]` which get combined with the key of the message
into `(k,item1), (k,item2), ...`on the output stream.

KeyValueToValueListTransformers get the following fixed arguments:

| Parameter | Value Type | Description                                                        |
|:----------|:-----------|:-------------------------------------------------------------------|
| `key`     | _any_      | The key of the message.                                            |
| `value`   | _any_      | The value of the message.                                          |
| returns   | `[_any_]`  | A list of values to be combined with the key on the output stream. |

### KeyValueTransformer

A `keyValueTransformer` takes one message and converts it into another message, which may have different key/value
types.

KeyValueTransformers get the following fixed arguments:

| Parameter | Value Type       | Description               |
|:----------|:-----------------|:--------------------------|
| `key`     | _any_            | The key of the message.   |
| `value`   | _any_            | The value of the message. |
| returns   | `(_any_, _any_)` | The transformed message.  |

### Merger

A `merger` takes a key and two values, and merges those values together into a new value. That value is combined with
the original key and sent to the output stream.

Mergers get the following fixed arguments:

| Parameter | Value Type | Description                             |
|:----------|:-----------|:----------------------------------------|
| `key`     | _any_      | The key of the message.                 |
| `value1`  | _any_      | The value of the first message.         |
| `value2`  | _any_      | The value of the second message.        |
| returns   | _any_      | The merged value of the output message. |

### Predicate

A `predicate` is a function that takes the key/value of a message and returns `True` or `False`. It is used for
filtering and branching purposes (e.g. routing messages based on content).

Predicates get the following fixed arguments:

| Parameter | Value Type | Description               |
|:----------|:-----------|:--------------------------|
| `key`     | _any_      | The key of the message.   |
| `value`   | _any_      | The value of the message. |
| returns   | `boolean`  | `True` or `False`.        |

### Reducer

A `reducer` is a function that combines two aggregated results into one.

Reducers get the following fixed arguments:

| Parameter | Value Type | Description                                   |
|:----------|:-----------|:----------------------------------------------|
| `value1`  | _any_      | The value of the first aggregation result.    |
| `value2`  | _any_      | The value of the second aggregation result.   |
| returns   | _any_      | The value of the combined aggregation result. |

### StreamPartitioner

A `streamPartitioner` is a function that can assign a partition number to every message. It is used to repartition Kafka
topics, based on message contents.

StreamPartitioners get the following fixed arguments:

| Parameter       | Value Type | Description                                             |
|:----------------|:-----------|:--------------------------------------------------------|
| `topic`         | `string`   | The topic of the message.                               |
| `key`           | _any_      | The key of the message.                                 |
| `value`         | _any_      | The value of the message.                               |
| `numPartitions` | `integer`  | The number of partitions available on the output topic. |
| returns         | `integer`  | The partition number to which this message gets sent.   |

### TopicNameExtractor

A `topicNameExtractor` is a function which can derive a topic name from a message, for example by getting the customer
name from a message and deriving the topic name from that. It is used by `toTopicNameExtractor` operations to send
messages to individually determined topics.

TopicNameExtractors get the following fixed arguments:

| Parameter | Value Type | Description                                    |
|:----------|:-----------|:-----------------------------------------------|
| `key`     | _any_      | The key of the message.                        |
| `value`   | _any_      | The value of the message.                      |
| returns   | `string`   | The name of the topic to send this message to. |

### ValueJoiner

A `valueJoiner` takes a key and two values, and combines the two values into one. That value is then combined with the
original key and sent to the output stream.

ValueJoiners get the following fixed arguments:

| Parameter | Value Type | Description                             |
|:----------|:-----------|:----------------------------------------|
| `key`     | _any_      | The key of the message.                 |
| `value1`  | _any_      | The value of the first message.         |
| `value2`  | _any_      | The value of the second message.        |
| returns   | _any_      | The joined value of the output message. |

### ValueTransformer

A `valueTransformer` takes a key/value and transforms it into a new value, which is combined with the original key and
sent to the output stream.

ValueTransformers get the following fixed arguments:

| Parameter | Value Type | Description                      |
|:----------|:-----------|:---------------------------------|
| `key`     | _any_      | The key of the message.          |
| `value`   | _any_      | The value of the message.        |
| returns   | _any_      | The value of the output message. |

## Function parameters

Besides the parameters mentioned above, all Python functions in KSML get special parameters passed in:

### Logger

Every function can access the `log` variable, which is mapped to a plain Java Logger object. It can be used to send
output to the KSML log by calling its methods.
It supports the following operations:

| Method                                 | Description                   |
|----------------------------------------|-------------------------------|
| `error(message: str, value_params...)` | logs an error message         |
| `warn(message: str, value_params...)`  | logs a warning message        |
| `info(message: str, value_params...)`  | logs an informational message |
| `debug(message: str, value_params...)` | logs a debug message          |
| `trace(message: str, value_params...)` | logs a trace message          |

The message contains double curly brackets `{}`, which will be substituted by the value parameters.
Examples are:

```
log.error("Something went completely bad here!")
log.info("Received message from topic: key={}, value={}", key, value)
log.debug("I'm printing five variables here: {}, {}, {}, {}, {}. Lovely isn't it?", 1, 2, 3, "text", {"json":"is cool"})
```

Output of the above statements looks like:

```
[LOG TIMESTAMP] ERROR function.name   Something went completely bad here!
[LOG TIMESTAMP] INFO  function.name   Received message from topic: key=123, value={"key":"value"}
[LOG TIMESTAMP] DEBUG function.name   I'm printing five variables here: 1, 2, 3, text, {"json":"is cool"}. Lovely isn't it?
```

### Metrics

KSML supports metric collection and exposure through JMX and built-in Prometheus agent. Metrics for Python functions are
automatically generated and collected, but users can also specify their own metrics. For an example,
see `17-example-inspect-with-metrics.yaml` in the `examples` directory.

KSML supports the following metric types:

* Counter: an increasing integer, which counts for example the number of calls made to a Python function.
* Meter: used for periodically updating a measurement value. Preferred over Counter when don't care too much about exact
  averages, but want to monitor trends instead.
* Timer: measures the time spent by processes or functions, that get called internally.

Every Python function in KSML can use the `metrics` variable, which is made available by KSML. The object supports the
following methods to create your own metrics:

* counter(name: str, tags: dict) -> Counter
* counter(name: str) -> Counter
* meter(name: str, tags: dict) -> Meter
* meter(name: str) -> Meter
* timer(name: str, tags: dict) -> Timer
* timer(name: str) -> Timer

In turn these objects support the following:

#### Counter

* increment()
* increment(delta: int)

#### Meter

* mark()
* mark(nrOfEvents: int)

#### Timer

* updateSeconds(valueSeconds: int)
* updateMillis(valueMillis: int)
* updateNanos(valueNanos: int)

### State stores

Some functions are allowed to access local state stores. These functions specify the
`stores` attribute in their definitions. The state stores they reference are accessible
as variables with the same name as the state store.

Examples:

```
streams:
  sensor_source_avro:
    topic: ksml_sensordata_avro
    keyType: string
    valueType: avro:SensorData

stores:
  last_sensor_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: false
    historyRetention: 1h
    caching: false
    logging: false

functions:
  process_message:
    type: forEach
    code: |
      last_value = last_sensor_data_store.get(key)
      if last_value is not None:
        log.info("Found last value: {} = {}", key, last_value)
      last_sensor_data_store.put(key, value)
      if value is not None:
        log.info("Stored new value: {} = {}", key, value)
    stores:
      - last_sensor_data_store

pipelines:
  process_message:
    from: sensor_source_avro
    forEach: process_message
```

In this example the function `process_message` uses the state store `last_sensor_data_store`
directly as a variable. It is allowed to do that when it declares such use in its
definition under the `stores` attribute.

State stores have common methods like `get` and `put`, which you can call directly from
Python code.
