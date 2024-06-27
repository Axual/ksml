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

| Type                                | Returns            | Parameter         | Value Type | Description                               |
|:------------------------------------|:-------------------|:------------------|:-----------|:------------------------------------------|
| `aggregator`                        | _any_              | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
|                                     |                    | `aggregatedValue` | _any_      | The aggregated value thus far.            |
| `forEach`                           | _none_             | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `foreignKeyExtractor`               | _any_              | `value`           | _any_      | The value to extract the foreign key from |
| `initializer`                       | _any_              | _none_            |            |                                           |
| `keyTransformer`                    | _any_              | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `keyValuePrinter`                   | `string`           | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `keyValueToKeyValueListTransformer` | [ (_any_, _any_) ] | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `keyValueToValueListTransformer`    | [ _any_ ]          | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `keyValueTransformer`               | (_any_, _any_)     | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `merger`                            | _any_              | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value1`          | _any_      | The first value to be merged              |
|                                     |                    | `value2`          | _any_      | The second value to be merged             |
| `predicate`                         | `boolean`          | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `reducer`                           | _any_              | `value1`          | _any_      | The first value to be reduced             |
|                                     |                    | `value2`          | _any_      | The second value to be reduced            |
| `streamPartitioner`                 | `int`              | `topic`           | `String`   | The topic of the message                  |
|                                     |                    | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
|                                     |                    | `numPartitions`   | `int`      | The number of partitions on the topic     |
| `topicNameExtractor`                | `string`           | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |
| `valueJoiner`                       | _any_              | `key`             | _any_      | The key of both messages                  |
|                                     |                    | `value1`          | _any_      | The first value to join                   |
|                                     |                    | `value2`          | _any_      | The second value to join                  |
| `valueTransformer`                  | _any_              | `key`             | _any_      | The key of the message                    |
|                                     |                    | `value`           | _any_      | The value of the message                  |

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
