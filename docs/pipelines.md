# Pipeline

### Table of Contents

1. [Introduction](#introduction)
2. [Definition](#definition)
3. [Elements](#elements)
    * [Source](#source)
    * [Operations](#operations)
    * [Sink](#sinks)

## Introduction

Pipelines form the heart of KSML streams logic. They take one or more input streams and apply processing logic to them.
Output is passed on from operation to operation.

## Definition

Pipelines are contained in the `pipelines` section. Each pipeline has a name, which is the name of the YAML tag.

As an example, the following defines a pipeline called `copy_pipeline`, which consumes messages from `some_input_stream`
and outputs the same messages to `some_output_stream`.

```yaml
pipelines:
  copy_pipeline:
    from: some_input_stream
    to: some_output_stream
```

## Elements

All pipelines contain three elements:

### Source

The source of a pipeline is marked with the `from` keyword. The value of the YAML node is the name of a
defined `Stream`, `Table` or `GlobalTable`.
See [Streams](streams.md) for more information.

### Operations

Once a source was selected, a list of operations can be applied to the input. The list is started through the
keyword `via`, below which a list of operations is defined.

Example:

```yaml
pipelines:
  copy_pipeline:
    from: some_input_stream
    via:
      - type: peek
        forEach: my_peek_function
      - type: transformKeyValue
        mapper: my_transform_function
    to: some_output_stream
```

Each operation must have a `type` field, which indicates the type of operations applied to the input.
See [Operations](operations.md) for a full list of operations that can be applied.

### Sinks

After all transformation operations are applied, a pipeline can define a sink to which all messages are sent. There are
four sink types in KSML:

| Sink type | Description                                                                                                                                                                                                          |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `as`      | Allows the pipeline result to be saved under an internal name, which can later be referenced. Pipelines defined after this point may refer to this name in their `from` statement.                                   |
| `branch`  | This statement allows the pipeline to be split up in several branches. Each branch filters messages with an `if` statement. Messages will be processed only by the first branch of which the `if` statement is true. |
| `forEach` | Sends every message to a function, without expecting any return type. Because there is no return type, the pipeline always stops after this statement.                                                               |
| `print`   | Prints out every message according to a given output specification.                                                                                                                                                  |
| `to`      | Sends all output messages to a specific target. This target can be a pre-defined `stream`, `table` or `globalTable`, an inline-defined topic, or a special function called a `topicNameExtractor`.                   |

For more information, see the respective documentation on pipeline definitions in
the [definitions section of the KSML language spec](ksml-language-spec.html#definitions/PipelineDefinition).

## Duration

Some pipeline operations require specifying durations. Durations can be expressed
as strings with the following syntax:

```
###x
```

where `#` is a positive number between 0 and 999999 and `x` is an optional letter from the following table:

| Letter | Description              |
|--------|--------------------------|
| _none_ | Duration in milliseconds |
| s      | Duration in seconds      |
| m      | Duration in minutes      |
| h      | Duration in hours        |
| d      | Duration in days         |
| w      | Duration in weeks        |

Examples:

```
100 ==> hundred milliseconds
30s ==> thirty seconds
8h ==> eight hours
2w ==> two weeks
```

Note that durations are _not_ a data type that can used as key or value on a Kafka
topic.
