[<< Back to index](index.md)

# Pipeline

### Table of Contents
1. [Introduction](#introduction)
2. [Definition](#definition)
3. [Elements](#elements)
    * [Source](#source)
    * [Operations](#operations)
    * [Sink](#sink)

## Introduction

Pipelines form the heart of KSML streams logic. They take one or more input streams and apply processing logic to them. Output is passed on from operation to operation.

## Definition

Pipelines are contained in the `pipelines` section. Each pipeline has a name, which is the name of the YAML tag.

As an example, the following defines a pipeline called `copy_pipeline`, which consumes messages from `some_input_stream` and outputs the same messages to `some_output_stream`.

```yaml
pipelines:
  copy_pipeline:
    from: some_input_stream
    to: some_output_stream
```

## Elements

All pipelines contain three elements:

### Source

The source of a pipeline is marked with the `from` keyword. The value of the YAML node is the name of a defined `Stream`, `Table` or `GlobalTable`.
See [Streams](streams.md) for more information.

### Operations

Once a source was selected, a list of operations can be applied to the input. The list is started through the keyword `via`, below which a list of operations is defined.

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

### Sink

After all operations are applied, a pipeline can define a sink to which all messages are sent. There are four sink types in KSML:

* Branch
* ForEach
* To
* ToTopicNameExtractor
