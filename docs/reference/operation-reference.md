# Operation Reference

This document provides a comprehensive reference for all operations available in KSML. Each operation is described with its parameters, behavior, and examples.

## Introduction

Operations are the building blocks of stream processing in KSML. They define how data is transformed, filtered, aggregated, and otherwise processed as it flows through your application. Operations form the middle part of pipelines, taking input from the previous operation and producing output for the next operation.

Understanding the different types of operations and when to use them is crucial for building effective stream processing applications.

## Operations Overview

KSML supports 28 operations for stream processing. Each operation serves a specific purpose in transforming, filtering, aggregating, or routing data:

| Operation | Purpose | Common Use Cases |
|-----------|---------|------------------|
| **Stateless Transformation Operations** | | |
| [map](#map) | Transform both key and value | Change message format, enrich data |
| [mapValues](#mapvalues) | Transform only the value (preserves key) | Modify payload without affecting partitioning |
| [mapKey](#mapkey) | Transform only the key | Change partitioning key |
| [flatMap](#flatmap) | Transform one record into multiple records | Split batch messages, expand arrays |
| [selectKey](#selectkey) | Select a new key from the value | Extract key from message content |
| [transformKey](#transformkey) | Transform key using custom function | Complex key transformations |
| [transformValue](#transformvalue) | Transform value using custom function | Complex value transformations |
| | | |
| **Filtering Operations** | | |
| [filter](#filter) | Keep records that match a condition | Remove unwanted messages |
| [filterNot](#filternot) | Remove records that match a condition | Exclude specific messages |
| | | |
| **Format Conversion Operations** | | |
| [convertKey](#convertkey) | Convert key format (e.g., JSON to Avro) | Change serialization format |
| [convertValue](#convertvalue) | Convert value format (e.g., JSON to Avro) | Change serialization format |
| | | |
| **Grouping & Partitioning Operations** | | |
| [groupBy](#groupby) | Group by a new key | Prepare for aggregation with new key |
| [groupByKey](#groupbykey) | Group by existing key | Prepare for aggregation |
| | | |
| **Stateful Aggregation Operations** | | |
| [aggregate](#aggregate) | Build custom aggregations | Complex calculations, custom state |
| [count](#count) | Count records per key | Track occurrences |
| [reduce](#reduce) | Combine records with same key | Accumulate values |
| | | |
| **Join Operations** | | |
| [join](#join) | Inner join two streams | Correlate related events |
| [leftJoin](#leftjoin) | Left outer join two streams | Include all left records |
| [outerJoin](#outerjoin) | Full outer join two streams | Include all records from both sides |
| | | |
| **Windowing Operations** | | |
| [windowByTime](#windowbytime) | Group into fixed time windows | Time-based aggregations |
| [windowBySession](#windowbysession) | Group into session windows | User session analysis |
| | | |
| **Output Operations** | | |
| [to](#to) | Send to a specific topic | Write results to Kafka |
| [toTopicNameExtractor](#totopicnameextractor) | Send to dynamically determined topic | Route to different topics |
| [forEach](#foreach) | Process without producing output | Side effects, external calls |
| [print](#print) | Print to console | Debugging, monitoring |
| | | |
| **Control Flow Operations** | | |
| [branch](#branch) | Split stream into multiple branches | Conditional routing |
| [peek](#peek) | Observe records without modification | Logging, debugging |

## Choosing the Right Operation

When designing your KSML application, consider these factors:

- **State Requirements**: Stateful operations (aggregations, joins) require state stores and more resources
- **Partitioning**: Operations like `groupBy` may trigger data redistribution
- **Performance**: Some operations are more computationally expensive than others
- **Error Handling**: Use `try` operations to handle potential failures gracefully

## Stateless Transformation Operations

Stateless transformation operations modify records (key, value, or both) without maintaining state between records. These operations are the most common building blocks for data processing pipelines.

### `map`

Transforms both the key and value of each record.

#### Parameters

| Parameter | Type   | Required | Description                                  |
|-----------|--------|----------|----------------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform the key and value |

The `mapper` can be defined using:

- `expression`: A simple expression returning a tuple (key, value)
- `code`: A Python code block returning a tuple (key, value)

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)

### `mapValues`

Transforms the value of each record without changing the key.

#### Parameters

| Parameter | Type   | Required | Description                          |
|-----------|--------|----------|--------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform the value |

The `mapper` can be defined using:

- `expression`: A simple expression
- `code`: A Python code block

##### **See it in action**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md)

### `mapKey`

Transforms the key of each record without modifying the value.

#### Parameters

| Parameter | Type   | Required | Description                        |
|-----------|--------|----------|-----------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform the key |

The `mapper` can be defined using:
- `expression`: A simple expression returning the new key
- `code`: A Python code block returning the new key

##### **See it in action**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md)

### `flatMap`

Transforms each record into zero or more records, useful for splitting batch messages into individual records.

#### Parameters

| Parameter | Type   | Required | Description                                                  |
|-----------|--------|----------|--------------------------------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform each record into multiple records |

The `mapper` must specify:

- `resultType`: Format `"[(keyType,valueType)]"` indicating list of tuples
- `code`: Python code returning a list of tuples `[(key, value), ...]`

#### Example

This example splits order batches containing multiple items into individual item records:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/flatmap-producer.yaml"
    %}
    ```

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/flatmap-processor.yaml"
    %}
    ```

**What this example does:** 

- The producer generates order batches containing multiple items.
- The processor uses `flatMap` to split each order batch into individual item records - transforming 1 input record into 3 output records (one per item).
- Each output record has a unique key combining order ID and item ID, with calculated total prices per item.

### `selectKey`

Changes the key of each record without modifying the value. This operation is useful for repartitioning data or preparing streams for joins based on different key attributes.

#### Parameters

| Parameter | Type   | Required | Description                                    |
|-----------|--------|----------|------------------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to extract the new key from the record |

The `mapper` can be defined using:

- `expression`: A simple expression returning the new key
- `code`: A Python code block returning the new key

#### Example

This example demonstrates changing the key from session_id to user_id for better data organization:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/selectkey-producer.yaml"
    %}
    ```

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/selectkey-processor.yaml"
    %}
    ```

**What this example does:**

- The producer generates user events (login, purchase, view, logout, search) with different session IDs as keys
- The processor uses `selectKey` to change the key from `session_id` to `user_id`, enabling better data partitioning for user-centric analytics
- This rekeying allows subsequent operations to group and aggregate data by user rather than by session

### `transformKey`

Transforms the key using a custom transformer function.

#### Parameters

| Parameter | Type   | Required | Description                        |
|-----------|--------|----------|-----------------------------------|
| `mapper`  | String | Yes      | Name of the key transformer function |

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)

### `transformValue`

Transforms the value using a custom transformer function.

#### Parameters

| Parameter | Type   | Required | Description                          |
|-----------|--------|----------|--------------------------------------|
| `mapper`  | String | Yes      | Name of the value transformer function |

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)

## Filtering Operations

Filtering operations selectively pass or remove records based on conditions, allowing you to control which data continues through your processing pipeline.

### `filter`

Keeps only records that satisfy a condition.

#### Parameters

| Parameter | Type   | Required | Description             |
|-----------|--------|----------|-------------------------|
| `if`      | Object | Yes      | Specifies the condition |

The `if` can be defined using:

- `expression`: A simple boolean expression
- `code`: A Python code block returning a boolean

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)

### `filterNot`

Excludes records that satisfy a condition (opposite of filter). Records are kept when the condition returns false.

#### Parameters

| Parameter | Type   | Required | Description             |
|-----------|--------|----------|-------------------------|
| `if`      | Object | Yes      | Specifies the condition |

The `if` parameter must reference a predicate function that returns a boolean.

#### Example

This example filters out products with "inactive" status, keeping all other products:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/filternot-producer.yaml"
    %}
    ```

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/filternot-processor.yaml"
    %}
    ```

**What this example does:**

- The producer generates products with different statuses: active, inactive, pending, discontinued
- The processor uses `filterNot` with a predicate function to exclude products with "inactive" status
- Products with other statuses (active, pending, discontinued) are kept and passed through to the output topic

## Format Conversion Operations

Format conversion operations change the serialization format of keys or values without altering the actual data content.

### `convertKey`

Converts the key to a different data format.

#### Parameters

| Parameter | Type   | Required | Description              |
|-----------|--------|----------|--------------------------|
| `into`    | String | Yes      | Target format for the key |

##### **See it in action**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md)

### `convertValue`

Converts the value to a different data format.

#### Parameters

| Parameter | Type   | Required | Description                |
|-----------|--------|----------|----------------------------|
| `into`    | String | Yes      | Target format for the value |

##### **See it in action**:

- [Tutorial: Data Formats](../tutorials/beginner/data-formats.md)

## Grouping & Partitioning Operations

Grouping and partitioning operations organize data by keys and control how records are distributed across partitions, preparing data for aggregation or improving processing parallelism.

### `groupBy`

Groups records by a new key derived from the record.

#### Parameters

| Parameter     | Type   | Required | Description                         |
|---------------|--------|----------|-------------------------------------|
| `keySelector` | Object | Yes      | Specifies how to select the new key |

The `keySelector` can be defined using:
- `expression`: A simple expression returning the grouping key
- `code`: A Python code block returning the grouping key

##### **See it in action**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md)

### `groupByKey`

Groups records by their existing key for subsequent aggregation operations.

#### Parameters

None. This operation is typically followed by an aggregation operation.

##### **See it in action**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md)


## Stateful Aggregation Operations

Stateful aggregation operations maintain state between records to perform calculations like counting, summing, or building custom aggregates based on record keys.

### `aggregate`

Aggregates records by key using a custom aggregation function.

#### Parameters

| Parameter     | Type   | Required | Description                                                    |
|---------------|--------|----------|----------------------------------------------------------------|
| `initializer` | Object | Yes      | Specifies the initial value for the aggregation                |
| `aggregator`  | Object | Yes      | Specifies how to combine the current record with the aggregate |

Both `initializer` and `aggregator` can be defined using:

- `expression`: A simple expression
- `code`: A Python code block

### `count`

Counts the number of records for each key.

#### Parameters

None.

##### **See it in action**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md)

### `reduce`

Combines records with the same key using a reducer function.

#### Parameters

| Parameter | Type   | Required | Description                         |
|-----------|--------|----------|-------------------------------------|
| `reducer` | Object | Yes      | Specifies how to combine two values |

The `reducer` can be defined using:

- `expression`: A simple expression
- `code`: A Python code block

##### **See it in action**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md)

## Join Operations

Join operations combine data from multiple streams or tables based on matching keys, enabling you to correlate related events from different data sources.

### `join`

Performs an inner join between two streams.

#### Parameters

| Parameter        | Type     | Required | Description                                                           |
|------------------|----------|----------|-----------------------------------------------------------------------|
| `stream`         | String   | Yes      | The name of the stream to join with                                   |
| `table`          | String   | Yes      | The name of the table to join with (for stream-table joins)          |
| `valueJoiner`    | Object   | Yes      | Function that defines how to combine values from both sides          |
| `timeDifference` | Duration | No       | The time difference for the join window (for stream-stream joins)    |
| `grace`          | Duration | No       | Grace period for late-arriving data (for stream-stream joins)        |
| `foreignKeyExtractor` | Object | No  | Function to extract foreign key (for stream-table joins)             |

##### **See it in action**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md)

### `leftJoin`

Performs a left join between two streams.

#### Parameters

| Parameter        | Type     | Required | Description                                                           |
|------------------|----------|----------|-----------------------------------------------------------------------|
| `stream`         | String   | Yes      | The name of the stream to join with                                   |
| `table`          | String   | Yes      | The name of the table to join with (for stream-table joins)          |
| `valueJoiner`    | Object   | Yes      | Function that defines how to combine values from both sides          |
| `timeDifference` | Duration | No       | The time difference for the join window (for stream-stream joins)    |
| `grace`          | Duration | No       | Grace period for late-arriving data (for stream-stream joins)        |
| `foreignKeyExtractor` | Object | No  | Function to extract foreign key (for stream-table joins)             |

##### **See it in action**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md)

### `outerJoin`

Performs an outer join between two streams.

#### Parameters

| Parameter        | Type     | Required | Description                                                           |
|------------------|----------|----------|-----------------------------------------------------------------------|
| `stream`         | String   | Yes      | The name of the stream to join with                                   |
| `table`          | String   | Yes      | The name of the table to join with (for stream-table joins)          |
| `valueJoiner`    | Object   | Yes      | Function that defines how to combine values from both sides          |
| `timeDifference` | Duration | No       | The time difference for the join window (for stream-stream joins)    |
| `grace`          | Duration | No       | Grace period for late-arriving data (for stream-stream joins)        |
| `foreignKeyExtractor` | Object | No  | Function to extract foreign key (for stream-table joins)             |

##### **See it in action**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md)

## Windowing Operations

Windowing operations group records into time-based windows, enabling temporal aggregations and time-bounded processing.

### `windowByTime`

Groups records into time windows.

#### Parameters

| Parameter        | Type     | Required | Description                                                          |
|------------------|----------|----------|----------------------------------------------------------------------|
| `windowType`     | String   | No       | The type of window (`tumbling`, `hopping`, or `sliding`)             |
| `timeDifference` | Duration | Yes      | The duration of the window                                           |
| `advanceBy`      | Long     | No       | Only required for `hopping` windows, how often to advance the window |
| `grace`          | Long     | No       | Grace period for late-arriving data                                  |

##### **See it in action**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md)

### `windowBySession`

Groups records into session windows, where events with timestamps within `inactivityGap` durations are seen as belonging
to the same session.

#### Parameters

| Parameter       | Type     | Required | Description                                                                                  |
|-----------------|----------|----------|----------------------------------------------------------------------------------------------|
| `inactivityGap` | Duration | Yes      | The maximum duration between events before they are seen as belonging to a different session |
| `grace`         | Long     | No       | Grace period for late-arriving data                                                          |

##### **See it in action**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md)

## Output Operations

Output operations represent the end of a processing pipeline, sending records to topics or performing terminal actions like logging.

### `to`

Sends records to a specific Kafka topic.

#### Parameters

| Parameter | Type   | Required | Description                       |
|-----------|--------|----------|-----------------------------------|
| `topic`   | String | Yes      | The name of the target topic      |
| `keyType` | String | No       | The data type of the key          |
| `valueType` | String | No     | The data type of the value        |

##### **See it in action**:

- [Tutorial: Data Formats](../tutorials/beginner/data-formats.md)

### `toTopicNameExtractor`

Sends records to topics determined dynamically based on the record content. This operation enables content-based routing, allowing you to distribute messages to different topics based on their attributes, priorities, or business logic.

#### Parameters

| Parameter              | Type   | Required | Description                                           |
|------------------------|--------|----------|-------------------------------------------------------|
| `topicNameExtractor`   | String | Yes      | Name of the function that determines the topic name   |

#### Example

This example demonstrates routing system events to different topics based on severity level:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/topicnameextractor-producer.yaml"
    %}
    ```

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/topicnameextractor-processor.yaml"
    %}
    ```

**What this example does:**

- The producer generates system events with different severity levels (INFO, WARNING, ERROR, CRITICAL) from various system components
- The processor uses `toTopicNameExtractor` with a custom function to route events to different topics based on severity
- Critical events are logged and sent to `critical_alerts` topic, while other severities go to their respective log topics
- This pattern enables priority-based processing and separate handling of critical system issues

### `forEach`

Processes each record with a side effect, typically used for logging or external actions. This is a terminal operation that does not forward records.

#### Parameters

| Parameter | Type   | Required | Description                                    |
|-----------|--------|----------|------------------------------------------------|
| `forEach` | Object | Yes      | Specifies the action to perform on each record |

The `forEach` can be defined using:
- `code`: A Python code block performing the side effect

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)

### `print`

Prints each record to stdout for debugging purposes. This operation can use a custom mapper function to format the output, providing colored indicators and structured logging.

#### Parameters

| Parameter | Type   | Required | Description                                  |
|-----------|--------|----------|----------------------------------------------|
| `mapper`  | String | No       | Name of keyValuePrinter function to format output |
| `prefix`  | String | No       | Optional prefix for the printed output       |

#### Example

This example demonstrates printing debug messages with color-coded log levels and custom formatting:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/print-producer.yaml"
    %}
    ```

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/print-processor.yaml"
    %}
    ```

**What this example does:**

- The producer generates different types of debug messages (INFO, WARNING, ERROR, DEBUG) from various system components
- The processor uses `print` with a custom `keyValuePrinter` function to format each message with color indicators ("red" for ERROR, "yellow" for WARNING, "green" for INFO, "blue" for DEBUG)
- The formatted output includes component name, message text, request ID, and thread information for comprehensive debugging

## Control Flow Operations

Control flow operations manage the flow of data through your processing pipeline, allowing for branching logic and record observation.

### `branch`

Splits a stream into multiple substreams based on conditions.

#### Parameters

| Parameter  | Type  | Required | Description                                              |
|------------|-------|----------|----------------------------------------------------------|
| `branches` | Array | Yes      | List of conditions and handling pipeline for each branch |

The tag `branches` does not exist in the KSML language, but is meant to represent a composite object here that consists of two elements:


| Parameter  | Type      | Required | Description                                                                                                |
|------------|-----------|----------|------------------------------------------------------------------------------------------------------------|
| `if`       | Predicate | Yes      | A condition which can evaluate to True or False. When True, the message is sent down the branch's pipeline |
| `pipeline` | Pipeline  | Yes      | A pipeline that contains a list of processing steps to send the message through                            |

##### **See it in action**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md)

### `peek`

Performs a side effect on each record without changing it.

#### Parameters

| Parameter | Type   | Required | Description                                    |
|-----------|--------|----------|------------------------------------------------|
| `forEach` | Object | Yes      | Specifies the action to perform on each record |

The `forEach` can be defined using:

- `expression`: A simple expression (rarely used for peek)
- `code`: A Python code block performing the side effect

##### **See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)

## Combining Operations

Operations can be combined in various ways to create complex processing pipelines.

### Sequential Operations

Operations are executed in sequence, with each operation processing the output of the previous operation.

```yaml
pipelines:
  my_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: value.get("amount") > 0
      - type: transformValue
        mapper:
          code: enrich_transaction(value)
      - type: peek
        forEach:
          code: |
            log.info("Processed transaction: {}", value)
    to: output_stream
```

### Branching and Merging

You can create complex topologies by branching streams and merging them back together.

```yaml
pipelines:
  branch_pipeline:
    from: input_stream
    branch:
      - if:
          expression: value.get("type") == "A"
        as: type_a_stream
      - if:
          expression: value.get("type") == "B"
        as: type_b_stream

  process_a_pipeline:
    from: type_a_stream
    via:
      - type: mapValues
        mapper:
          code: process_type_a(value)
    to: merged_stream

  process_b_pipeline:
    from: type_b_stream
    via:
      - type: mapValues
        mapper:
          code: process_type_b(value)
    to: merged_stream
```

## Best Practices

- **Chain operations thoughtfully**: Consider the performance implications of chaining multiple operations.
- **Use stateless operations when possible**: Stateless operations are generally more efficient than stateful ones.
- **Be careful with window sizes**: Large windows can consume significant memory.
- **Handle errors gracefully**: Use error handling operations to prevent pipeline failures.
- **Monitor performance**: Keep an eye on throughput and latency, especially for stateful operations.
