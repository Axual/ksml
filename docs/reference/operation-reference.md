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

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml:67:68"
```

**Full example for `map`**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#applying-multiple-transformations)

### `mapValues`

Transforms the value of each record without changing the key.

#### Parameters

| Parameter | Type   | Required | Description                          |
|-----------|--------|----------|--------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform the value |

The `mapper` can be defined using:

- `expression`: A simple expression
- `code`: A Python code block

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/branching/processor-order-processing.yaml:94:95"
```

**Full example for `mapValues`**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md#example-2-multi-condition-data-processing-pipeline)

### `mapKey`

Transforms the key of each record without modifying the value.

#### Parameters

| Parameter | Type   | Required | Description                        |
|-----------|--------|----------|-----------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform the key |

The `mapper` can be defined using:
- `expression`: A simple expression returning the new key
- `code`: A Python code block returning the new key

#### Example

```yaml
--8<-- "definitions/reference/functions/keytransformer-processor.yaml:29:30"
```

**Full example for `mapKey`**:

- [Function Reference: keyTransformer](function-reference.md#keytransformer) - Shows mapKey used with keyTransformer function

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

```yaml
--8<-- "definitions/reference/operations/flatmap-processor.yaml:11:21"
```

This example splits order batches containing multiple items into individual item records:

??? info "Producer - `flatMap` example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/flatmap-producer.yaml"
    %}
    ```

??? info "Processor - `flatMap` example (click to expand)"

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

Changes the key of each record without modifying the value. This operation extracts a new key from the existing key and/or value, enabling data repartitioning and preparation for joins based on different key attributes.

#### Parameters

| Parameter | Type   | Required | Description                                    |
|-----------|--------|----------|------------------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to derive the new key from the key/value |

The `mapper` can be defined using:

- `expression`: A simple expression returning the new key (can use both `key` and `value`)
- `code`: A Python code block returning the new key (can use both `key` and `value`)

#### Example

```yaml
--8<-- "definitions/reference/operations/selectkey-processor.yaml:16:18"
```

This example demonstrates changing the key from session_id to user_id for better data organization:

??? info "Producer - `selectKey` example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/selectkey-producer.yaml"
    %}
    ```

??? info "Processor - `selectKey` example (click to expand)"

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/joins/processor-stream-table-join.yaml:58:59"
```

**Full example for `transformKey`**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md#use-case-order-enrichment)

### `transformValue`

Transforms the value using a custom transformer function.

#### Parameters

| Parameter | Type   | Required | Description                          |
|-----------|--------|----------|--------------------------------------|
| `mapper`  | String | Yes      | Name of the value transformer function |

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml:71:72"
```

**Full example for `transformValue`**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#applying-multiple-transformations)

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

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-custom-filter.yaml:31:32"
```

**Full example for `filter`**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)

### `filterNot`

Excludes records that satisfy a condition (opposite of filter). Records are kept when the condition returns false.

#### Parameters

| Parameter | Type   | Required | Description             |
|-----------|--------|----------|-------------------------|
| `if`      | Object | Yes      | Specifies the condition |

The `if` parameter must reference a predicate function that returns a boolean.

#### Example

```yaml
--8<-- "definitions/reference/operations/filternot-processor.yaml:25:26"
```

This example filters out products with "inactive" status, keeping all other products:

??? info "Producer - `filterNot` example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/filternot-producer.yaml"
    %}
    ```

??? info "Processor - `filterNot` example (click to expand)"

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml:31:32"
```

**Full example for `convertKey`**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md#tumbling-window-click-counting)

### `convertValue`

Converts the value to a different data format.

#### Parameters

| Parameter | Type   | Required | Description                |
|-----------|--------|----------|----------------------------|
| `into`    | String | Yes      | Target format for the value |

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/different-data-formats/processor-converting.yaml:28:29"
```

**Full example for `convertValue`**:

- [Tutorial: Data Formats](../tutorials/beginner/data-formats.md#working-with-avro-data)

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/state-stores/processor-inline-store.yaml:48:57"
```

**Full example for `groupBy`**:

- [Tutorial: State Stores](../tutorials/intermediate/state-stores.md#2-inline-store-configuration)

### `groupByKey`

Groups records by their existing key for subsequent aggregation operations.

#### Parameters

None. This operation is typically followed by an aggregation operation.

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/aggregations/processor-simple.yaml:22:22"
```

**Full example for `groupByKey`**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md#count-example)


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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/aggregations/processor-sales-analytics.yaml:91:97"
```

**Full example for `aggregate`**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md#complex-example-regional-sales-analytics)

### `count`

Counts the number of records for each key.

#### Parameters

None.

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/aggregations/processor-count.yaml:17:17"
```

**Full example for `count`**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md#count-example)

### `reduce`

Combines records with the same key using a reducer function.

#### Parameters

| Parameter | Type   | Required | Description                         |
|-----------|--------|----------|-------------------------------------|
| `reducer` | Object | Yes      | Specifies how to combine two values |

The `reducer` can be defined using:

- `expression`: A simple expression
- `code`: A Python code block

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/aggregations/processor-reduce.yaml:62:63"
```

**Full example for `reduce`**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md#simple-reduce-binary-format)

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/joins/processor-stream-stream-join-working.yaml:48:64"
```

**Full example for `join`**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md#use-case-user-behavior-analysis)

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/joins/processor-stream-table-left-join.yaml:59:61"
```

**Full example for `leftJoin`**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md#use-case-activity-enrichment-with-location)

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/joins/processor-stream-stream-outer-join.yaml:74:90"
```

**Full example for `outerJoin`**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md#stream-stream-outer-join)

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml:19:22"
```

**Full example for `windowByTime`**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md#tumbling-window-click-counting)

### `windowBySession`

Groups records into session windows, where events with timestamps within `inactivityGap` durations are seen as belonging
to the same session.

#### Parameters

| Parameter       | Type     | Required | Description                                                                                  |
|-----------------|----------|----------|----------------------------------------------------------------------------------------------|
| `inactivityGap` | Duration | Yes      | The maximum duration between events before they are seen as belonging to a different session |
| `grace`         | Long     | No       | Grace period for late-arriving data                                                          |

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/windowing/processor-session-activity.yaml:19:21"
```

**Full example for `windowBySession`**:

- [Tutorial: Windowing](../tutorials/intermediate/windowing.md#session-window-user-activity-analysis)

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

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml:77:77"
```

**Full example for `to`**:

- [Full example for `to`](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)

### `toTopicNameExtractor`

Sends records to topics determined dynamically based on the record content. This operation enables content-based routing, allowing you to distribute messages to different topics based on their attributes, priorities, or business logic.

#### Parameters

| Parameter              | Type   | Required | Description                                           |
|------------------------|--------|----------|-------------------------------------------------------|
| `topicNameExtractor`   | String | Yes      | Name of the function that determines the topic name   |

#### Example

```yaml
--8<-- "definitions/reference/operations/topicnameextractor-processor.yaml:42:43"
```

This example demonstrates routing system events to different topics based on severity level:

??? info "Producer - `toTopicNameExtractor` example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/topicnameextractor-producer.yaml"
    %}
    ```

??? info "Processor - `toTopicNameExtractor` example (click to expand)"

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

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-complex-filtering-multiple-filters.yaml:27:29"
```

**Full example for `forEach`**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#using-multiple-filters)

### `print`

Prints each record to stdout for debugging purposes. This operation can use a custom mapper function to format the output, providing colored indicators and structured logging.

#### Parameters

| Parameter | Type   | Required | Description                                  |
|-----------|--------|----------|----------------------------------------------|
| `mapper`  | String | No       | Name of keyValuePrinter function to format output |
| `prefix`  | String | No       | Optional prefix for the printed output       |

#### Example

```yaml
--8<-- "definitions/reference/operations/print-processor.yaml:20:22"
```

This example demonstrates printing debug messages with color-coded log levels and custom formatting:

??? info "Producer - `print` example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/operations/print-producer.yaml"
    %}
    ```

??? info "Processor - `print` example (click to expand)"

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

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/branching/processor-order-processing.yaml:90:"
```

**Full example for `branch`**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md#example-2-multi-condition-data-processing-pipeline)

### `peek`

Performs a side effect on each record without changing it.

#### Parameters

| Parameter | Type   | Required | Description                                    |
|-----------|--------|----------|------------------------------------------------|
| `forEach` | Object | Yes      | Specifies the action to perform on each record |

The `forEach` can be defined using:

- `expression`: A simple expression (rarely used for peek)
- `code`: A Python code block performing the side effect

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/branching/processor-order-processing.yaml:96:100"
```

**Full example for `peek`**:

- [Tutorial: Branching](../tutorials/intermediate/branching.md#example-2-multi-condition-data-processing-pipeline)

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
