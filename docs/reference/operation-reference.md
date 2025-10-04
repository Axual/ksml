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
| [flatMap](#flatmap) | Transform one record into multiple records | Split batch messages, expand arrays |
| [map](#map) | Transform both key and value | Change message format, enrich data |
| [mapKey](#mapkey) | Transform only the key | Change partitioning key |
| [mapValues](#mapvalues) | Transform only the value (preserves key) | Modify payload without affecting partitioning |
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
| [repartition](#repartition) | Redistribute records across partitions | Custom partitioning logic |
| | | |
| **Stateful Aggregation Operations** | | |
| [aggregate](#aggregate) | Build custom aggregations | Complex calculations, custom state |
| [count](#count) | Count records per key | Track occurrences |
| [reduce](#reduce) | Combine records with same key | Accumulate values |
| | | |
| **Join Operations** | | |
| [join](#join) | Inner join two streams | Correlate related events |
| [leftJoin](#leftjoin) | Left outer join two streams | Include all left records |
| [merge](#merge) | Combine multiple streams into one | Stream unification |
| [outerJoin](#outerjoin) | Full outer join two streams | Include all records from both sides |
| | | |
| **Windowing Operations** | | |
| [windowBySession](#windowbysession) | Group into session windows | User session analysis |
| [windowByTime](#windowbytime) | Group into fixed time windows | Time-based aggregations |
| | | |
| **Output Operations** | | |
| [forEach](#foreach) | Process without producing output | Side effects, external calls |
| [print](#print) | Print to console | Debugging, monitoring |
| [to](#to) | Send to a specific topic | Write results to Kafka |
| [toTopicNameExtractor](#totopicnameextractor) | Send to dynamically determined topic | Route to different topics |
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
--8<-- "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml:94:95"
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
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-inline-store.yaml:48:57"
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

### `repartition`

Redistributes records across partitions, optionally using custom partitioning logic. This operation allows you to control data distribution for performance optimization or to meet specific processing requirements.

#### Parameters

| Parameter | Type   | Required | Description                       |
|-----------|--------|----------|-----------------------------------|
| `numberOfPartitions` | Integer | No | Number of partitions for redistribution |
| `partitioner` | String | No    | Function name for custom partitioning logic |

#### Example

> **Note:**
> 
> To test this repartition example, ensure your topics have sufficient partitions. The example requires **minimum 4 partitions** since it redistributes to 4 partitions (0-3). Update your docker-compose.yml:
> 
> ```bash
> kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 4 --replication-factor 1 --topic user_activities
> kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 4 --replication-factor 1 --topic repartitioned_activities
> ```

```yaml
--8<-- "docs-examples/reference/operations/repartition-example-processor.yaml:73:76"
```

??? info "Producer - `repartition` example (click to expand)"

    ```yaml
    {%
      include "../../ksml/src/test/resources/docs-examples/reference/operations/repartition-example-producer.yaml"
    %}
    ```

??? info "Processor - `repartition` example (click to expand)"

    ```yaml
    {%
      include "../../ksml/src/test/resources/docs-examples/reference/operations/repartition-example-processor.yaml"
    %}
    ```

The repartition operation demonstrates data redistribution by changing keys from regions to user IDs, then using custom partitioning logic to distribute activities based on user patterns. This ensures related user activities are processed together while optimizing partition utilization.

**What the example does:**

Demonstrates intelligent data redistribution for user-centric processing:

* Changes partitioning strategy from region-based to user-based keys
* Applies custom partitioning logic based on user ID patterns
* Routes even user numbers (002, 004) to partitions 0-1
* Routes odd user numbers (001, 003, 005) to partitions 2-3
* Producer generates activities initially keyed by region for realistic repartitioning scenario

**Key Features:**

* Dynamic key transformation from region to user_id
* Custom partition calculation based on user patterns
* Guaranteed co-location of activities for the same user
* Processing metadata tracking for observability
* Explicit partition count handling (4 partitions total)
* Fallback to partition 0 for edge cases

**Expected Results:**

When running this example, you'll see log messages like:

- `"Generated activity: activity_0001 for user user_001 in region south - type: purchase"` - Producer creating activities
- `"Changing key from region 'south' to user_id 'user_001'"` - Key transformation
- `"Repartitioned activity user_001: Repartitioned by user: user_001 -> user-based partitioning applied"` - Successful repartitioning
- User_001 and user_003 (odd numbers) go to partitions 2-3
- User_002 and user_004 (even numbers) go to partitions 0-1
- Activities for the same user are guaranteed to be processed in order


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
| `partitioner`    | String   | No       | Function name for custom partitioning of current stream             |
| `otherPartitioner` | String | No       | Function name for custom partitioning of join stream/table          |

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
| `partitioner`    | String   | No       | Function name for custom partitioning of current stream             |
| `otherPartitioner` | String | No       | Function name for custom partitioning of join stream/table          |

#### Example

```yaml
--8<-- "definitions/intermediate-tutorial/joins/processor-stream-table-left-join.yaml:59:61"
```

**Full example for `leftJoin`**:

- [Tutorial: Joins](../tutorials/intermediate/joins.md#use-case-activity-enrichment-with-location)

### `merge`

Merges multiple streams with identical key and value types into a single unified stream. The merge operation combines streams without any joining logic - it simply forwards all records from all input streams to the output stream in the order they arrive.

#### Parameters

| Parameter | Type   | Required | Description                                          |
|-----------|--------|----------|------------------------------------------------------|
| `stream`  | String | Yes      | The name of the stream to merge with the main stream |

#### Example

```yaml
--8<-- "definitions/reference/operations/merge-example-processor.yaml:36:37"
```

??? info "Producer - `merge` example (click to expand)"

    ```yaml
    {% include "../definitions/reference/operations/merge-example-producer.yaml" %}
    ```

??? info "Processor - `merge` example (click to expand)"

    ```yaml
    {% include "../definitions/reference/operations/merge-example-processor.yaml" %}
    ```

**What This Example Does:**

The example demonstrates merging two independent streams (`stream_a` and `stream_b`) into a single processing pipeline. Both producers generate messages with a color key and JSON values containing an id, color, and source field. The merge operation combines both streams so that messages from either stream flow through the same downstream processing.

**How the Merge Operation Works:**

- **Stream Union**: The merge operation creates a simple union of multiple streams - all records from all input streams are forwarded to the output
- **No Transformation**: Records pass through unchanged, maintaining their original keys and values
- **Interleaved Processing**: Messages from different streams are processed as they arrive, interleaved based on timing
- **Shared Pipeline**: After merging, both streams share the same downstream operations (in this example, the peek operation logs all messages)

**Important Notes:**

- All streams being merged must have identical key and value types
- Records maintain their original timestamps and ordering per stream
- No complex joining logic - this is a simple stream union operation
- Can merge any number of streams by chaining multiple merge operations

**Expected Results:**

When running this example, you'll see interleaved log messages like:

- `"Generated message from stream A: stream_a_1 - color: green"` - Producer A creating messages every 3 seconds
- `"Generated message from stream B: stream_b_2 - color: blue"` - Producer B creating messages every 2 seconds  
- `"Merged message: stream_a_1 from stream_a - color: green"` - Merged pipeline processing stream A messages
- `"Merged message: stream_b_2 from stream_b - color: blue"` - Same pipeline processing stream B messages

Both streams flow through the unified pipeline after merging, demonstrating how merge combines multiple data sources for shared processing.

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
| `partitioner`    | String   | No       | Function name for custom partitioning of current stream             |
| `otherPartitioner` | String | No       | Function name for custom partitioning of join stream/table          |

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
--8<-- "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml:19:21"
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
| `partitioner` | String | No    | Function name for custom partitioning logic |

#### Example

```yaml
--8<-- "definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml:77:77"
```

#### Example with Custom Partitioner

```yaml
--8<-- "definitions/reference/functions/streampartitioner-example-processor.yaml:85:89"
```

**Full example for `to`**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)

**Full example for `to` with partitioner**:

- [streamPartitioner Function Reference](function-reference.md#streampartitioner)

### `toTopicNameExtractor`

Sends records to topics determined dynamically based on the record content. This operation enables content-based routing, allowing you to distribute messages to different topics based on their attributes, priorities, or business logic.

#### Parameters

| Parameter              | Type   | Required | Description                                           |
|------------------------|--------|----------|-------------------------------------------------------|
| `topicNameExtractor`   | String | Yes      | Name of the function that determines the topic name   |
| `partitioner`          | String | No       | Function name for custom partitioning logic          |

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
--8<-- "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml:90:"
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
--8<-- "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml:96:100"
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

## How KSML Operations Relate to Kafka Streams

KSML operations are YAML-based wrappers around Kafka Streams topology operations. Understanding this relationship helps you leverage Kafka Streams documentation and concepts:

### Direct Mappings

#### Stateless Transformation Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| filter | `KStream.processValues()` / `KTable.filter()` | Filter records based on conditions |
| filterNot | `KStream.processValues()` / `KTable.filterNot()` | Filter out matching records |
| flatMap | `KStream.process()` | Transform one record to multiple records |
| map | `KStream.process()` | Transform both key and value |
| mapKey | `KStream.process()` | Transform only the key |
| mapValues | `KStream.processValues()` / `KTable.transformValues()` | Transform only the value |
| selectKey | `KStream.process()` | Select new key from record content |
| transformKey | `KStream.process()` | Transform key using custom function |
| transformValue | `KStream.processValues()` | Transform value using custom function |

#### Format Conversion Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| convertKey | `KStream.processValues()` | Convert key data format |
| convertValue | `KStream.processValues()` | Convert value data format |

#### Grouping & Partitioning Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| groupBy | `KStream.groupBy()` / `KTable.groupBy()` | Group by new key |
| groupByKey | `KStream.groupByKey()` | Group by existing key |
| repartition | `KStream.repartition()` | Redistribute across partitions |

#### Stateful Aggregation Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| aggregate | `KGroupedStream.aggregate()` / `KGroupedTable.aggregate()` | Custom aggregation logic |
| count | `KGroupedStream.count()` / `KGroupedTable.count()` | Count records per key |
| reduce | `KGroupedStream.reduce()` / `KGroupedTable.reduce()` | Reduce to single value per key |

#### Join Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| join | `KStream.join()` / `KTable.join()` | Inner join streams/tables |
| leftJoin | `KStream.leftJoin()` / `KTable.leftJoin()` | Left outer join streams/tables |
| merge | `KStream.merge()` | Merge multiple streams into one |
| outerJoin | `KStream.outerJoin()` / `KTable.outerJoin()` | Full outer join streams/tables |

#### Windowing Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| windowBySession | `KGroupedStream.windowedBy(SessionWindows)` | Session-based windowing |
| windowByTime | `KGroupedStream.windowedBy(TimeWindows)` | Time-based windowing |

#### Output Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| forEach | `KStream.processValues()` | Side effects without output |
| print | `KStream.processValues()` | Print to stdout/file |
| to | `KStream.to()` | Send to Kafka topic |
| toTopicNameExtractor | `KStream.to(TopicNameExtractor)` | Dynamic topic routing |

#### Control Flow Operations
| KSML Operation | Kafka Streams Method | Purpose |
|---|---|---|
| branch | `KStream.split()` | Split into multiple branches |
| peek | `KStream.processValues()` | Observe records without changes |

### Key Implementation Details

- Most KSML operations use `KStream.process()` or `KStream.processValues()` with custom processor suppliers rather than direct DSL methods. This enables seamless integration with KSML's Python function execution system.
- Operations automatically adapt to work with KStream, KTable, and windowed streams, mapping to the appropriate Kafka Streams method based on context.
- Stateful operations support configurable state stores through KSML's unified state management system.
- Each operation integrates with Python functions through specialized user function wrappers (`UserPredicate`, `UserKeyTransformer`, etc.).
