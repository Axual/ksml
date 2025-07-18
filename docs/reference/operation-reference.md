# KSML Operation Reference

This document provides a comprehensive reference for all operations available in KSML. Each operation is described with
its parameters, behavior, and examples.

## Stateless Operations

Stateless operations process each record independently, without maintaining any state between records.

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
- type: filter
  if:
    expression: value.get("age") >= 18
```

```yaml
- type: filter
  if:
    code: |
      if value.get("status") == "ACTIVE" and value.get("age") >= 18:
        return True
      return False
```

### `flatMap`

Transforms each record into zero or more records.

#### Parameters

| Parameter | Type   | Required | Description                                                  |
|-----------|--------|----------|--------------------------------------------------------------|
| `mapper`  | Object | Yes      | Specifies how to transform each record into multiple records |

The `mapper` can be defined using:

- `expression`: A simple expression returning a list of tuples (key, value)
- `code`: A Python code block returning a list of tuples (key, value)

#### Example

```yaml
- type: flatMap
  mapper:
    code: |
      result = []
      for item in value.get("items", []):
        result.append((item.get("id"), item))
      return result
```

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
- type: map
  mapper:
    code: |
      new_key = value.get("id")
      new_value = {
        "name": value.get("firstName") + " " + value.get("lastName"),
        "age": value.get("age")
      }
      return (new_key, new_value)
```

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
- type: mapValues
  mapper:
    expression: {"name": value.get("firstName") + " " + value.get("lastName"), "age": value.get("age")}
```

```yaml
- type: mapValues
  mapper:
    code: |
      return {
        "full_name": value.get("firstName") + " " + value.get("lastName"),
        "age_in_months": value.get("age") * 12
      }
```

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
- type: peek
  forEach:
    code: |
      log.info("Processing record with key={}, value={}", key, value)
```

### `selectKey`

Changes the key of each record without modifying the value.

#### Parameters

| Parameter     | Type   | Required | Description                         |
|---------------|--------|----------|-------------------------------------|
| `keySelector` | Object | Yes      | Specifies how to select the new key |

The `keySelector` can be defined using:

- `expression`: A simple expression returning the new key
- `code`: A Python code block returning the new key

#### Example

```yaml
- type: selectKey
  keySelector:
    expression: value.get("userId")
```

## Stateful Operations

Stateful operations maintain state between records, typically based on the record key.

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
- type: aggregate
  initializer:
    expression: {"count": 0, "sum": 0}
  aggregator:
    code: |
      if aggregate is None:
        return {"count": 1, "sum": value.get("amount", 0)}
      else:
        return {
          "count": aggregate.get("count", 0) + 1,
          "sum": aggregate.get("sum", 0) + value.get("amount", 0)
        }
```

### `count`

Counts the number of records for each key.

#### Parameters

None.

#### Example

```yaml
- type: groupByKey
- type: count
```

### `groupByKey`

Groups records by key for subsequent aggregation operations.

#### Parameters

None. This operation is typically followed by an aggregation operation.

#### Example

```yaml
- type: groupByKey
- type: count
```

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
- type: reduce
  reducer:
    code: |
      return {
        "count": value1.get("count", 0) + value2.get("count", 0),
        "sum": value1.get("sum", 0) + value2.get("sum", 0)
      }
```

## Join Operations

Join operations combine data from multiple streams based on keys.

### `join`

Performs an inner join between two streams.

#### Parameters

| Parameter    | Type   | Required | Description                                                           |
|--------------|--------|----------|-----------------------------------------------------------------------|
| `with`       | String | Yes      | The name of the stream to join with                                   |
| `windowSize` | Long   | No       | The size of the join window in milliseconds (for stream-stream joins) |

#### Example

```yaml
- type: join
  with: customers
```

### `leftJoin`

Performs a left join between two streams.

#### Parameters

| Parameter    | Type   | Required | Description                                                           |
|--------------|--------|----------|-----------------------------------------------------------------------|
| `with`       | String | Yes      | The name of the stream to join with                                   |
| `windowSize` | Long   | No       | The size of the join window in milliseconds (for stream-stream joins) |

#### Example

```yaml
- type: leftJoin
  with: customers
```

### `outerJoin`

Performs an outer join between two streams.

#### Parameters

| Parameter    | Type   | Required | Description                                                           |
|--------------|--------|----------|-----------------------------------------------------------------------|
| `with`       | String | Yes      | The name of the stream to join with                                   |
| `windowSize` | Long   | No       | The size of the join window in milliseconds (for stream-stream joins) |

#### Example

```yaml
- type: outerJoin
  with: customers
  windowSize: 60000  # 1 minute
```

## Windowing Operations

Windowing operations group records into time-based windows.

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
- type: windowBySession
  inactivityGap: 1m  # 1 minute window
```

```yaml
- type: windowBySession
  inactivityGap: 1m  # 1 minute window
  grace: 15s         # 15 seconds grace
```

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
- type: windowByTime
  windowType: tumbling
  timeDifference: 60000  # 1 minute window
```

```yaml
- type: windowByTime
  windowType: hopping
  timeDifference: 5m  # 5 minute window
  advanceBy: 1m       # Advance every 1 minute
  grace: 15s          # 15 seconds grace
```

## Branch Operations

Branch operations split a stream into multiple substreams.

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
- branch:
    - if: predicate1
      via:
        - type: transformValue
          mapper: my_value_transformer
      to: target_topic
    - if: predicate2
      as: some_name_to_refer_to_by_another_pipeline
    - if: predicate3
      toTopicNameExtractor: my_topic_name_extractor
```

## Error Handling Operations

Error handling operations provide mechanisms to handle errors during processing.

### `try`

Attempts to execute operations and catches any exceptions.

#### Parameters

| Parameter    | Type  | Required | Description                                  |
|--------------|-------|----------|----------------------------------------------|
| `operations` | Array | Yes      | Operations to try                            |
| `catch`      | Array | Yes      | Operations to execute if an exception occurs |

#### Example

```yaml
- type: try
  operations:
    - type: mapValues
      mapper:
        code: parse_complex_json(value)
  catch:
    - type: mapValues
      mapper:
        code: |
          log.error("Failed to parse JSON: {}", exception)
          return {"error": "Failed to parse", "original": value}
```

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
