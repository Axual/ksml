# KSML Functions Reference

This document provides a comprehensive reference for all function types available in KSML. Each function type is described with its parameters, behavior, and examples.

## Function Types Overview

KSML supports various function types, each designed for specific purposes in stream processing:

| Function Type | Purpose | Used In |
|--------------|---------|---------|
| **Stateless Functions** | Process each message independently | |
| predicate | Return true/false based on message content | filter, branch |
| mapper | Transform messages from one form to another | mapValues |
| keyValueMapper | Transform both key and value | map |
| forEach | Process each message for side effects | peek |
| **Stateful Functions** | Maintain state across multiple messages | |
| aggregator | Incrementally build aggregated results | aggregate |
| reducer | Combine two values into one | reduce |
| initializer | Provide initial values for aggregations | aggregate |
| **Special Purpose Functions** | | |
| transformer | Access state stores and process records | process |
| valueJoiner | Combine data from multiple streams | join, leftJoin, outerJoin |
| timestampExtractor | Extract timestamps from messages | stream definitions |

## Stateless Functions

### `predicate`

Returns true or false based on message content. Used for filtering and branching operations.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |

#### Return Value

Boolean (true or false)

#### Example

```yaml
functions:
  is_adult:
    type: predicate
    expression: value.get("age") >= 18

  is_valid_transaction:
    type: predicate
    code: |
      if value is None:
        return False

      amount = value.get("amount")
      if amount is None or amount <= 0:
        return False

      return True
```

### `mapper`

Transforms a value from one form to another.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |

#### Return Value

Transformed value

#### Example

```yaml
functions:
  enrich_user:
    type: mapper
    code: |
      return {
        "id": value.get("user_id"),
        "full_name": value.get("first_name") + " " + value.get("last_name"),
        "email": value.get("email"),
        "age": value.get("age"),
        "is_adult": value.get("age", 0) >= 18,
        "processed_at": int(time.time() * 1000)
      }
```

### `keyValueMapper`

Transforms both the key and value of a record.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |

#### Return Value

Tuple of (new_key, new_value)

#### Example

```yaml
functions:
  repartition_by_user_id:
    type: keyValueMapper
    code: |
      new_key = value.get("user_id")
      new_value = value
      return (new_key, new_value)
```

### `forEach`

Processes each message for side effects like logging, without changing the message.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |

#### Return Value

None (the function is called for its side effects)

#### Example

```yaml
functions:
  log_message:
    type: forEach
    code: |
      log.info("Processing record with key={}, value={}", key, value)

      # You can also increment metrics
      metrics.counter("records_processed").increment()
```

## Stateful Functions

### `aggregator`

Incrementally builds aggregated results from multiple messages.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |
| aggregatedValue | Any | The current aggregated value (can be None) |

#### Return Value

New aggregated value

#### Example

```yaml
functions:
  average_calculator:
    type: aggregator
    code: |
      if aggregatedValue is None:
        return {"count": 1, "sum": value.get("amount", 0), "average": value.get("amount", 0)}
      else:
        count = aggregatedValue.get("count", 0) + 1
        sum = aggregatedValue.get("sum", 0) + value.get("amount", 0)
        return {
          "count": count,
          "sum": sum,
          "average": sum / count
        }
```

### `reducer`

Combines two values into one.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| value1 | Any | The first value to combine |
| value2 | Any | The second value to combine |

#### Return Value

Combined value

#### Example

```yaml
functions:
  sum_reducer:
    type: reducer
    code: |
      return {
        "count": value1.get("count", 0) + value2.get("count", 0),
        "sum": value1.get("sum", 0) + value2.get("sum", 0)
      }
```

### `initializer`

Provides initial values for aggregations.

#### Parameters

None

#### Return Value

Initial value for aggregation

#### Example

```yaml
functions:
  counter_initializer:
    type: initializer
    expression: {"count": 0, "sum": 0, "min": null, "max": null}
```

## Special Purpose Functions

### `transformer`

Advanced function that can access state stores and process records.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |
| stateStore | StateStore | The state store to use (if configured) |

#### Return Value

Transformed value or None

#### Example

```yaml
functions:
  deduplicate_events:
    type: transformer
    code: |
      # Access a state store to check for duplicates
      event_id = value.get("event_id")
      if event_id is None:
        return value

      # Check if we've seen this event before
      seen_before = stateStore.get(event_id)
      if seen_before:
        # Skip duplicate event
        return None

      # Mark this event as seen
      stateStore.put(event_id, True)

      # Process the event
      return value
```

### `valueJoiner`

Combines data from multiple streams during join operations.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| value1 | Any | The value from the first stream |
| value2 | Any | The value from the second stream |

#### Return Value

Combined value

#### Example

```yaml
functions:
  join_order_with_customer:
    type: valueJoiner
    code: |
      order = value1
      customer = value2

      if customer is None:
        customer_name = "Unknown"
        customer_email = "Unknown"
      else:
        customer_name = customer.get("name", "Unknown")
        customer_email = customer.get("email", "Unknown")

      return {
        "order_id": order.get("order_id"),
        "customer_id": order.get("customer_id"),
        "customer_name": customer_name,
        "customer_email": customer_email,
        "items": order.get("items", []),
        "total": order.get("total", 0),
        "status": order.get("status", "PENDING")
      }
```

### `timestampExtractor`

Extracts timestamps from messages for time-based operations.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| key | Any | The key of the record being processed |
| value | Any | The value of the record being processed |
| previousTimestamp | Long | The previous timestamp (can be used as fallback) |

#### Return Value

Timestamp in milliseconds (long)

#### Example

```yaml
functions:
  event_timestamp_extractor:
    type: timestampExtractor
    code: |
      # Try to get timestamp from the event
      if value is not None and "timestamp" in value:
        return value.get("timestamp")

      # Fall back to record timestamp
      return previousTimestamp
```

## Function Definition Formats

KSML supports two formats for defining functions:

### Expression Format

For simple, one-line functions:

```yaml
functions:
  is_valid:
    type: predicate
    expression: value.get("status") == "ACTIVE"
```

### Code Block Format

For more complex functions:

```yaml
functions:
  process_transaction:
    type: mapper
    code: |
      result = {}

      # Copy basic fields
      result["transaction_id"] = value.get("id")
      result["amount"] = value.get("amount", 0)

      # Calculate fee
      amount = value.get("amount", 0)
      if amount > 1000:
        result["fee"] = amount * 0.02
      else:
        result["fee"] = amount * 0.03

      # Add timestamp
      result["processed_at"] = int(time.time() * 1000)

      return result
```

## Function Execution Context

When your Python functions execute, they have access to:

### Logger

For outputting information to the application logs:

```yaml
# In your function code, you can use the log object:
# Example:
# log.debug("Debug message")
# log.info("Info message")
# log.warn("Warning message")
# log.error("Error message")
```

### Metrics

For monitoring function performance and behavior:

```yaml
# In your function code, you can use the metrics object:
# Examples:
# Increment a counter
# metrics.counter("records_processed").increment()
#
# Record a value
# metrics.gauge("record_size").record(len(str(value)))
```

### State Stores

For maintaining state between function invocations (when configured):

```yaml
# In your function code, you can use the stateStore object:
# Examples:
# Get a value from the state store
# previous_value = stateStore.get(key)
#
# Put a value in the state store
# stateStore.put(key, new_value)
#
# Delete a value from the state store
# stateStore.delete(key)
```

## Best Practices

1. **Keep functions focused**: Each function should do one thing well
2. **Handle errors gracefully**: Use try/except blocks to prevent pipeline failures
3. **Consider performance**: Python functions introduce some overhead, so keep them efficient
4. **Use appropriate function types**: Choose the right function type for your use case
5. **Leverage state stores**: For complex stateful operations, use state stores rather than global variables
6. **Document your functions**: Add comments to explain complex logic and business rules
7. **Test thoroughly**: Write unit tests for your functions to ensure they behave as expected

## Related Topics

- [KSML Language Reference](language-reference.md)
- [Operations Reference](operations-reference.md)
- [Data Types Reference](data-types-reference.md)
- [Configuration Reference](configuration-reference.md)
