# KSML Function Reference

This document provides a comprehensive reference for all function types available in KSML. Each function type is
described with its parameters, behavior, and examples.

## Function Types Overview

KSML supports various function types, each designed for specific purposes in stream processing:

| Function Type                                                           | Purpose                                              | Used In                                     |
|-------------------------------------------------------------------------|------------------------------------------------------|---------------------------------------------|
| **Functions for stateless operations**                                  |                                                      |                                             |
| [forEach](#foreach)                                                     | Process each message for side effects                | peek                                        |
| [keyTransformer](#keytransformer)                                       | Convert a key to another type or value               | mapKey, selectKey, toStream, transformKey   |
| [keyValueToKeyValueListTransformer](#keyvaluetokeyvaluelisttransformer) | Convert key and value to a list of key/values        | flatMap, transformKeyValueToKeyValueList    |
| [keyValueToValueListTransformer](#keyvaluetovaluelisttransformer)       | Convert key and value to a list of values            | flatMapValues, transformKeyValueToValueList |
| [keyValueTransformer](#keyvaluetransformer)                             | Convert key and value to another key and value       | flatMapValues, transformKeyValueToValueList |
| [predicate](#predicate)                                                 | Return true/false based on message content           | filter, branch                              |
| [valueTransformer](#valuetransformer)                                   | Convert value to another type or value               | mapValue, mapValues, transformValue         |
|                                                                         |                                                      |                                             |
| **Functions for stateful operations**                                   |                                                      |                                             |
| [aggregator](#aggregator)                                               | Incrementally build aggregated results               | aggregate                                   |
| [initializer](#initializer)                                             | Provide initial values for aggregations              | aggregate                                   |
| [merger](#merger)                                                       | Merge two aggregation results into one               | aggregate                                   |
| [reducer](#reducer)                                                     | Combine two values into one                          | reduce                                      |
|                                                                         |                                                      |                                             |
| **Special Purpose Functions**                                           |                                                      |                                             |
| [foreignKeyExtractor](#foreignkeyextractor)                             | Extract a key from a join table's record             | join, leftJoin                              |
| [generator](#generator)                                                 | Function used in producers to generate a message     | producer                                    |
| [keyValueMapper](#keyvaluemapper)                                       | Convert key and value into a single output value     | groupBy, join, leftJoin                     |
| [keyValuePrinter](#keyvalueprinter)                                     | Output key and value                                 | print                                       |
| [metadataTransformer](#metadatatransformer)                             | Convert Kafka headers and timestamps                 | transformMetadata                           |
| [valueJoiner](#valuejoiner)                                             | Combine data from multiple streams                   | join, leftJoin, outerJoin                   |
|                                                                         |                                                      |                                             |
| **Stream Related Functions**                                            |                                                      |                                             |
| [timestampExtractor](#timestampextractor)                               | Extract timestamps from messages                     | stream, table, globalTable                  |
| [topicNameExtractor](#topicnameextractor)                               | Derive a target topic name from key and value        | toTopicNameExtractor                        |
| [streamPartitioner](#streampartitioner)                                 | Determine to which partition(s) a record is produced | stream, table, globalTable                  |
|                                                                         |                                                      |                                             |
| **Other Functions**                                                     |                                                      |                                             |
| [generic](#generic)                                                     | Generic custom function                              |                                             |

## Functions for stateless operations

### forEach

Processes each message for side effects like logging, without changing the message.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

None (the function is called for its side effects)

#### Example

```yaml
{% include "../../examples/reference/functions/foreach-example.yaml" %}
```

### keyTransformer

Transforms a key/value into a new key, which then gets combined with the original value as a new message on the output
stream.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

New key for the output message

#### Example

```yaml
{% include "../../examples/reference/functions/keytransformer-example.yaml" %}
```

### keyValueToKeyValueListTransformer

Takes one message and converts it into a list of output messages, which then get sent to the output stream.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

A list of key-value pairs `[(key1, value1), (key2, value2), ...]`

#### Example

```yaml
{% include "../../examples/reference/functions/keyvaluetokeyvaluelisttransformer.yaml" %}
```

### keyValueToValueListTransformer

Takes one message and converts it into a list of output values, which then get combined with the original key and sent
to the output stream.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

A list of values `[value1, value2, ...]` that will be combined with the original key

#### Example

```yaml
functions:
  explode_items:
    type: keyValueToValueListTransformer
    code: |
      # Input: key = "order123", value = {"items": [{"id": "item1"}, {"id": "item2"}]}
      # Output: ("order123", {"id": "item1"}), ("order123", {"id": "item2"})

      if value is None or "items" not in value:
        return []

      return value["items"]
    result: "[struct]"
```

### keyValueTransformer

Takes one message and converts it into another message, which may have different key/value types.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

A tuple of (new_key, new_value)

#### Example

```yaml
functions:
  transform_order:
    type: keyValueTransformer
    code: |
      if value is None:
        return (None, None)

      # Create a new key based on customer ID
      new_key = value.get("customer_id", "unknown")

      # Create a new value with selected fields
      new_value = {
        "order_id": value.get("order_id"),
        "total_amount": value.get("total_amount", 0),
        "item_count": len(value.get("items", [])),
        "processed_at": int(time.time() * 1000)
      }

      return (new_key, new_value)
    resultType: "(string,struct)"
```

### predicate

Returns true or false based on message content. Used for filtering and branching operations.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

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

  deduplicate_events:
    type: predicate
    code: |
      # Access a state store to check for duplicates
      event_id = value.get("event_id")
      if event_id is None:
        return True

      # Check if we've seen this event before
      seen_before = event_store.get(event_id)
      if seen_before:
        # Skip duplicate event
        return False

      # Mark this event as seen
      stateStore.put(event_id, True)

      # Process the event
      return True
    stores:
      - event_store
```

### valueTransformer

Transforms a key/value into a new value, which is combined with the original key and sent to the output stream.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

New value for the output message

#### Example

```yaml
functions:
  enrich_user:
    type: valueTransformer
    code: |
      return {
        "id": value.get("user_id"),
        "full_name": value.get("first_name") + " " + value.get("last_name"),
        "email": value.get("email"),
        "age": value.get("age"),
        "is_adult": value.get("age", 0) >= 18,
        "processed_at": int(time.time() * 1000)
      }
    resultType: struct

```

## Functions for stateful operations

### aggregator

Incrementally builds aggregated results from multiple messages.

#### Parameters

| Parameter       | Type | Description                                |
|-----------------|------|--------------------------------------------|
| key             | Any  | The key of the record being processed      |
| value           | Any  | The value of the record being processed    |
| aggregatedValue | Any  | The current aggregated value (can be None) |

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
    resultType: struct
```

### initializer

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
    expression: { "count": 0, "sum": 0, "average": 0 }
    resultType: struct
```

### merger

Merges two aggregation results into one. Used in aggregation operations to combine partial results.

#### Parameters

| Parameter | Type | Description                           |
|-----------|------|---------------------------------------|
| key       | Any  | The key of the record being processed |
| value1    | Any  | The value of the first aggregation    |
| value2    | Any  | The value of the second aggregation   |

#### Return Value

The merged aggregation result

#### Example

```yaml
functions:
  merge_stats:
    type: merger
    code: |
      # Merge two statistics objects
      if value1 is None:
        return value2
      if value2 is None:
        return value1

      # Combine counts and sums
      count = value1.get("count", 0) + value2.get("count", 0)
      sum = value1.get("sum", 0) + value2.get("sum", 0)
      result = {
        "count": count,
        "sum": sum,
        "average": sum/count if count>0 else 0
      }

      return result
    resultType: struct
```

### reducer

Combines two values into one.

#### Parameters

| Parameter | Type | Description                 |
|-----------|------|-----------------------------|
| value1    | Any  | The first value to combine  |
| value2    | Any  | The second value to combine |

#### Return Value

Combined value

#### Example

```yaml
functions:
  sum_reducer:
    type: reducer
    code: |
      count = value1.get("count", 0) + value2.get("count", 0)
      sum = value1.get("sum", 0) + value2.get("sum", 0)
      return {
        "count": count,
        "sum": sum,
        "average": sum/count if count>0 else 0
      }
    resultType: struct
```

## Special Purpose Functions

### foreignKeyExtractor

Extracts a key from a join table's record. Used during join operations to determine which records to join.

#### Parameters

| Parameter | Type | Description                               |
|-----------|------|-------------------------------------------|
| value     | Any  | The value of the record to get a key from |

#### Return Value

The key to look up in the table being joined with

#### Example

```yaml
functions:
  extract_customer_id:
    type: foreignKeyExtractor
    code: |
      # Extract customer ID from an order to join with customer table
      if value is None:
        return None

      return value.get("customer_id")
    resultType: string
```

### generator

Function used in producers to generate messages. It takes no input parameters and produces key-value pairs.

#### Parameters

None

#### Return Value

A tuple of (key, value) representing the generated message

#### Example

```yaml
functions:
  generate_sensordata_message:
    type: generator
    globalCode: |
      import time
      import random
      sensorCounter = 0
    code: |
      global sensorCounter

      key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
      sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

      # Generate some random sensor measurement data
      types = { 0: { "type": "AREA", "unit": random.choice([ "m2", "ft2" ]), "value": str(random.randrange(1000)) },
                1: { "type": "HUMIDITY", "unit": random.choice([ "g/m3", "%" ]), "value": str(random.randrange(100)) },
                2: { "type": "LENGTH", "unit": random.choice([ "m", "ft" ]), "value": str(random.randrange(1000)) },
                3: { "type": "STATE", "unit": "state", "value": random.choice([ "off", "on" ]) },
                4: { "type": "TEMPERATURE", "unit": random.choice([ "C", "F" ]), "value": str(random.randrange(-100, 100)) }
              }

      # Build the result value using any of the above measurement types
      value = { "name": key, "timestamp": str(round(time.time()*1000)), **random.choice(types) }
      value["color"] = random.choice([ "black", "blue", "red", "yellow", "white" ])
      value["owner"] = random.choice([ "Alice", "Bob", "Charlie", "Dave", "Evan" ])
      value["city"] = random.choice([ "Amsterdam", "Xanten", "Utrecht", "Alkmaar", "Leiden" ])

      if random.randrange(10) == 0:
        key = None
      if random.randrange(10) == 0:
        value = None
    expression: (key, value)                      # Return a message tuple with the key and value
    resultType: (string, struct)                  # Indicate the type of key and value
```

### keyValueMapper

Transforms both the key and value of a record.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

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
    resultType: "(string, struct)"
```

### keyValuePrinter

Converts a message to a string for output to a file or stdout.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

String to be written to file or stdout

#### Example

```yaml
functions:
  format_message:
    type: keyValuePrinter
    code: |
      # Format the message as a JSON string with timestamp
      import json
      import time

      timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

      if value is None:
        return f"[{timestamp}] Key: {key}, Value: null"

      try:
        # Try to format value as JSON
        value_str = json.dumps(value, indent=2)
        return f"[{timestamp}] Key: {key}\nValue:\n{value_str}"
      except:
        # Fall back to string representation
        return f"[{timestamp}] Key: {key}, Value: {str(value)}"
```

### metadataTransformer

Transforms a message's metadata (headers and timestamp).

#### Parameters

| Parameter | Type   | Description                                       |
|-----------|--------|---------------------------------------------------|
| key       | Any    | The key of the record being processed             |
| value     | Any    | The value of the record being processed           |
| metadata  | Object | Contains the headers and timestamp of the message |

#### Return Value

Modified metadata for the output message

#### Example

```yaml
functions:
  addTime:
    type: metadataTransformer
    code: |
      # Add a custom header to the message
      metadata["headers"] = metadata["headers"] + [ { "key": "my_own_header_key", "value": "some_value" } ]
    expression: metadata
```

### valueJoiner

Combines data from multiple streams during join operations.

#### Parameters

| Parameter | Type | Description                      |
|-----------|------|----------------------------------|
| value1    | Any  | The value from the first stream  |
| value2    | Any  | The value from the second stream |

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
    resultType: struct
```

## Stream Related Functions

### timestampExtractor

Extracts timestamps from messages for time-based operations.

#### Parameters

| Parameter         | Type | Description                                      |
|-------------------|------|--------------------------------------------------|
| key               | Any  | The key of the record being processed            |
| value             | Any  | The value of the record being processed          |
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

### topicNameExtractor

Derives a target topic name from key and value. Used to dynamically route messages to different topics.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

String representing the topic name to send the message to

#### Example

```yaml
functions:
  route_by_sensor:
    type: topicNameExtractor
    code: |
      if key == 'sensor1':
        return 'sensordata_sensor1'
      if key == 'sensor2':
        return 'sensordata_sensor2'
      return 'sensordata_other_sensors'
```

### streamPartitioner

Determines to which partition a record is produced. Used to control the partitioning of output topics.

#### Parameters

| Parameter     | Type    | Description                                  |
|---------------|---------|----------------------------------------------|
| topic         | String  | The topic of the message                     |
| key           | Any     | The key of the record being processed        |
| value         | Any     | The value of the record being processed      |
| numPartitions | Integer | The number of partitions in the output topic |

#### Return Value

Integer representing the partition number to which the message will be sent

#### Example

```yaml
functions:
  custom_partitioner:
    type: streamPartitioner
    code: |
      # Partition by the first character of the key (if it's a string)
      if key is None:
        # Use default partitioning for null keys
        return None

      if isinstance(key, str) and len(key) > 0:
        # Use the first character's ASCII value modulo number of partitions
        return ord(key[0]) % numPartitions

      # For non-string keys, use a hash of the string representation
      return hash(str(key)) % numPartitions
```

## Other Functions

### generic

Generic custom function that can be used for any purpose. It can accept custom parameters and return any type of value.

#### Parameters

User-defined parameters

#### Return Value

Any value, depending on the function's purpose

#### Example

```yaml
functions:
  calculate_discount:
    type: generic
    parameters:
      - name: basePrice
        type: double
      - name: discountPercentage
        type: double
    code: |
      # Calculate the discounted price
      discountAmount = basePrice * (discountPercentage / 100)
      finalPrice = basePrice - discountAmount

      # Return both the final price and the discount amount
      return {
        "finalPrice": finalPrice,
        "discountAmount": discountAmount,
        "discountPercentage": discountPercentage
      }
    resultType: struct
```

## Function Definition Formats

KSML supports two formats for defining functions:

### Expression Format

For simple, one-line functions:

```yaml
functions:
  is_valid:
    type: predicate
    code: |
      # Code is optional here
    expression: value.get("status") == "ACTIVE"
```

### Code Block Format

For more complex functions:

```yaml
functions:
  process_transaction:
    type: keyValueMapper
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
    resultType: struct
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

In your function code, you can use the state stores declared by the function as variables:

```yaml
  deduplicate_events:
    type: predicate
    code: |
      previous_value = my_state_store.get(key)
      if previous_value is not None:
        
      
      # Access a state store to check for duplicates
      event_id = value.get("event_id")
      if event_id is None:
        return True

      # Check if we've seen this event before
      seen_before = event_store.get(event_id)
      if seen_before:
        # Skip duplicate event
        return False

      # Mark this event as seen
      stateStore.put(event_id, True)

      # Process the event
      return True
    stores:
      - event_store
      
# Examples:
# Get a value from the state store
# previous_value = state_store.get(key)
#
# Put a value in the state store
# state_store.put(key, new_value)
#
# Delete a value from the state store
# state_store.delete(key)
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
- [Operations Reference](operation-reference.md)
- [Data Types Reference](data-types-reference.md)
- [Configuration Reference](configuration-reference.md)
