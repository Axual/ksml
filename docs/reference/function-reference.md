# Function Reference

This document provides a comprehensive reference for all function types available in KSML. Functions in KSML allow you
to implement custom logic for processing your streaming data using Python, making stream processing accessible to data
scientists, analysts, and developers who may not be familiar with Java or Kafka Streams API.

## Function Definition Structure

Functions are defined in the `functions` section of your KSML definition file. Each function has the following
properties:

| Property     | Type      | Required  | Description                                                                                    |
|--------------|-----------|-----------|------------------------------------------------------------------------------------------------|
| `type`       | String    | Yes       | The type of function (predicate, aggregator, valueJoiner, etc.)                                |
| `parameters` | Array     | No        | **Additional** custom parameters to add to the function's built-in parameters (see note below) |
| `globalCode` | String    | No        | Python code executed once upon startup                                                         |
| `code`       | String    | No        | Python code implementing the function                                                          |
| `expression` | String    | No        | An expression that the function will return as value                                           |
| `resultType` | Data type | Sometimes | The data type returned by the function. Required when it cannot be derived from function type. |
| `stores`     | Array     | No        | List of state stores the function can access                                                   |

**Note about parameters:** Every function type has built-in parameters that are automatically provided by KSML (e.g.,
`key` and `value` for most function types). The `parameters` property is only needed when you want to add custom
parameters beyond these built-in ones. These additional parameters can then be passed when calling the function from
Python code.

## What are Functions in KSML?

Functions provide the flexibility to go beyond built-in operations and implement specific business logic,
transformations, and data processing requirements. They are written in Python and executed within the KSML runtime,
combining the power of Kafka Streams with the simplicity and expressiveness of Python.

## Writing Python Functions

### Example KSML Function Definition

??? info "Example KSML Function Definition"

      ```yaml
      functions:
        # Example of a complete function definition with all components
        process_sensor_data:
          type: valueTransformer
          globalCode: |
            # This code runs once when the application starts
            import json
            import time
            
            # Initialize global variables
            sensor_threshold = 25.0
            alert_count = 0
            
          code: |
            # This code runs for each message
            global alert_count
            
            # Process the sensor value
            if value is None:
              return None
              
            temperature = value.get("temperature", 0)
            
            # Convert Celsius to Fahrenheit
            temperature_f = (temperature * 9/5) + 32
            
            # Check for alerts
            is_alert = temperature > sensor_threshold
            if is_alert:
              alert_count += 1
              log.warn("High temperature detected: {}°C", temperature)
            
            # Return enriched data
            result = {
              "original_temp_c": temperature,
              "temp_fahrenheit": temperature_f,
              "is_alert": is_alert,
              "total_alerts": alert_count,
              "processed_at": int(time.time() * 1000)
            }
            
            return result
            
          resultType: json
          
        # Example of a simple expression-based function
        is_high_priority:
          type: predicate
          expression: value.get("priority", 0) > 7
          resultType: boolean
      ```

KSML functions are defined in the `functions` section of your KSML definition file. A typical function definition
includes:

- **Type**: Specifies the function's purpose and behavior
- **Parameters**: Input parameters the function accepts (defined by the function type)
- **GlobalCode**: Python code executed only once upon application start
- **Code**: Python code implementing the function's logic
- **Expression**: Shorthand for simple return expressions
- **ResultType**: The expected return type of the function

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

## Function Parameters

### Built-in vs Custom Parameters

Every function type in KSML has **built-in parameters** that are automatically provided by KSML. These are implicitly
available in your function code without needing to declare them:

Most function types (like `forEach`, `predicate`, `valueTransformer`) automatically receive:

- `key` - The record key
- `value` - The record value

Some specialized types have different built-in parameters:

- `aggregator`: receives `key`, `value`, and `aggregate`
- `merger`: receives `key`, `aggregate1`, and `aggregate2`
- `initializer`: receives no parameters

### Adding Custom Parameters

The `parameters` property allows you to **add custom parameters** beyond the built-in ones. This is useful when:

1. **Creating reusable functions** that can behave differently based on configuration
2. **Calling functions from Python code** with specific arguments
3. **Using the `generic` function type** which has no built-in parameters

#### Example WITHOUT custom parameters:

```yaml
functions:
  simple_logger:
    type: forEach
    # Only uses built-in key and value parameters
    code: |
      log.info("Processing: key={}, value={}", key, value)
```

#### Example WITH custom parameters:

```yaml
functions:
  configurable_logger:
    type: forEach
    parameters: # ADDS 'prefix' to the built-in key and value
      - name: prefix
        type: string
    code: |
      log.info("{}: key={}, value={}", prefix, key, value)
```

When calling this function from Python:

```python
# The custom parameter is passed along with built-in ones
configurable_logger(key, value, prefix="DEBUG")
```

### Parameter Definition Structure

When defining custom parameters:

```yaml
parameters:
  - name: parameter_name   # Name of the parameter
    type: parameter_type   # Data type (string, int, double, etc.)
```

**Important:** The `parameters` property **adds to** the built-in parameters - it doesn't replace them. Built-in
parameters like `key` and `value` are still available in your function code.

## Function Execution Context

When your Python functions execute, they have access to:

- **Logger**: For outputting information to application logs
  - `log.<log-level>("Debug message")` - <log_level> can be debug, info, warn, error, trace

- **Metrics**: For monitoring function performance and behavior
  - `metrics.counter("name").increment()` - Count occurrences
  - `metrics.gauge("name").record(value)` - Record values
  - `with metrics.timer("name"):` - Measure execution time

- **State Stores**: For maintaining state between function invocations (when configured)
  - `store.get(key)` - Retrieve value from store
  - `store.put(key, value)` - Store a value
  - `store.delete(key)` - Remove a value
  - Must be declared in the function's `stores` parameter

This execution context provides the tools needed for debugging, monitoring, and implementing stateful processing.

## Function Types Overview

KSML supports 21 function types, each designed for specific purposes in stream processing. Functions can range from
simple one-liners to complex implementations with multiple operations:

| Function Type                                                           | Purpose                                          | Used In                                     |
|-------------------------------------------------------------------------|--------------------------------------------------|---------------------------------------------|
| **Functions for stateless operations**                                  |                                                  |                                             |
| [forEach](#foreach)                                                     | Process each message for side effects            | peek                                        |
| [keyTransformer](#keytransformer)                                       | Convert a key to another type or value           | mapKey, selectKey, toStream, transformKey   |
| [keyValueToKeyValueListTransformer](#keyvaluetokeyvaluelisttransformer) | Convert key and value to a list of key/values    | flatMap, transformKeyValueToKeyValueList    |
| [keyValueToValueListTransformer](#keyvaluetovaluelisttransformer)       | Convert key and value to a list of values        | flatMapValues, transformKeyValueToValueList |
| [keyValueTransformer](#keyvaluetransformer)                             | Convert key and value to another key and value   | flatMapValues, transformKeyValueToValueList |
| [predicate](#predicate)                                                 | Return true/false based on message content       | filter, branch                              |
| [valueTransformer](#valuetransformer)                                   | Convert value to another type or value           | mapValue, mapValues, transformValue         |
|                                                                         |                                                  |                                             |
| **Functions for stateful operations**                                   |                                                  |                                             |
| [aggregator](#aggregator)                                               | Incrementally build aggregated results           | aggregate                                   |
| [initializer](#initializer)                                             | Provide initial values for aggregations          | aggregate                                   |
| [merger](#merger)                                                       | Merge two aggregation results into one           | aggregate                                   |
| [reducer](#reducer)                                                     | Combine two values into one                      | reduce                                      |
|                                                                         |                                                  |                                             |
| **Special Purpose Functions**                                           |                                                  |                                             |
| [foreignKeyExtractor](#foreignkeyextractor)                             | Extract a key from a join table's record         | join, leftJoin                              |
| [generator](#generator)                                                 | Function used in producers to generate a message | producer                                    |
| [keyValueMapper](#keyvaluemapper)                                       | Convert key and value into a single output value | groupBy, join, leftJoin                     |
| [keyValuePrinter](#keyvalueprinter)                                     | Output key and value                             | print                                       |
| [metadataTransformer](#metadatatransformer)                             | Convert Kafka headers and timestamps             | transformMetadata                           |
| [valueJoiner](#valuejoiner)                                             | Combine data from multiple streams               | join, leftJoin, outerJoin                   |
|                                                                         |                                                  |                                             |
| **Stream Related Functions**                                            |                                                  |                                             |
| [timestampExtractor](#timestampextractor)                               | Extract timestamps from messages                 | stream, table, globalTable                  |
| [topicNameExtractor](#topicnameextractor)                               | Derive a target topic name from key and value    | toTopicNameExtractor                        |
|                                                                         |                                                  |                                             |
| **Other Functions**                                                     |                                                  |                                             |
| [generic](#generic)                                                     | Generic custom function                          |                                             |

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

```yaml
--8<-- "definitions/reference/functions/keytransformer-processor.yaml:11:17"
```

**See how `forEach` is used in an example definition**:

- [Tutorial: Filtering and Transforming Example](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)

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

**Function Definition:**

```yaml
--8<-- "definitions/reference/functions/keytransformer-processor.yaml:11:17"
```

This function extracts the region from transaction data to use as the new message key, enabling region-based
partitioning.

**Complete Working Example:**

??? info "Producer - Regional Transaction Data (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/functions/keytransformer-producer.yaml"
    %}
    ```

??? info "Processor - Repartition by Region (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/functions/keytransformer-processor.yaml"
    %}
    ```

**Additional Example:**

See how `keyTransformer` is used for stream-table
joins: [Stream Table Join Tutorial](../tutorials/intermediate/joins.md#use-case-order-enrichment)

### keyValueToKeyValueListTransformer

Takes one message and converts it into a list of output messages, which then get sent to the output stream. Unlike
`keyValueToValueListTransformer`, this function can create new keys for each output message, enabling data reshaping and
repartitioning.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

A list of key-value pairs `[(key1, value1), (key2, value2), ...]`

#### Example

This example demonstrates splitting batch orders into individual orders with unique keys, useful for processing bulk
data into individual records.

??? info "Producer - `keyvaluetokeyvaluelisttransformer` example (click to expand)"
```yaml
{% include "../definitions/reference/functions/keyvaluetokeyvaluelisttransformer-producer.yaml" %}
```

??? info "Processor - `keyvaluetokeyvaluelisttransformer` example (click to expand)"
```yaml
{% include "../definitions/reference/functions/keyvaluetokeyvaluelisttransformer-processor.yaml" %}
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

??? info "Producer - Order Data with Items (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/functions/keyvaluetovaluelisttransformer-producer.yaml"
    %}
    ```

??? info "Processor - Explode Order Items (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/functions/keyvaluetovaluelisttransformer-processor.yaml"
    %}
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

**See how `keyValueTransformer` is used in an example definition**:

- [Async Integration Pattern with
  `keyValueTransformer`](../tutorials/advanced/external-integration.md#async-integration-pattern)

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

**See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#complex-filtering-techniques)
  for predicate functions for data filtering

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

**See it in action**:

- [Tutorial: Filtering and Transforming](../tutorials/beginner/filtering-transforming.md#applying-multiple-transformations)
  for understanding valueTransformer for data enrichment

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

**See it in action**:

- [Tutorial: Aggregations](../tutorials/intermediate/aggregations.md#aggregate-example) for comprehensive aggregator
  function examples

### initializer

Provides initial values for aggregations.

#### Parameters

None

#### Return Value

Initial value for aggregation

#### Example

**See it in action**:

- [Example for `initializer`](../tutorials/intermediate/aggregations.md#complex-example-regional-sales-analytics)

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

??? info "Producer - Session Activity Events (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/merger-example-producer.yaml" %}
    ```

??? info "Processor - Session Window with Merger (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/merger-example-processor.yaml" %}
    ```

The merger function is specifically designed for session window aggregations where late-arriving events can merge
previously separate sessions. This example demonstrates user activity tracking with session-based counting.

**What the example does:**

Simulates user activity tracking with session windows and merging:

* Groups events into 10-min inactivity sessions
* Counts events per user
* Merges sessions when late events connect them
* Producer simulates gaps to trigger merging

**Key Features:**

* Automatic session windowing & late data handling
* Type-safe merger (integers)
* Windowed key transformation for output
* Merge logic adds event counts

**Expected Results:**

When running this example, you'll see log messages like:

- `"Merging sessions: 3 + 2 = 5"` - Shows the merger function combining session counts
- `"User alice session: 4 events"` - Displays final session results after merging
- Session windows spanning different time periods for each user

### reducer

Combines two values into one.

#### Parameters

| Parameter | Type | Description                 |
|-----------|------|-----------------------------|
| value1    | Any  | The first value to combine  |
| value2    | Any  | The second value to combine |

#### Return Value

Combined value

**See how `reducer` is used in an example definition**:

- [Example for `reducer` function](../tutorials/intermediate/aggregations.md#human-readable-reduce-json-format)

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

??? info "Producer - Orders and Customers (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/foreignkeyextractor-producer.yaml" %}
    ```

??? info "Processor - Table Join with ForeignKeyExtractor (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/foreignkeyextractor-processor.yaml" %}
    ```

The foreignKeyExtractor enables table joins where the join key is embedded within the record value rather than being the
record key. This example demonstrates order enrichment by joining with customer data using a foreign key relationship.

**Example**

This example simulates an e-commerce system enriching orders with customer data:

* Orders keyed by `order_id`, referencing `customer_id`
* Customer details looked up by `customer_id`
* Foreign key extracted from orders
* Orders joined with customers to produce enriched records

**Key Features:**

* Foreign key join pattern
* KSML table join with key extraction
* Data enrichment from multiple sources
* Preserves original order keys

**Expected Results:**

When running this example, you'll see log messages like:

- `"Joined order order_001 with customer Alice Johnson"` - Shows successful order-customer joins
- `"Enriched order: order_003 for customer: Bob Smith"` - Displays enriched results with customer names
- Orders enriched with customer tier, email, and name information

**Use Cases:**

This pattern is commonly used for:

- Order enrichment with customer details
- Transaction enrichment with account information
- Event enrichment with user profiles
- Any scenario where records contain foreign key references

### generator

Function used in producers to generate messages. It takes no input parameters and produces key-value pairs.

#### Parameters

None

#### Return Value

A tuple of (key, value) representing the generated message

#### See how `generator` is used in an example definition:

- [Example: Generating JSON data](../tutorials/beginner/filtering-transforming.md#creating-test-data)
- [Example: Generating AVRO data](../tutorials/beginner/data-formats.md#working-with-avro-data)

### keyValueMapper

Transforms both the key and value of a record.

#### Parameters

| Parameter | Type | Description                             |
|-----------|------|-----------------------------------------|
| key       | Any  | The key of the record being processed   |
| value     | Any  | The value of the record being processed |

#### Return Value

Tuple of (new_key, new_value)

#### See how `keyValueMapper` is used in an example definition:

- [Example: Product Catalog Enrichment](../tutorials/intermediate/joins.md#use-case-product-catalog-enrichment)

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

The keyValuePrinter formats records for human-readable output to stdout or files. This example shows converting sales
data into formatted reports for monitoring and debugging.

??? info "Producer - Sales Data Generation (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/keyvalueprinter-producer.yaml" %}
    ```

??? info "Processor - Sales Report Formatting (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/keyvalueprinter-processor.yaml" %}
    ```

**What the example does:**

Demonstrates formatted business reporting with KSML:

* **Sales Data Processing:** Converts raw sales records into reports
* **Custom Formatting:** Transforms JSON into readable output
* **Print Operation:** Outputs formatted data via `print`
* **Real-time Monitoring:** Enables instant visibility of transactions

**Key Features:**

* Python string formatting
* Null/error handling
* `keyValuePrinter` function
* Field extraction from JSON

**Expected Results:**

When running this example, you'll see formatted output like:

- `SALE REPORT | ID: sale_001 | Customer: alice | Product: laptop | Qty: 2 | Amount: $1299.99 | Region: north`
- `SALE REPORT | ID: sale_002 | Customer: bob | Product: mouse | Qty: 1 | Amount: $29.99 | Region: south`

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

??? info "Producer - API Event Generation (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/metadatatransformer-producer.yaml" %}
    ```

??? info "Processor - Header Enrichment and Processing (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/metadatatransformer-processor.yaml" %}
    ```

**This example:**

- Shows metadata enrichment in stream processing.

* Generates realistic API events (endpoints + status codes)
* Enriches headers with timestamps, severity, processor ID
* Classifies events (critical/warning/info) by status code
* Logs enrichment for monitoring/debugging
* Uses Python `time` for timestamps
* Has conditional + extensible headers
* Uses `transformMetadata` with `mapper` param
* Output is formatted via `print`

**Expected Results:**

When running this example, you'll see enriched events with additional headers:

- `ENRICHED EVENT | evt_0001 | POST /api/users | Status: 200 | Headers processed`
- `ENRICHED EVENT | evt_0002 | DELETE /api/health | Status: 400 | Headers processed`
- Log messages showing: "Enriched event evt_0001 with 3 additional headers"

### valueJoiner

Combines values from two streams or tables during join operations, creating enriched records that contain data from both
sources. The function must handle cases where one or both values might be null, depending on the join type (inner, left,
outer). This function has access to the join key for context-aware value combination.

#### Parameters

| Parameter | Type | Description                            |
|-----------|------|----------------------------------------|
| key       | Any  | The join key used to match records     |
| value1    | Any  | The value from the first stream/table  |
| value2    | Any  | The value from the second stream/table |

#### Return Value

Combined value

#### Example

- [Tutorial: Joins](../tutorials/intermediate/joins.md#core-join-concepts) for learning about stream enrichment

## Stream Related Functions

### timestampExtractor

Extracts timestamps from messages for time-based operations.

#### Parameters

| Parameter         | Type   | Description                                                       |
|-------------------|--------|-------------------------------------------------------------------|
| record            | Object | The ConsumerRecord containing key, value, timestamp, and metadata |
| previousTimestamp | Long   | The previous timestamp (can be used as fallback)                  |

#### Return Value

Timestamp in milliseconds (long)

#### Example

??? info "Producer - Events with Custom Timestamps (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/timestampextractor-producer.yaml" %}
    ```

??? info "Processor - Extract Event Time (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/timestampextractor-processor.yaml" %}
    ```

**What the example does:**

Demonstrates custom timestamp extraction for event-time processing:

* Creates events with custom timestamps that simulate out-of-order and delayed processing scenarios
* Extracts event-time timestamps from message content rather than using record timestamps
* Processes events based on their event time rather than arrival time
* Provides robust fallback mechanisms for missing or invalid timestamps
* Uses custom `timestampExtractor` function in stream definition
* Uses Python time manipulation and timestamp handling
* Support for both ConsumerRecord and direct value access patterns

**Expected Results:**

When running this example, you'll see events processed in time order:

- `Event processed in time order: event_0001 (event_time=1755974335641, delay=155s)`
- `Event processed in time order: event_0002 (event_time=1755974539885, delay=41s)`
- Log messages showing: "Using event timestamp: 1755974601885 for event_0015"

### topicNameExtractor

Dynamically determines the target topic for message routing based on record content. This enables intelligent message
distribution, multi-tenancy support, and content-based routing patterns without requiring separate processing pipelines.
The function has access to record context for advanced routing decisions.

#### Parameters

| Parameter     | Type   | Description                             |
|---------------|--------|-----------------------------------------|
| key           | Any    | The key of the record being processed   |
| value         | Any    | The value of the record being processed |
| recordContext | Object | Record metadata and processing context  |

#### Return Value

String representing the topic name to send the message to

#### Example

??? info "Producer - Mixed Sensor Data (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/topicnameextractor-producer.yaml" %}
    ```

??? info "Processor - Dynamic Topic Routing (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/topicnameextractor-processor.yaml" %}
    ```

**What the example does:**

Demonstrates dynamic topic routing based on message content:

* Creates mixed sensor data (temperature, humidity, pressure) with varying alert levels
* Routes messages to different topics based on sensor type and priority
* Prioritizes critical alerts to a dedicated topic regardless of sensor type
* Distributes normal/warning messages to type-specific topics

**Key Technical Features:**

* `topicNameExtractor` function for dynamic topic selection
* Priority-based routing with alert level evaluation
* Fallback topic handling for unknown sensor types
* `toTopicNameExtractor` operation instead of static `to` operation
* Integration with logging for routing visibility

**Expected Results:**

When running this example, you'll see messages routed to different topics:

- Critical alerts: `"Critical alert from sensor sensor_05: pressure reading = 78.26"` → `critical_sensor_alerts` topic
- Normal temperature readings → `temperature_sensors` topic
- Normal humidity readings → `humidity_sensors` topic
- Normal pressure readings → `pressure_sensors` topic
- Unknown sensor types → `unknown_sensor_data` topic

## Other Functions

### generic

Generic custom function that can be used for any purpose. It can accept custom parameters and return any type of value.

#### Parameters

User-defined parameters

#### Return Value

Any value, depending on the function's purpose

#### Example

??? info "Producer - Product Data Generation (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/generic-producer.yaml" %}
    ```

??? info "Processor - Price Calculation with Generic Function (click to expand)"

    ```yaml
    {% include "../definitions/reference/functions/generic-processor.yaml" %}
    ```

**What the example does:**

Demonstrates how to create reusable business logic with generic functions:

* Creates sample product data with base prices and discount rates
* `calculate_price` accepts parameters to compute final pricing with tax
* The generic function is called from within a valueTransformer
* Adds calculated pricing information to product records

**Key Technical Features:**

* `type: generic` for reusable custom functions
* Custom parameter definitions with types (double, string, etc.)
* Return any data structure (JSON objects, arrays, primitives)
* Call generic functions from other functions using standard Python syntax
* Mix generic functions with standard KSML function types

**Expected Results:**

When running these examples, you will see:

- Product data being generated with random base prices and discount rates
- Log messages showing: "Processed product: prod_001 - Original: $856.58, Final: $922.50, Saved: $128.49"
- Each product enriched with detailed pricing calculations including tax
- Generic function providing consistent pricing logic across all products

## Best Practices

1. **Keep functions focused**: Each function should do one thing well
2. **Handle errors gracefully**: Use try/except blocks to prevent pipeline failures
3. **Consider performance**: Python functions introduce some overhead, so keep them efficient
4. **Use appropriate function types**: Choose the right function type for your use case
5. **Leverage state stores**: For complex stateful operations, use state stores rather than global variables
6. **Document your functions**: Add comments to explain complex logic and business rules
7. **Test thoroughly**: Write unit tests for your functions to ensure they behave as expected

## How KSML Functions Relate to Kafka Streams

KSML functions are Python implementations that map directly to Kafka Streams Java interfaces. Understanding this
relationship helps you leverage Kafka Streams documentation and concepts:

### Direct Mappings

| KSML Function Type  | Kafka Streams Interface                        | Purpose                            |
|---------------------|------------------------------------------------|------------------------------------|
| predicate           | `Predicate<K,V>`                               | Filter records based on conditions |
| valueTransformer    | `ValueTransformer<V,VR>` / `ValueMapper<V,VR>` | Transform values                   |
| keyTransformer      | `KeyValueMapper<K,V,KR>`                       | Transform keys                     |
| keyValueTransformer | `KeyValueMapper<K,V,KeyValue<KR,VR>>`          | Transform both key and value       |
| forEach             | `ForeachAction<K,V>`                           | Process records for side effects   |
| aggregator          | `Aggregator<K,V,VA>`                           | Aggregate records incrementally    |
| initializer         | `Initializer<VA>`                              | Provide initial aggregation values |
| reducer             | `Reducer<V>`                                   | Combine values of same type        |
| merger              | `Merger<K,V>`                                  | Merge aggregation results          |
| valueJoiner         | `ValueJoiner<V1,V2,VR>`                        | Join values from two streams       |
| timestampExtractor  | `TimestampExtractor`                           | Extract event time from records    |
| foreignKeyExtractor | `Function<V,FK>`                               | Extract foreign key for joins      |
| topicNameExtractor  | `TopicNameExtractor<K,V>`                      | Dynamic topic routing              |