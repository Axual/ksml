# Pipelines

This page explains the concept of pipelines in KSML, how they work, and best practices for designing effective data processing flows.

## What is a Pipeline?

In KSML, pipelines form the heart of stream processing logic. A pipeline defines how data flows from one or more input streams through a series of operations and finally to one or more output destinations.

Pipelines connect the dots between:
- **Sources**: Where data comes from (Kafka topics via streams, tables, or global tables)
- **Operations**: What happens to the data (transformations, filters, aggregations, etc.)
- **Sinks**: Where the processed data goes (output topics, other pipelines, functions, etc.)

Think of a pipeline as a recipe that describes exactly how data should be processed from start to finish.

## Pipeline Structure

### Basic Structure

Every KSML pipeline has a consistent structure with three main components:

```yaml
pipelines:
  my_pipeline_name:
    from: input_stream_name    # Source
    via:                       # Operations (optional)
      - type: operation1
        # operation parameters
      - type: operation2
        # operation parameters
    to: output_stream_name     # Sink
```

This structure makes pipelines easy to read and understand, with a clear flow from source to operations to sink.

### Sources: Where Data Comes From

The source of a pipeline is specified with the `from` keyword. It defines where the pipeline gets its data from:

```yaml
from: user_clicks_stream
```

A source can be:
- A **stream** (KStream): For event-based processing
- A **table** (KTable): For state-based processing
- A **globalTable** (GlobalKTable): For reference data
- Another **pipeline**: For chaining pipelines together

Sources must be defined elsewhere in your KSML file (in the `streams`, `tables`, or `globalTables` sections) or be the result of a previous pipeline.

### Operations: Transforming the Data

Operations are defined in the `via` section as a list of transformations to apply to the data:

```yaml
via:
  - type: filter
    if:
      expression: value.get('amount') > 100
  - type: mapValues
    mapper:
      expression: {"user": key, "amount": value.get('amount'), "currency": "USD"}
```

Each operation:
- Has a `type` that defines what it does
- Takes parameters specific to that operation type
- Receives data from the previous operation (or the source)
- Passes its output to the next operation (or the sink)

Operations are applied in sequence, creating a processing pipeline where data flows from one operation to the next.

### Sinks: Where Data Goes

The sink defines where the processed data should be sent. KSML supports several sink types:

```yaml
to: processed_orders_stream  # Send to a predefined stream
```

Or more complex sinks:

```yaml
branch:  # Split the pipeline based on conditions
  - if:
      expression: value.get('type') == 'purchase'
    to: purchases_stream
  - if:
      expression: value.get('type') == 'refund'
    to: refunds_stream
  - to: other_events_stream  # Default branch
```

Common sink types include:
- `to`: Send to a predefined stream, table, or topic
- `as`: Save the result under a name for use in later pipelines
- `branch`: Split the pipeline based on conditions
- `forEach`: Process each message with a function (terminal operation)
- `print`: Output messages for debugging
- `toTopicNameExtractor`: Dynamically determine the output topic

## Pipeline Patterns and Techniques

### Connecting Pipelines

KSML allows you to connect pipelines together, creating more complex processing flows:

```yaml
pipelines:
  filter_high_value_orders:
    from: orders_stream
    via:
      - type: filter
        if:
          expression: value.get('total') > 1000
    as: high_value_orders  # Save the result for use in another pipeline

  process_high_value_orders:
    from: high_value_orders  # Use the result from the previous pipeline
    via:
      - type: mapValues
        mapper:
          expression: {"orderId": value.get('id'), "amount": value.get('total'), "priority": "high"}
    to: priority_orders_stream
```

This approach allows you to:
- Break complex logic into smaller, more manageable pieces
- Reuse intermediate results in multiple downstream pipelines
- Create cleaner, more maintainable code

### Branching Pipelines

The `branch` sink allows you to split a pipeline based on conditions:

```yaml
pipelines:
  route_messages_by_type:
    from: events_stream
    branch:
      - if:
          expression: value.get('type') == 'click'
        to: clicks_stream
      - if:
          expression: value.get('type') == 'purchase'
        to: purchases_stream
      - if:
          expression: value.get('type') == 'login'
        to: logins_stream
      - to: other_events_stream  # Default branch for anything else
```

This is useful for:
- Routing different types of events to different destinations
- Implementing content-based routing patterns
- Creating specialized processing flows for different data types

### Dynamic Output Topics

The `toTopicNameExtractor` sink allows you to dynamically determine the output topic:

```yaml
pipelines:
  route_to_dynamic_topics:
    from: events_stream
    toTopicNameExtractor:
      expression: "events-" + value.get('category').lower()
```

This is useful for:
- Partitioning data across multiple topics
- Creating topic hierarchies
- Implementing multi-tenant architectures

## Input and Output Configurations

### Configuring Input Streams

When defining input streams, you can configure various properties:

```yaml
streams:
  orders_stream:
    topic: orders
    keyType: string
    valueType: json
    offsetResetPolicy: earliest  # Start from the beginning of the topic
    timestampExtractor: order_timestamp_extractor  # Custom timestamp extraction
```

Important configuration options include:
- `offsetResetPolicy`: Determines where to start consuming from when no offset is stored
- `timestampExtractor`: Defines how to extract event timestamps from messages
- `keyType` and `valueType`: Define the data types and formats

### Configuring Output Streams

Similarly, output streams can be configured:

```yaml
streams:
  processed_orders_stream:
    topic: processed-orders
    keyType: string
    valueType: avro:ProcessedOrder
```

For inline topic definitions in sinks:

```yaml
to:
  topic: dynamic-output-topic
  keyType: string
  valueType: json
```

## Best Practices for Pipeline Design

### 1. Keep Pipelines Focused

Each pipeline should have a clear, single responsibility:

```yaml
# Good: Focused pipeline
pipelines:
  enrich_user_events:
    from: raw_user_events
    via:
      - type: join
        with: user_profiles
        valueJoiner:
          expression: {"event": value1, "user": value2}
    to: enriched_user_events
```

```yaml
# Avoid: Pipeline doing too many things
pipelines:
  do_everything:
    from: raw_events
    via:
      - type: filter
        # Filter logic
      - type: join
        # Join logic
      - type: aggregate
        # Aggregation logic
      - type: mapValues
        # Transformation logic
    to: final_output
```

### 2. Use Meaningful Names

Choose descriptive names for pipelines and intermediate results:

```yaml
# Good: Clear naming
pipelines:
  filter_active_users:
    from: user_events
    via:
      - type: filter
        if:
          expression: value.get('status') == 'active'
    as: active_user_events
```

```yaml
# Avoid: Vague naming
pipelines:
  pipeline1:
    from: stream1
    via:
      - type: filter
        if:
          expression: value.get('x') == 'y'
    as: filtered_data
```

### 3. Chain Pipelines for Complex Logic

Break complex processing into multiple pipelines:

```yaml
pipelines:
  # Step 1: Filter and enrich
  prepare_data:
    from: raw_data
    via:
      - type: filter
        # Filter logic
      - type: join
        # Enrichment logic
    as: prepared_data

  # Step 2: Transform
  transform_data:
    from: prepared_data
    via:
      - type: mapValues
        # Transformation logic
    as: transformed_data

  # Step 3: Aggregate
  aggregate_data:
    from: transformed_data
    via:
      - type: groupByKey
      - type: aggregate
        # Aggregation logic
    to: aggregated_results
```

### 4. Use Comments for Complex Logic

Add comments to explain complex operations:

```yaml
pipelines:
  process_transactions:
    from: transactions
    via:
      # Filter out test transactions
      - type: filter
        if:
          expression: not value.get('isTest', False)

      # Convert amounts to standard currency
      - type: mapValues
        mapper:
          functionRef: convert_to_usd

      # Group by user for aggregation
      - type: groupBy
        keySelector:
          expression: value.get('userId')
    to: processed_transactions
```

### 5. Consider Performance Implications

Be mindful of operations that can impact performance:

```yaml
# Potentially expensive: Joining large streams without proper keying
pipelines:
  expensive_join:
    from: large_stream_1
    via:
      - type: join
        with: large_stream_2
        # Missing proper key selection
    to: joined_output

# Better: Ensure proper keying for efficient joins
pipelines:
  efficient_join:
    from: large_stream_1
    via:
      - type: selectKey
        keySelector:
          expression: value.get('joinKey')
      - type: join
        with: large_stream_2
        # Now the streams are properly keyed
    to: joined_output
```

## Duration Specification

Some pipeline operations require specifying time durations. In KSML, durations are expressed using a simple format:

```
###x
```

Where:
- `###` is a positive number
- `x` is an optional time unit:
  - (none): milliseconds
  - `s`: seconds
  - `m`: minutes
  - `h`: hours
  - `d`: days
  - `w`: weeks

Examples:
```
100   # 100 milliseconds
30s   # 30 seconds
5m    # 5 minutes
2h    # 2 hours
1d    # 1 day
4w    # 4 weeks
```

Durations are commonly used in windowing operations:

```yaml
- type: windowedBy
  timeWindows:
    size: 5m        # 5-minute windows
    advanceBy: 1m   # Advancing every minute (sliding window)
```

## Examples

### Example 1: Simple Filtering and Transformation

```yaml
pipelines:
  process_sensor_readings:
    from: raw_sensor_readings
    via:
      # Filter out readings with errors
      - type: filter
        if:
          expression: value.get('error') is None

      # Convert temperature from Celsius to Fahrenheit
      - type: mapValues
        mapper:
          expression: {
            "sensorId": value.get('sensorId'),
            "timestamp": value.get('timestamp'),
            "tempF": value.get('tempC') * 9/5 + 32,
            "humidity": value.get('humidity')
          }

      # Add a timestamp for when we processed the reading
      - type: mapValues
        mapper:
          expression: dict(list(value.items()) + [("processedAt", time.time())])
    to: processed_sensor_readings
```

### Example 2: Joining Streams

```yaml
pipelines:
  enrich_orders:
    from: orders
    via:
      # Join with customer information
      - type: join
        with: customers
        valueJoiner:
          expression: {
            "orderId": value1.get('orderId'),
            "products": value1.get('products'),
            "total": value1.get('total'),
            "customer": {
              "id": key,
              "name": value2.get('name'),
              "email": value2.get('email')
            }
          }

      # Join with shipping information
      - type: join
        with: shipping_info
        valueJoiner:
          expression: dict(list(value1.items()) + [("shipping", value2)])
    to: enriched_orders
```

### Example 3: Aggregation

```yaml
pipelines:
  calculate_sales_by_category:
    from: sales
    via:
      # Group by product category
      - type: groupBy
        keySelector:
          expression: value.get('category')

      # Sum the sales amounts
      - type: aggregate
        initializer:
          expression: {"count": 0, "total": 0.0}
        aggregator:
          expression: {
            "count": aggregate.get('count') + 1,
            "total": aggregate.get('total') + value.get('amount')
          }

      # Format the output
      - type: mapValues
        mapper:
          expression: {
            "category": key,
            "count": value.get('count'),
            "total": value.get('total'),
            "average": value.get('total') / value.get('count') if value.get('count') > 0 else 0
          }
    to: sales_by_category
```

## Conclusion

Pipelines are the core building blocks of KSML applications. By understanding how to design and connect pipelines effectively, you can create powerful stream processing applications that are both maintainable and efficient.

For more detailed information about specific operations that can be used in pipelines, see the [Operations](operations.md) page. To learn about functions that can be used within pipeline operations, see the [Functions](functions.md) page.
