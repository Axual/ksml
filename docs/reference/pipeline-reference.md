# Pipeline Reference

This document provides a comprehensive reference for pipelines in KSML. It covers pipeline structure, configuration options, and best practices.

## What is a Pipeline?

In KSML, pipelines form the heart of stream processing logic. A pipeline defines how data flows from one or more input streams through a series of operations and finally to one or more output destinations.

Pipelines connect:
- **Sources**: Where data comes from (Kafka topics via streams, tables, or global tables)
- **Operations**: What happens to the data (transformations, filters, aggregations, etc.)
- **Sinks**: Where the processed data goes (output topics, other pipelines, functions, etc.)

## Pipeline Structure

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

## Pipeline Components

### Sources

The source of a pipeline is specified with the `from` keyword. It defines where the pipeline gets its data from:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `from` | String or Array | Yes | The source stream, table, or pipeline name(s) |

A source can be:
- A **stream** (KStream): For event-based processing
- A **table** (KTable): For state-based processing
- A **globalTable** (GlobalKTable): For reference data
- Another **pipeline**: For chaining pipelines together

Sources must be defined elsewhere in your KSML file (in the `streams`, `tables`, or `globalTables` sections) or be the result of a previous pipeline.

#### Example

```yaml
# Single source
from: user_clicks_stream
```

```yaml
# Multiple sources (for joins)
from:
  - orders_stream
  - customers_stream
```

### Operations

Operations are defined in the `via` section as a list of transformations to apply to the data:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `via` | Array | No | List of operations to apply to the data |

Each operation:
- Has a `type` that defines what it does
- Takes parameters specific to that operation type
- Receives data from the previous operation (or the source)
- Passes its output to the next operation (or the sink)

Operations are applied in sequence, creating a processing pipeline where data flows from one operation to the next.

For a complete list of available operations, see the [Operations Reference](operation-reference.md).

#### Example

```yaml
via:
  - type: filter
    if:
      expression: value.get('amount') > 100
  - type: mapValues
    mapper:
      expression: {"user": key, "amount": value.get('amount'), "currency": "USD"}
```

### Sinks

The sink defines where the processed data should be sent:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `to` | String | No* | The destination stream or topic name |
| `as` | String | No* | Name to save the result for use in later pipelines |
| `branch` | Array | No* | Split the pipeline based on conditions |
| `forEach` | Object | No* | Process each message with a function (terminal operation) |
| `print` | Boolean | No* | Output messages for debugging |
| `toTopicNameExtractor` | Object | No* | Dynamically determine the output topic |

*At least one sink type is required.

#### Example: Simple Sink

```yaml
to: processed_orders_stream
```

#### Example: Branch Sink

```yaml
branch:
  - if:
      expression: value.get('type') == 'purchase'
    to: purchases_stream
  - if:
      expression: value.get('type') == 'refund'
    to: refunds_stream
  - to: other_events_stream  # Default branch
```

#### Example: Dynamic Topic Sink

```yaml
toTopicNameExtractor:
  expression: "events-" + value.get('category').lower()
```

## Pipeline Patterns

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

## Best Practices

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
    to: sales_by_category
```

## Related Topics

- [KSML Language Reference](language-reference.md)
- [Operations Reference](operation-reference.md)
- [Functions Reference](function-reference.md)
- [State Store Reference](state-store-reference.md)
