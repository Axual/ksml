# Operations

Learn about the various operations you can perform on your data streams in KSML applications.

## What are Operations in KSML?

Operations are the building blocks of stream processing in KSML. They define how data is transformed, filtered, aggregated, and otherwise processed as it flows through your application. Operations form the middle part of pipelines, taking input from the previous operation and producing output for the next operation.

Understanding the different types of operations and when to use them is crucial for building effective stream processing applications.

## Types of Operations

KSML operations can be broadly categorized into several types:

### Stateless Operations

These operations process each message independently, without maintaining state between messages:

- **Map Operations**: Transform individual messages (e.g., `map`, `mapValues`, `mapKey`)
- **Filter Operations**: Include or exclude messages based on conditions (e.g., `filter`, `filterNot`)
- **Conversion Operations**: Change the format or structure of messages (e.g., `convertKey`, `convertValue`)

Stateless operations are typically simpler and more efficient, as they don't require state storage.

### Stateful Operations

These operations maintain state across multiple messages, allowing for more complex processing:

- **Aggregation Operations**: Combine multiple messages into a single result (e.g., `aggregate`, `count`, `reduce`)
- **Join Operations**: Combine data from multiple streams (e.g., `join`, `leftJoin`, `outerJoin`)
- **Windowing Operations**: Group and process data within time windows (e.g., `windowByTime`, `windowBySession`)

Stateful operations require state stores to maintain their state, which has implications for performance and resource usage.

### Grouping Operations

These operations reorganize messages based on keys:

- **Group By Operations**: Group messages by a specific key (e.g., `groupBy`, `groupByKey`)
- **Repartition Operations**: Change how messages are distributed across partitions (e.g., `repartition`)

Grouping operations often precede aggregation operations, as they organize data in a way that makes aggregation possible.

### Sink Operations

These operations represent the end of a pipeline, where data is sent to an external system or another part of your application:

- **Output Operations**: Send data to Kafka topics (e.g., `to`, `toTopicNameExtractor`)
- **Terminal Operations**: Process data without producing further output (e.g., `forEach`, `print`)
- **Branching Operations**: Split a stream into multiple streams based on conditions (e.g., `branch`)

## Common Operations and Their Uses

### Transforming Data

- **map**: Transform both key and value of messages
- **mapValues**: Transform only the value of messages (preserves key)
- **flatMap**: Transform a message into multiple output messages

### Filtering Data

- **filter**: Include messages that match a condition
- **filterNot**: Exclude messages that match a condition

### Aggregating Data

- **aggregate**: Build custom aggregations using an initializer and aggregator function
- **count**: Count the number of messages with the same key
- **reduce**: Combine messages with the same key using a reducer function

### Joining Streams

- **join**: Inner join of two streams
- **leftJoin**: Left join of two streams
- **outerJoin**: Full outer join of two streams

### Working with Windows

- **windowByTime**: Group messages into time-based windows
- **windowBySession**: Group messages into session-based windows

## Choosing the Right Operation

When designing your KSML application, consider these factors when choosing operations:

- **State Requirements**: Stateful operations require more resources
- **Performance Impact**: Some operations are more computationally expensive than others
- **Ordering Guarantees**: Some operations may affect message ordering
- **Parallelism**: Some operations affect how data is partitioned and processed in parallel

## Examples

### Transforming Messages

```yaml
pipelines:
  transform_temperatures:
    from: temperature_readings
    mapValues: celsius_to_fahrenheit
    to: fahrenheit_temperatures
```

### Filtering and Branching

```yaml
pipelines:
  filter_by_region:
    from: sensor_readings
    branch:
      - predicate: is_north_america
        to: north_america_readings
      - predicate: is_europe
        to: europe_readings
      - to: other_regions_readings
```

### Aggregating Data

```yaml
pipelines:
  calculate_averages:
    from: sales_data
    groupByKey:
    aggregate:
      initializer: initialize_sales_aggregation
      aggregator: aggregate_sales
    to: sales_summaries
```

## Related Topics

- [Streams and Data Types](../reference/stream-types-reference.md): Understand the data types that operations work with
- [Functions](functions.md): Learn about the functions that power many operations
- [Pipelines](pipelines.md): See how operations fit into the overall pipeline structure

By understanding the different operations available in KSML and when to use them, you can build powerful stream processing applications that efficiently transform, filter, and aggregate your data.