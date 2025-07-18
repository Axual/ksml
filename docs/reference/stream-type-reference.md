# KSML Stream Type Reference

This page explains the fundamental concepts of streams and data types in KSML, providing a foundation for understanding
how data flows through your KSML applications.

## Understanding Streams in KSML

In KSML, streams represent continuous flows of data. Every KSML definition file contains a list of declared streams that
connect to Kafka topics. These streams are the entry and exit points for your data processing pipelines.

### Types of Streams

KSML supports three types of streams, each with different characteristics and use cases:

#### KStream

A KStream represents an unbounded sequence of records. Each record in a KStream is an independent entity or event.

**Key characteristics:**

- Records are immutable (once created, they cannot be changed)
- New records can be added to the stream at any time
- Records are processed one at a time in the order they arrive
- Ideal for event-based processing

**Example use cases:**

- Processing individual user actions (clicks, purchases, etc.)
- Handling sensor readings or log entries
- Processing transactions

**KSML definition:**

```yaml
streams:
  user_clicks_stream:
    topic: user-clicks
    keyType: string
    valueType: json
```

#### KTable

A KTable represents a changelog stream from a primary-keyed table. Each record in a KTable is an update to a key in the
table.

**Key characteristics:**

- Records with the same key represent updates to the same entity
- The latest record for a key represents the current state
- Records are compacted (only the latest value for each key is retained)
- Ideal for state-based processing

**Example use cases:**

- Maintaining user profiles or preferences
- Tracking current inventory levels
- Storing configuration settings

**KSML definition:**

```yaml
tables:
  user_profiles_table:
    topic: user-profiles
    keyType: string
    valueType: avro:UserProfile
    store: user_profiles_store
```

#### GlobalKTable

A GlobalKTable is similar to a KTable, but with one key difference: it's fully replicated on each instance of your
application.

**Key characteristics:**

- Contains the same data as a KTable
- Fully replicated on each instance (not partitioned)
- Allows joins without requiring co-partitioning
- Ideal for reference data that needs to be available on all instances

**Example use cases:**

- Reference data (product catalogs, country codes, etc.)
- Configuration settings needed by all instances
- Small to medium-sized datasets that change infrequently

**KSML definition:**

```yaml
globalTables:
  product_catalog:
    topic: product-catalog
    keyType: string
    valueType: avro:Product
    store: product_catalog_store
```

### Optional settings for all stream types

Each stream type allows the following additional settings to be specified:

- **offsetResetPolicy**: the policy to use when there is no (valid) consumer group offset in Kafka. Choice of `earliest`,
  `latest`, `none`, or `by_duration:<duration>`. In the latter case, you can pass in a custom duration.
- **timestampExtractor**: a function that is able to extract a timestamp from a consumed message, to be used by Kafka
  Streams as the message timestamp in all pipeline processing.
- **partitioner**: a stream partitioner that determines to which topic partitions an output record needs to be written.

### Choosing the Right Stream Type

The choice between KStream, KTable, and GlobalKTable depends on your specific use case:

| If you need to...                                         | Consider using... |
|-----------------------------------------------------------|-------------------|
| Process individual events as they occur                   | KStream           |
| Maintain the latest state of entities                     | KTable            |
| Join with data that's needed across all partitions        | GlobalKTable      |
| Process time-ordered events                               | KStream           |
| Track changes to state over time                          | KTable            |
| Access reference data without worrying about partitioning | GlobalKTable      |

## Best Practices

Here are some best practices for working with streams and data types in KSML:

1. **Choose the right stream type for your use case**:
   - Use KStream for event processing
   - Use KTable for state tracking
   - Use GlobalKTable for reference data

2. **Use appropriate data types**:
   - Be specific about your types (avoid using `any` when possible)
   - Use complex types (struct, list, etc.) to represent structured data
   - Consider using windowed types for time-based aggregations

3. **Select the right notation**:
   - Use AVRO for production systems with schema evolution
   - Use JSON for development and debugging
   - Consider compatibility with upstream and downstream systems

4. **Manage schemas effectively**:
   - Use a schema registry for AVRO schemas in production
   - Keep schemas under version control
   - Plan for schema evolution

5. **Consider performance implications**:
   - GlobalKTables replicate data to all instances, which can impact memory usage
   - Complex types and notations may have serialization/deserialization overhead
   - Large schemas can impact performance

## Examples

### Example 1: Event Processing with KStream

```yaml
streams:
  page_views:
    topic: page-views
    keyType: string  # User ID
    valueType: json  # Page view event
    offsetResetPolicy: earliest

pipelines:
  process_page_views:
    from: page_views
    via:
      - type: filter
        if:
          expression: value.get('duration') > 10  # Only process views longer than 10 seconds
      # Additional processing steps...
    to: processed_page_views
```

### Example 2: User Profile Management with KTable

```yaml
tables:
  user_profiles:
    topic: user-profiles
    keyType: string  # User ID
    valueType: avro:UserProfile
    store: user_profiles_store

pipelines:
  update_user_preferences:
    from: user_preference_updates
    via:
      - type: join
        with: user_profiles
        valueJoiner:
          expression: { "userId": key, "name": value2.get('name'), "preferences": value1 }
          resultType: struct
      # Additional processing steps...
    to: updated_user_profiles
```

### Example 3: Product Catalog as GlobalKTable

```yaml
globalTables:
  product_catalog:
    topic: product-catalog
    keyType: string  # Product ID
    valueType: avro:Product

pipelines:
  enrich_orders:
    from: orders
    via:
      - type: join
        with: product_catalog
        mapper:
          expression: value.get('productId')  # Map from order to product ID
          resultType: string
        valueJoiner:
          expression: { "orderId": value1.get('orderId'), "product": value2, "quantity": value1.get('quantity') }
          resultType: struct
      # Additional processing steps...
    to: enriched_orders
```

## Conclusion

Understanding streams and data types is fundamental to building effective KSML applications. By choosing the right
stream types, data types, and notations for your use case, you can create efficient, maintainable, and scalable stream
processing pipelines.

For more detailed information, refer to the [KSML Language Reference](language-reference.md)
and [Data Types Reference](data-types-reference.md).
