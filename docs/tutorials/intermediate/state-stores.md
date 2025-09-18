# State Stores

State stores are persistent or in-memory storage components used by stateful operations in Kafka Streams to maintain intermediate results, aggregations, and other stateful data. KSML provides flexible configuration options for state stores, allowing you to optimize performance, durability, and storage characteristics for your specific use cases.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_ownership_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic owner_sensor_counts && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_type_totals && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic owner_counts && \
    ```

## Understanding State Stores in Kafka Streams

State stores serve several critical purposes in stream processing:

- **Aggregations**: Store running totals, counts, averages, and other aggregate calculations
- **Windowing**: Maintain data within time windows for temporal analysis
- **Joins**: Cache data from one stream to join with another
- **Deduplication**: Track processed records to eliminate duplicates
- **State Management**: Persist application state across restarts

State stores can be backed by RocksDB (persistent) or kept in memory (non-persistent), with optional changelog topics for fault tolerance.

## State Store Types

KSML supports three types of state stores, each optimized for specific use cases:

| Type | Description | Use Cases | Examples                                                                                                  |
|------|-------------|-----------|-----------------------------------------------------------------------------------------------------------|
| `keyValue` | Simple key-value storage | General lookups, non-windowed aggregations, manual state management | [`keyValue` type state store](#1-predefined-store-configuration)                                 |
| `window` | Time-windowed storage with automatic expiry | Time-based aggregations, windowed joins, temporal analytics | [`window` type state store](aggregations.md#windowed-aggregation-example)       |
| `session` | Session-based storage with activity gaps | User session tracking, activity-based grouping | [`session` type state store](windowing.md#session-window-user-activity-analysis) |

## Configuration Methods

KSML provides two ways to configure state stores:

### 1. Predefined Store Configuration

Define stores in the global `stores` section and reference them by name:

??? info "Sensor Ownership Producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/state-stores/producer-sensor-ownership.yaml"
    %}
    ```

??? info "Predefined Store Processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/state-stores/processor-predefined-store.yaml"
    %}
    ```

This example demonstrates:

- **Predefined store configuration**: The `owner_count_store` is defined in the global `stores` section
- **Store reference**: The `count` operation references the predefined store by name
- **Persistent storage**: Data survives application restarts (`persistent: true`)
- **Changelog logging**: State changes are logged for fault tolerance (`logging: true`)

You won't be able to read the output in `owner_sensor_counts` topic because it is binary, to read it you can consume the messages like this:

   ```bash
   # Read owner sensor counts (Long values require specific deserializer)
   docker exec broker /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server broker:9093 \
     --topic owner_sensor_counts \
     --from-beginning \
     --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
     --value-deserializer org.apache.kafka.common.serialization.LongDeserializer \
     --property print.key=true \
     --property key.separator=": " \
     --timeout-ms 10000
   ```

### 2. Inline Store Configuration

Define stores directly within operations for single-use scenarios:

??? info "Sensor Ownership Producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/state-stores/producer-sensor-ownership.yaml"
    %}
    ```

??? info "Inline Store Processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/state-stores/processor-inline-store.yaml"
    %}
    ```

This example demonstrates:

- **Inline store configuration**: Store defined directly within the `aggregate` operation
- **Custom aggregation**: Uses initializer and aggregator functions for complex calculations
- **Memory-only storage**: Non-persistent storage for temporary calculations (`persistent: false`)
- **Caching enabled**: Improves performance by batching state updates (`caching: true`)

## Configuration Parameters

| Parameter     | Type      | Default | Description |
|:-------------|:----------|:--------|:------------|
| `name`       | `string`  | _none_  | Unique identifier for the state store |
| `type`       | `string`  | _none_  | Store type: `keyValue`, `window`, or `session` |
| `keyType`    | `string`  | _none_  | Data type for keys (e.g., `string`, `long`, `json`) |
| `valueType`  | `string`  | _none_  | Data type for values (e.g., `long`, `json`, `avro:Schema`) |
| `persistent` | `boolean` | `false` | Whether to persist data to disk using RocksDB |
| `retention`  | `duration` | _none_  | How long to retain data (e.g., `5m`, `1h`, `1d`) |
| `caching`    | `boolean` | `false` | Enable caching to batch state updates |
| `logging`    | `boolean` | `false` | Enable changelog topic for fault tolerance |
| `timestamped`| `boolean` | `false` | Include timestamps with values (keyValue/window stores) |
| `versioned`  | `boolean` | `false` | Enable versioning for keyValue stores |

### Window Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `windowSize` | Duration | Yes | - | Size of the time window (must match operation's window duration) |
| `retention` | Duration | No | - | How long to retain window data (should be > windowSize + grace period) |
| `retainDuplicates` | Boolean | No | `false` | Whether to keep duplicate entries in windows |

### KeyValue Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `versioned` | Boolean | No | `false` | If `true`, maintains version history of values |
| `historyRetention` | Duration | No (Yes if versioned) | - | How long to keep old versions |
| `segmentInterval` | Duration | No | - | Segment size for versioned stores |

**Important:** Versioned stores (`versioned: true`) cannot have caching enabled (`caching: false` is required).

### Session Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `retention` | Duration | No | - | How long to retain session data |


### Retention and Cleanup

State stores can automatically clean up old data based on retention policies:

- **Time-based retention**: Data older than the specified duration is removed
- **Window stores**: Automatically expire data outside the window + grace period
- **Session stores**: Remove sessions after inactivity timeout

### Performance Considerations

**Caching (`caching: true`)**:

- **Benefits**: Reduces downstream message volume, improves throughput
- **Trade-offs**: Increases memory usage, adds latency to state updates
- **Use when**: You can tolerate slightly delayed updates for better performance

**Persistence (`persistent: true`)**:

- **Benefits**: Data survives restarts, enables exactly-once processing
- **Trade-offs**: Slower startup, additional disk I/O overhead
- **Use when**: Data durability is important, or you need exactly-once guarantees

**Changelog Logging (`logging: true`)**:

- **Benefits**: Enables fault tolerance and exactly-once processing
- **Trade-offs**: Additional Kafka topics, increased network and storage usage
- **Use when**: You need fault tolerance or are using exactly-once semantics

## Best Practices

### When to Use Predefined Stores
- Multiple operations need to access the same store
- Store configuration is complex or frequently reused
- You want centralized store management for maintainability

### When to Use Inline Stores
- Store is used by a single operation
- Simple, one-off configurations
- Prototyping or small applications

### Naming Conventions
- Use descriptive names that indicate the store's purpose
- Include the data type or domain in the name (e.g., `user_session_store`, `product_inventory`)
- Be consistent across your application

### Memory Management
- Set appropriate retention periods to prevent unbounded growth
- Use non-persistent stores for temporary calculations
- Monitor memory usage, especially with caching enabled

### Fault Tolerance
- Enable logging for critical business data
- Use persistent stores for data that must survive restarts
- Consider the trade-off between durability and performance

State stores are a powerful feature in KSML that enable sophisticated stateful stream processing patterns. By understanding the configuration options and trade-offs, you can build robust and efficient streaming applications.

## Next Steps

Ready to explore more advanced state store patterns? Continue with:

- **[Custom State Stores Tutorial](../advanced/custom-state-stores.md)** - Learn advanced patterns including window stores, session stores, multi-store coordination, and optimization techniques for production applications