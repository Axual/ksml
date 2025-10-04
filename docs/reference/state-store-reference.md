# State Store Reference

State stores enable stateful stream processing in KSML by maintaining data across multiple messages. This reference covers state store configuration, usage patterns, and best practices.

For hands-on tutorials, please check out:

- [State Stores Intermediate Tutorial](../tutorials/intermediate/state-stores.md)
- [Custom State Stores Advanced Tutorial](../tutorials/advanced/custom-state-stores.md) 

## What are State Stores?

State stores allow KSML applications to:

- Maintain state between messages for aggregations, counts, and joins
- Track historical information for context-aware processing  
- Build complex business logic that depends on previous events

## State Store Types

KSML supports three types of state stores, each optimized for specific use cases:

| Type | Description | Use Cases | Examples                                                                                                          |
|------|-------------|-----------|-------------------------------------------------------------------------------------------------------------------|
| `keyValue` | Simple key-value storage | General lookups, non-windowed aggregations, manual state management | [`keyValue` type state store](../tutorials/intermediate/state-stores.md#1-predefined-store-configuration) |
| `window` | Time-windowed storage with automatic expiry | Time-based aggregations, windowed joins, temporal analytics | [`window` type state store](../tutorials/intermediate/aggregations.md#windowed-aggregation-example)               |
| `session` | Session-based storage with activity gaps | User session tracking, activity-based grouping | [`session` type state store](../tutorials/intermediate/windowing.md#session-window-user-activity-analysis)                                             |

## Configuration Methods

### 1. Pre-defined State Stores

Define once in the `stores` section, reuse across operations:

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml:17:26"
```

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml:17:26"
```

**Full example:**

- [Pre-defined State Stores Tutorial](../tutorials/intermediate/state-stores.md#1-predefined-store-configuration)

### 2. Inline State Stores

Define directly within operations for custom configurations:

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-inline-store.yaml:58:67"
```

**Full example:**

- [Inline State Stores Tutorial](../tutorials/intermediate/state-stores.md#2-inline-store-configuration)

## Configuration Parameters

### Common Parameters (All Store Types)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | String | No | Operation name | Unique identifier for the state store |
| `type` | String | Yes | - | Store type: `keyValue`, `window`, or `session` |
| `keyType` | String | Yes | - | Data type for keys (see [Data Types Reference](data-and-formats-reference.md)) |
| `valueType` | String | Yes | - | Data type for values |
| `persistent` | Boolean | No | `false` | If `true`, uses RocksDB (disk); if `false`, uses in-memory storage |
| `caching` | Boolean | No | `false` | If `true`, improves read performance but delays updates |
| `logging` | Boolean | No | `false` | If `true`, creates changelog topic for fault tolerance (in addition to local storage) |
| `timestamped` | Boolean | No | `false` | If `true`, stores timestamp with each entry |

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

## Usage in Operations

### With Aggregations

State stores are automatically used by aggregation operations:

```yaml
pipelines:
  calculate_statistics:
    from: payment_stream
    via:
      - type: groupByKey
      - type: aggregate
        store:
          type: keyValue
          retention: 1h
          caching: false
```

**Full example:**

- [Aggregations with State Stores](../tutorials/intermediate/aggregations.md#complex-aggregate-json-format)

### With Manual State Access

Access state stores directly in functions for custom caching, enrichment, and state management:

```yaml
stores:
  user_profile_cache:
    name: user_profile_cache
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    logging: true
    retention: 1h
```


```yaml
functions:
  enrich_and_cache:
    type: valueTransformer
    code: |
      # Get cached data for this user
      cached_data = user_profile_cache.get(key)
```

**Full example**

??? info "State Store with Manual State Access (click to expand)"

    ```yaml
    {%
      include "../../ksml/src/test/resources/docs-examples/intermediate-tutorial/state-stores/producer-user-events.yaml"
    %}
    ```

??? info "State Store with Manual State Access (click to expand)"

    ```yaml
    {%
      include "../definitions/intermediate-tutorial/state-stores/processor-manual-store.yaml"
    %}
    ```

This example demonstrates:

- **Store declaration**: List store names in the function's `stores` parameter
- **State retrieval**: Use `store_name.get(key)` to read cached data
- **State storage**: Use `store_name.put(key, value)` to update cache
- **Null handling**: Check if `cached_data is not None` for first-time detection

**Key functions:**

- `store.get(key)` - Retrieve value from state store (returns None if not found)
- `store.put(key, value)` - Store key-value pair in state store

### With Session Operations

Session stores track user activity with inactivity gaps and automatic timeout:
```yaml
pipelines:
  user_session_analysis:
    from: user_clicks
    via:
      - type: groupByKey
      - type: windowBySession
        inactivityGap: 2m  # Close session after 2 minutes of inactivity
        grace: 30s
      - type: count
        store:
          name: user_sessions
          type: session
          retention: 1h
          caching: false
```

**Full example:**

- [Session store type example](../tutorials/intermediate/windowing.md#session-window-user-activity-analysis)

**Session store patterns:**

- **Activity tracking**: Monitor user engagement and session duration
- **Timeout detection**: Identify inactive sessions for cleanup
- **State aggregation**: Accumulate metrics within session boundaries

### With Windowed Operations

Window stores require specific configuration that varies by operation type:

#### For Aggregations (Count, Aggregate, etc.)

```yaml
pipelines:
  count_clicks_5min:
    from: user_clicks
    via:
      - type: groupByKey
      - type: windowByTime
        windowType: tumbling
        duration: 5m
        grace: 30s
      - type: count
        store:
          name: click_counts_5min
          type: window
          windowSize: 5m          # Must match window duration
          retention: 35m          # windowSize + grace + buffer
          caching: false
```

#### For Stream-Stream Joins

```yaml
pipelines:
  join_streams:
    from: stream1
    via:
      - type: join
        stream: stream2
        timeDifference: 15m
        grace: 5m
        thisStore:
          type: window
          windowSize: 30m         # Must be 2 × timeDifference
          retention: 35m          # 2 × timeDifference + grace
          retainDuplicates: true
```

### Retention Guidelines

**For Window Aggregations:**
```
retention = windowSize + gracePeriod + processingBuffer
```

**For Stream-Stream Joins:**
```
windowSize = 2 × timeDifference
retention = 2 × timeDifference + gracePeriod
```

**Full examples:**

- [Windowed Aggregations](../tutorials/intermediate/windowing.md#tumbling-window-click-counting)
- [Stream-Stream Joins](../tutorials/intermediate/joins.md#stream-stream-join)

## Storage Architecture

### Local Storage vs Changelog Topics

It's important to understand that `persistent` and `logging` control different aspects of state storage:

1. **`persistent: true`** → Uses RocksDB for local storage (survives process restarts)
2. **`persistent: false`** → Uses in-memory storage (lost on process restart)
3. **`logging: true`** → ADDITIONALLY replicates state to a Kafka changelog topic
4. **`logging: false`** → No changelog topic (only local storage)

**Key Point**: When `logging: true`, state is stored **BOTH** locally (RocksDB or memory) **AND** in the changelog topic. The changelog is for fault tolerance and recovery, not primary storage.

### Common Configuration Patterns

| Configuration | Description | Use Case |
|--------------|-------------|----------|
| `persistent: true, logging: false` | RocksDB only (local durability, no fault tolerance) | Development, non-critical state |
| `persistent: true, logging: true` | RocksDB + changelog (local durability + fault tolerance) | **Production (Recommended)** ✅ |
| `persistent: false, logging: true` | In-memory + changelog (fast access, fault tolerant) | High-performance with recovery |
| `persistent: false, logging: false` | In-memory only (fastest, data lost on restart) | Temporary caches, testing |

### Changelog Topics

When `logging: true` is configured:

- **Topic Name**: `<application.id>-<store-name>-changelog`
- **Compaction**: Topics are log-compacted to keep only the latest value per key
- **Recovery**: On restart/rebalance, missing local state is rebuilt from the changelog
- **Performance**: Normal reads use local storage (fast), changelog is only for recovery
- **Requirements**: Required for exactly-once semantics

## Performance Considerations

### Caching Impact

| Setting | Behavior | Use When |
|---------|----------|----------|
| `caching: true` | Batches updates, reduces downstream load | High-throughput with acceptable latency |
| `caching: false` | Immediate emission of all updates | Real-time requirements, debugging |

### Persistence Trade-offs

| Setting | Pros | Cons | Use When |
|---------|------|------|----------|
| `persistent: true` | Survives restarts, enables recovery | Slower, uses disk space | Production, critical state |
| `persistent: false` | Fast, memory-only | Lost on restart | Temporary state, caching |

### Logging Impact

| Setting | Pros | Cons | Use When |
|---------|------|------|----------|
| `logging: true` | Fault tolerance, fast recovery, exactly-once support | Additional Kafka topics, network/storage overhead | Production, fault tolerance needed |
| `logging: false` | Lower overhead, simpler setup | No recovery on failure | Development, non-critical state |

**Important**: Changelog logging supplements local storage - it does NOT replace it. State is always stored locally (RocksDB or memory) for fast access, with the changelog used only for recovery purposes.