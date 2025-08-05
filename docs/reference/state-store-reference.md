# State Store Reference

This document provides a comprehensive reference for state stores in KSML. It covers state store types, configuration options, and best practices.

## What are State Stores?

State stores are a critical component of stateful stream processing applications. They allow your application to:

- Maintain data across multiple messages and events
- Track historical information for context-aware processing
- Implement stateful operations like aggregations and joins
- Build sophisticated business logic that depends on previous events

In KSML, state stores are used by stateful operations such as aggregations, joins, and windowed operations to maintain state between records.

## State Store Types

KSML supports several types of state stores:

| Type | Description | Use Cases |
|------|-------------|-----------|
| `keyValue` | Simple stores that map keys to values | General-purpose state storage, lookups |
| `window` | Stores that organize data by time windows | Time-based aggregations, windowed joins |
| `session` | Stores that organize data by session windows | Session-based analytics, user activity tracking |

## State Store Configuration

State stores in KSML are defined in the `stores` section of your KSML definition file:

```yaml
stores:
  my_store_name:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true
```

### Configuration Parameters

| Parameter | Type | Default | Required | Description |
|-----------|------|---------|----------|-------------|
| `name` | String | _none_ | Optional | The name of the state store. If not specified, the operation's name will be used. |
| `type` | String | _none_ | Required | The type of state store. Possible values: `keyValue`, `window`, `session`. |
| `persistent` | Boolean | `false` | Optional | When `true`, the state store is persisted to disk. When `false`, the state store is in-memory only. |
| `timestamped` | Boolean | `false` | Optional | (Only relevant for keyValue and window stores) When `true`, all messages in the state store are timestamped. |
| `versioned` | Boolean | `false` | Optional | (Only relevant for keyValue stores) When `true`, elements in the store are versioned. |
| `keyType` | String | _none_ | Required | The key type of the state store. See [Data Types Reference](data-type-reference.md) for more information. |
| `valueType` | String | _none_ | Required | The value type of the state store. See [Data Types Reference](data-type-reference.md) for more information. |
| `caching` | Boolean | `false` | Optional | When `true`, the state store caches entries. When `false`, all changes to the state store will be emitted immediately. |
| `logging` | Boolean | `false` | Optional | When `true`, state changes are written to a changelog topic. When `false`, no changelog topic is written. |
| `retention` | Duration | _none_ | Optional | (Only relevant for window and session stores) How long to retain data in the store. |
| `windowSize` | Duration | _none_ | Optional | (Only relevant for window stores) The size of the window. |
| `grace` | Duration | _none_ | Optional | (Only relevant for window stores) The grace period for late-arriving data. |

### Example Configurations

#### Key-Value Store

```yaml
stores:
  user_profile_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true
    logging: true
```

#### Window Store

```yaml
stores:
  sales_metrics_store:
    type: window
    keyType: string
    valueType: json
    windowSize: 1h
    retention: 24h
    persistent: true
    caching: true
```

#### Session Store

```yaml
stores:
  user_session_store:
    type: session
    keyType: string
    valueType: json
    retention: 24h
    persistent: true
```

## Using State Stores in Operations

State stores are used by stateful operations in KSML. You can reference a state store in an operation using the `store` parameter:

```yaml
pipelines:
  aggregate_sales:
    from: sales_stream
    via:
      - type: groupBy
        keySelector:
          expression: value.get('category')
      - type: aggregate
        store: sales_by_category_store  # Reference to a state store
        initializer:
          expression: {"count": 0, "total": 0.0}
        aggregator:
          expression: {
            "count": aggregate.get('count') + 1,
            "total": aggregate.get('total') + value.get('amount')
          }
    to: aggregated_sales
```

You can also define a state store inline within an operation:

```yaml
pipelines:
  aggregate_sales:
    from: sales_stream
    via:
      - type: groupBy
        keySelector:
          expression: value.get('category')
      - type: aggregate
        store:  # Inline state store definition
          type: keyValue
          keyType: string
          valueType: json
          persistent: true
          caching: true
        initializer:
          expression: {"count": 0, "total": 0.0}
        aggregator:
          expression: {
            "count": aggregate.get('count') + 1,
            "total": aggregate.get('total') + value.get('amount')
          }
    to: aggregated_sales
```

## Advanced State Store Features

### Custom Serialization

For complex data types or special performance requirements, you can implement custom serialization:

```yaml
stores:
  custom_serialized_store:
    type: keyValue
    keyType: string
    valueType: custom
    persistent: true
    serdes:
      key: org.example.CustomKeySerializer
      value: org.example.CustomValueSerializer
```

### Tiered Storage

You can implement tiered storage for different access patterns:

```yaml
stores:
  # Hot data (in-memory, fast access)
  hot_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: false
    caching: true

  # Warm data (persistent with caching)
  warm_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true

  # Cold data (persistent without caching)
  cold_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: false
```

## Best Practices

### 1. Choose the Right Store Type

Select the appropriate store type for your use case:
- Use `keyValue` for simple key-value lookups
- Use `window` for time-based aggregations
- Use `session` for user session tracking

### 2. Configure Persistence Appropriately

- Use `persistent: true` for critical data that must survive application restarts
- Use `persistent: false` for temporary data or when performance is critical

### 3. Optimize Caching

- Enable caching (`caching: true`) for frequently accessed data
- Disable caching for data that changes frequently or when immediate updates are required

### 4. Manage State Size

- Set appropriate retention periods for windowed stores
- Implement cleanup logic for old or unused data
- Consider partitioning large states across multiple instances

### 5. Error Handling

Implement robust error handling for state store operations:

```yaml
functions:
  robust_state_access:
    type: valueTransformer
    code: |
      try:
        # Access state store
        stored_value = my_store.get(key)

        # Process with state
        result = process_with_state(value, stored_value)

        # Update state
        my_store.put(key, updated_state)

        return result
      except Exception as e:
        log.error("Error accessing state store: {}", str(e))
        # Return fallback value or original input
        return value
    stores:
      - my_store
```

## Examples

### Example 1: User Profile Store

```yaml
stores:
  user_profiles:
    type: keyValue
    keyType: string  # User ID
    valueType: json  # User profile data
    persistent: true
    caching: true
    logging: true

functions:
  update_user_profile:
    type: valueTransformer
    code: |
      user_id = key
      profile_update = value

      # Get existing profile
      existing_profile = user_profiles.get(user_id)
      if existing_profile is None:
        existing_profile = {
          "created_at": int(time.time() * 1000),
          "attributes": {}
        }

      # Update profile with new data
      if "attributes" in profile_update:
        for attr_key, attr_value in profile_update["attributes"].items():
          existing_profile["attributes"][attr_key] = attr_value

      # Add metadata
      existing_profile["last_updated"] = int(time.time() * 1000)

      # Store updated profile
      user_profiles.put(user_id, existing_profile)

      return existing_profile
    stores:
      - user_profiles
```

### Example 2: Windowed Metrics

```yaml
stores:
  metrics_store:
    type: window
    keyType: string  # Metric name
    valueType: json  # Metric values
    windowSize: 5m
    retention: 1d
    persistent: true

pipelines:
  calculate_metrics:
    from: events_stream
    via:
      - type: groupBy
        keySelector:
          expression: value.get('metric_name')
      - type: windowedBy
        timeWindows:
          size: 5m
      - type: aggregate
        store: metrics_store
        initializer:
          expression: {"count": 0, "sum": 0, "min": null, "max": null}
        aggregator:
          code: |
            metric_value = value.get('metric_value', 0)
            
            if aggregate is None:
              return {
                "count": 1,
                "sum": metric_value,
                "min": metric_value,
                "max": metric_value
              }
            else:
              return {
                "count": aggregate.get('count', 0) + 1,
                "sum": aggregate.get('sum', 0) + metric_value,
                "min": min(aggregate.get('min', metric_value), metric_value),
                "max": max(aggregate.get('max', metric_value), metric_value)
              }
    to: windowed_metrics
```

### Example 3: Session Tracking

```yaml
stores:
  user_sessions:
    type: session
    keyType: string  # User ID
    valueType: json  # Session data
    retention: 24h
    persistent: true

pipelines:
  track_user_sessions:
    from: user_activity
    via:
      - type: groupByKey  # Group by user ID
      - type: windowBySession
        inactivityGap: 30m  # 30 minute inactivity defines a new session
      - type: aggregate
        store: user_sessions
        initializer:
          expression: {"events": [], "start_time": 0, "end_time": 0}
        aggregator:
          code: |
            current_time = value.get('timestamp', int(time.time() * 1000))
            
            if aggregate is None:
              return {
                "events": [value],
                "start_time": current_time,
                "end_time": current_time
              }
            else:
              events = aggregate.get('events', [])
              events.append(value)
              
              return {
                "events": events,
                "start_time": aggregate.get('start_time'),
                "end_time": current_time,
                "duration_ms": current_time - aggregate.get('start_time'),
                "event_count": len(events)
              }
    to: user_session_analytics
```

## Related Topics

- [KSML Language Reference](language-reference.md)
- [Operations Reference](operation-reference.md)
- [Pipeline Reference](pipeline-reference.md)
- [Data Types Reference](data-type-reference.md)