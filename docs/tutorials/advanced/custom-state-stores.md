# Custom State Stores in KSML

This tutorial explores how to implement and optimize custom state stores in KSML, allowing you to maintain and manage state in your stream processing applications with greater flexibility and control.

## Introduction to State Stores

State stores are a critical component of stateful stream processing applications. They allow your application to:

- Maintain data across multiple messages and events
- Track historical information for context-aware processing
- Implement stateful operations like aggregations and joins
- Build sophisticated business logic that depends on previous events

KSML provides built-in state store capabilities, but for advanced use cases, you may need to customize how state is stored, accessed, and managed.

## Prerequisites

Before starting this tutorial, you should:

- Understand intermediate KSML concepts (streams, functions, pipelines)
- Have completed the [Aggregations](../intermediate/aggregations.md) and [Joins](../intermediate/joins.md) tutorials
- Be familiar with [Stateful Operations](../../core-concepts/operations.md#stateful-operations)
- Have a basic understanding of Kafka Streams state stores

## State Store Fundamentals

### Types of State Stores in KSML

KSML supports several types of state stores:

1. **Key-Value Stores**: Simple stores that map keys to values
2. **Window Stores**: Stores that organize data by time windows
3. **Session Stores**: Stores that organize data by session windows

Each type has different characteristics and is suitable for different use cases.

### State Store Configuration

State stores in KSML are defined in the `stores` section of your KSML definition file:

```yaml
stores:
  user_profile_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    historyRetention: 7d
    caching: true
    logging: false
```

Key configuration options include:

- **type**: The type of store (keyValue, window, session)
- **keyType/valueType**: The data types for keys and values
- **persistent**: Whether the store should persist data to disk
- **historyRetention**: How long to retain historical data
- **caching**: Whether to enable caching for faster access
- **logging**: Whether to enable change logging

## Implementing Custom State Stores

### 1. Custom Serialization and Deserialization

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

In your Python functions, you can work with the deserialized objects directly:

```yaml
functions:
  process_with_custom_store:
    type: valueTransformer
    code: |
      # Get data from custom store
      stored_value = custom_serialized_store.get(key)

      # Process with custom object
      result = process_data(value, stored_value)

      # Update store with new value
      custom_serialized_store.put(key, result)

      return result
    stores:
      - custom_serialized_store
```

### 2. Custom State Store Implementations

For advanced use cases, you can implement custom state store classes in Java and reference them in KSML:

```yaml
stores:
  specialized_store:
    type: custom
    implementation: org.example.SpecializedStateStore
    config:
      customParam1: value1
      customParam2: value2
```

This approach allows you to implement specialized functionality like:

- Custom indexing for faster lookups
- Compression strategies for large states
- Integration with external systems
- Special eviction policies

### 3. Partitioned State Stores

For better scalability, you can implement partitioned state stores that distribute state across multiple instances:

```yaml
stores:
  partitioned_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    partitioned: true
    numPartitions: 10
```

This approach helps manage large state sizes by distributing the load across multiple partitions.

## Optimizing State Store Performance

### 1. Caching Strategies

Configure caching to balance between performance and memory usage:

```yaml
stores:
  optimized_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true
    cacheSizeBytes: 10485760  # 10MB cache
```

In your functions, organize access patterns to maximize cache hits:

```yaml
functions:
  cache_optimized_function:
    type: valueTransformer
    code: |
      # Batch similar lookups together
      keys_to_lookup = extract_related_keys(value)
      cached_values = {}

      for k in keys_to_lookup:
        cached_values[k] = optimized_store.get(k)

      # Process with cached values
      result = process_with_cached_data(value, cached_values)

      return result
    stores:
      - optimized_store
```

### 2. Memory Management

Implement strategies to control memory usage:

```yaml
functions:
  memory_efficient_function:
    type: valueTransformer
    code: |
      # Use compact data structures
      current_state = optimized_store.get(key)

      if current_state is None:
        current_state = {"c": 0, "s": 0}  # Use short keys

      # Update state with minimal memory footprint
      current_state["c"] += 1  # count
      current_state["s"] += value.get("amount", 0)  # sum

      # Store only what's needed
      optimized_store.put(key, current_state)

      return {"count": current_state["c"], "sum": current_state["s"]}
    stores:
      - optimized_store
```

### 3. Retention Policies

Configure appropriate retention policies to limit state size:

```yaml
stores:
  time_limited_store:
    type: window
    keyType: string
    valueType: json
    persistent: true
    historyRetention: 24h  # Only keep 24 hours of history
    retainDuplicates: false  # Don't store duplicates
```

### 4. Compaction and Cleanup

Implement periodic cleanup to manage state size:

```yaml
functions:
  cleanup_old_data:
    type: valueTransformer
    code: |
      current_time = int(time.time() * 1000)
      stored_data = time_limited_store.get(key)

      if stored_data and "timestamp" in stored_data:
        # Check if data is older than 7 days
        if current_time - stored_data["timestamp"] > 7 * 24 * 60 * 60 * 1000:
          # Delete old data
          time_limited_store.delete(key)
          return None

      return value
    stores:
      - time_limited_store
```

## Practical Example: Advanced User Behavior Tracking

Let's build a complete example that implements sophisticated user behavior tracking with optimized state stores:

```yaml
streams:
  user_events:
    topic: user_activity_events
    keyType: string  # User ID
    valueType: json  # Event details

  user_profiles:
    topic: user_profile_updates
    keyType: string  # User ID
    valueType: json  # Profile details

  user_insights:
    topic: user_behavior_insights
    keyType: string  # User ID
    valueType: json  # Behavior insights

stores:
  # Store for recent user events (last 100 events per user)
  recent_events_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true
    cacheSizeBytes: 52428800  # 50MB cache

  # Store for user profiles
  user_profile_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true

  # Store for behavior metrics (windowed)
  behavior_metrics_store:
    type: window
    keyType: string
    valueType: json
    windowSize: 30d  # 30-day windows
    persistent: true
    historyRetention: 90d  # Keep 90 days of history

functions:
  track_user_events:
    type: valueTransformer
    code: |
      user_id = key
      event = value
      current_time = event.get("timestamp", int(time.time() * 1000))

      # Get recent events for this user
      recent_events = recent_events_store.get(user_id)
      if recent_events is None:
        recent_events = {
          "events": [],
          "event_counts": {},
          "last_updated": current_time
        }

      # Add new event to the list
      event_type = event.get("event_type", "unknown")
      recent_events["events"].append({
        "timestamp": current_time,
        "type": event_type,
        "properties": event.get("properties", {})
      })

      # Update event counts
      if event_type in recent_events["event_counts"]:
        recent_events["event_counts"][event_type] += 1
      else:
        recent_events["event_counts"][event_type] = 1

      # Keep only the most recent 100 events
      if len(recent_events["events"]) > 100:
        recent_events["events"] = recent_events["events"][-100:]

      # Update last updated timestamp
      recent_events["last_updated"] = current_time

      # Store updated events
      recent_events_store.put(user_id, recent_events)

      # Don't emit anything from this function
      return None
    stores:
      - recent_events_store

  update_user_profile:
    type: valueTransformer
    code: |
      user_id = key
      profile_update = value

      # Get existing profile
      existing_profile = user_profile_store.get(user_id)
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
      user_profile_store.put(user_id, existing_profile)

      # Don't emit anything from this function
      return None
    stores:
      - user_profile_store

  generate_user_insights:
    type: valueTransformer
    code: |
      user_id = key
      trigger_event = value  # This could be any event that triggers insight generation
      current_time = int(time.time() * 1000)

      # Get user's recent events
      recent_events = recent_events_store.get(user_id)
      if recent_events is None or not recent_events.get("events"):
        return None  # No events to analyze

      # Get user profile
      user_profile = user_profile_store.get(user_id)
      if user_profile is None:
        user_profile = {"attributes": {}}

      # Get behavior metrics
      behavior_metrics = behavior_metrics_store.get(user_id)
      if behavior_metrics is None:
        behavior_metrics = {
          "session_count": 0,
          "total_time_spent": 0,
          "average_session_length": 0,
          "first_seen": current_time,
          "last_seen": current_time
        }

      # Analyze events to generate insights
      events = recent_events.get("events", [])
      event_counts = recent_events.get("event_counts", {})

      # Calculate recency (days since last activity)
      last_activity = max(event["timestamp"] for event in events) if events else current_time
      recency_days = (current_time - last_activity) / (24 * 60 * 60 * 1000)

      # Calculate frequency (number of events in last 30 days)
      thirty_days_ago = current_time - (30 * 24 * 60 * 60 * 1000)
      recent_event_count = sum(1 for event in events if event["timestamp"] > thirty_days_ago)

      # Calculate engagement score
      engagement_score = calculate_engagement_score(events, user_profile)

      # Update behavior metrics
      behavior_metrics["last_seen"] = last_activity
      behavior_metrics["event_count_30d"] = recent_event_count
      behavior_metrics["recency_days"] = recency_days
      behavior_metrics["engagement_score"] = engagement_score

      # Store updated metrics
      behavior_metrics_store.put(user_id, behavior_metrics)

      # Generate insights
      insights = {
        "user_id": user_id,
        "timestamp": current_time,
        "recency_days": recency_days,
        "frequency_30d": recent_event_count,
        "engagement_score": engagement_score,
        "top_activities": get_top_activities(event_counts),
        "user_segment": determine_user_segment(recency_days, recent_event_count, engagement_score),
        "recommendations": generate_recommendations(events, user_profile)
      }

      return insights
    stores:
      - recent_events_store
      - user_profile_store
      - behavior_metrics_store
    globalCode: |
      def calculate_engagement_score(events, user_profile):
          # Implementation of engagement scoring algorithm
          if not events:
              return 0

          # Simple scoring based on recency and frequency
          recent_events = [e for e in events if e["timestamp"] > (int(time.time() * 1000) - 7 * 24 * 60 * 60 * 1000)]
          score = len(recent_events) * 10

          # Bonus for high-value activities
          for event in recent_events:
              if event["type"] in ["purchase", "subscription", "share"]:
                  score += 20

          return min(100, score)

      def get_top_activities(event_counts):
          # Return top 5 activities by count
          sorted_activities = sorted(event_counts.items(), key=lambda x: x[1], reverse=True)
          return dict(sorted_activities[:5])

      def determine_user_segment(recency, frequency, engagement):
          # Segment users based on RFE (Recency, Frequency, Engagement)
          if recency < 7 and frequency > 10 and engagement > 70:
              return "highly_engaged"
          elif recency < 30 and frequency > 5 and engagement > 40:
              return "engaged"
          elif recency < 90:
              return "casual"
          else:
              return "inactive"

      def generate_recommendations(events, user_profile):
          # Simple recommendation engine based on user activity
          recommendations = []

          event_types = set(event["type"] for event in events)

          if "view_product" in event_types and "add_to_cart" not in event_types:
              recommendations.append("complete_purchase")

          if "subscription_view" in event_types and "subscription" not in event_types:
              recommendations.append("subscribe")

          return recommendations[:3]  # Return top 3 recommendations

pipelines:
  # Process user events
  track_events:
    from: user_events
    mapValues: track_user_events
    filter: is_not_null
    to: tracked_events

  # Process profile updates
  update_profiles:
    from: user_profiles
    mapValues: update_user_profile
    filter: is_not_null
    to: updated_profiles

  # Generate insights (triggered by specific events)
  filter_trigger_events:
    from: user_events
    filter: is_insight_trigger_event
    to: insight_trigger_events

  # Process trigger events to generate insights
  generate_insights:
    from: insight_trigger_events
    mapValues: generate_user_insights
    filter: is_not_null
    to: user_insights
```

This example:
1. Tracks user events in a memory-efficient way, keeping only the 100 most recent events
2. Maintains user profiles with attribute updates
3. Generates behavioral insights using data from multiple state stores
4. Implements sophisticated analytics like engagement scoring and user segmentation
5. Uses optimized state stores with appropriate caching and retention policies

## Advanced State Store Patterns

### 1. Tiered Storage

Implement tiered storage for different access patterns:

```yaml
stores:
  hot_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: false  # In-memory only
    caching: true

  warm_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true

  cold_data_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: false
```

### 2. Read-Through/Write-Through Caching

Implement patterns to synchronize state with external systems:

```yaml
functions:
  cache_with_external_system:
    type: valueTransformer
    code: |
      # Try to get from local cache first
      cached_value = local_cache_store.get(key)

      if cached_value is None:
        # Cache miss - fetch from external system
        external_value = fetch_from_external_system(key)

        # Update local cache
        if external_value is not None:
          local_cache_store.put(key, external_value)

        return process_data(value, external_value)
      else:
        # Cache hit
        return process_data(value, cached_value)
    stores:
      - local_cache_store
```

### 3. Composite State Patterns

Combine multiple state stores for complex use cases:

```yaml
functions:
  use_composite_state:
    type: valueTransformer
    code: |
      # Get data from multiple stores
      recent_data = recent_store.get(key)
      historical_data = historical_store.get(key)
      metadata = metadata_store.get(key)

      # Combine data for processing
      combined_state = {
        "recent": recent_data,
        "historical": historical_data,
        "metadata": metadata
      }

      # Process with combined state
      result = process_with_combined_state(value, combined_state)

      # Update stores as needed
      recent_store.put(key, result.get("recent"))
      metadata_store.put(key, result.get("metadata"))

      return result.get("output")
    stores:
      - recent_store
      - historical_store
      - metadata_store
```

## Best Practices for Custom State Stores

### Performance Considerations

- **Memory Management**: Monitor and control memory usage, especially for large state stores
- **Serialization Overhead**: Be aware of serialization/deserialization costs for frequent state access
- **Caching Strategy**: Configure appropriate cache sizes based on access patterns
- **Persistence Impact**: Understand the performance implications of persistent vs. in-memory stores

### Design Patterns

- **State Partitioning**: Partition large states to improve scalability
- **Minimal State**: Store only what's necessary to reduce resource usage
- **Batched Operations**: Group state operations to improve efficiency
- **Incremental Updates**: Update state incrementally rather than replacing entire objects

### Error Handling

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

## Conclusion

Custom state stores in KSML provide powerful capabilities for implementing sophisticated stateful stream processing applications. By understanding how to implement, optimize, and manage state stores, you can build applications that efficiently maintain state across events and deliver complex business logic.

In the next tutorial, we'll explore [Performance Optimization](performance-optimization.md) to learn how to tune and optimize your KSML applications for maximum throughput and minimal resource usage.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Intermediate Tutorial: Aggregations](../intermediate/aggregations.md)
- [Reference: State Stores](../../reference/data-types-reference.md)
