# Using Windowed Operations in KSML

This tutorial explores how to implement time-based windowing operations in KSML, allowing you to process data within specific time boundaries.

## Introduction to Windowed Operations

Windowed operations are a powerful feature in stream processing that allow you to group and process data within specific time intervals. They're essential for:

- Time-based aggregations (hourly counts, daily averages, etc.)
- Detecting patterns over time
- Handling late-arriving data
- Limiting the scope of stateful operations

In KSML, windowing operations are implemented using Kafka Streams' windowing capabilities, with the added flexibility of Python functions for processing the windowed data.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Be familiar with [Stateful Operations](../../core-concepts/operations.md#stateful-operations)
- Have a basic understanding of [Aggregations](aggregations.md)

## Types of Windows in KSML

KSML supports several types of windows, each with different semantics:

### Tumbling Windows

Tumbling windows divide the stream into fixed-size, non-overlapping time intervals. Each event belongs to exactly one window.

```yaml
windowByTime:
  size: 1h  # 1-hour windows
  advanceBy: 1h  # Move forward by the full window size
```

Use tumbling windows when you need:
- Clear boundaries between time periods
- Non-overlapping aggregations (e.g., hourly statistics)
- Minimal state storage requirements

### Hopping Windows

Hopping windows are fixed-size windows that can overlap because they "hop" forward by a specified amount that's smaller than the window size.

```yaml
windowByTime:
  size: 1h  # 1-hour windows
  advanceBy: 15m  # Move forward by 15 minutes
```

Use hopping windows when you need:
- Overlapping time periods
- Smoother transitions between windows
- Moving averages or sliding metrics

### Sliding Windows

Sliding windows in KSML are implemented using session windows with a very small inactivity gap:

```yaml
windowBySession:
  inactivityGap: 1ms  # Minimal gap creates a sliding effect
```

Use sliding windows when you need:
- Event-driven windows that adapt to data arrival patterns
- Windows that close after a period of inactivity

### Session Windows

Session windows group events that occur close to each other in time, with windows closing after a specified period of inactivity.

```yaml
windowBySession:
  inactivityGap: 30m  # Close the session after 30 minutes of inactivity
```

Use session windows when you need:
- Activity-based grouping (e.g., user sessions)
- Dynamic window sizes based on event patterns
- Windows that adapt to natural breaks in the data

## Working with Windowed Operations

### Basic Windowed Aggregation

Here's a simple example that counts events in 5-minute tumbling windows:

```yaml
streams:
  user_clicks:
    topic: website_clicks
    keyType: string  # User ID
    valueType: json  # Click details
    
  click_counts:
    topic: user_click_counts
    keyType: string  # User ID
    valueType: json  # Count information with window details

functions:
  initialize_count:
    type: initializer
    expression: 0
    
  increment_count:
    type: aggregator
    expression: aggregatedValue + 1

pipelines:
  count_clicks:
    from: user_clicks
    groupByKey:
    windowByTime:
      size: 5m
      advanceBy: 5m
    aggregate:
      initializer: initialize_count
      aggregator: increment_count
    to: click_counts
```

### Handling Late Data with Grace Periods

In real-world scenarios, data often arrives late. KSML allows you to specify a grace period during which late-arriving data will still be included in the window:

```yaml
windowByTime:
  size: 1h
  advanceBy: 1h
  grace: 10m  # Accept data up to 10 minutes late
```

### Accessing Window Information

When working with windowed operations, you might need to access information about the window itself. KSML provides this information through the window metadata:

```yaml
functions:
  aggregate_with_window_info:
    type: aggregator
    code: |
      # Initialize if needed
      if aggregatedValue is None:
        aggregatedValue = {"count": 0, "window_start": None, "window_end": None}
      
      # Update count
      aggregatedValue["count"] += 1
      
      # Access window information from metadata
      if "window" in metadata:
        aggregatedValue["window_start"] = metadata["window"]["start"]
        aggregatedValue["window_end"] = metadata["window"]["end"]
      
      return aggregatedValue
```

## Practical Example: Time-Based Analytics

Let's build a complete example that implements a real-time analytics system using windowed operations:

```yaml
streams:
  page_views:
    topic: website_page_views
    keyType: string  # Page URL
    valueType: json  # View details including user_id, duration, etc.
    
  hourly_stats:
    topic: page_view_hourly_stats
    keyType: string  # Page URL
    valueType: json  # Hourly statistics
    
  daily_stats:
    topic: page_view_daily_stats
    keyType: string  # Page URL
    valueType: json  # Daily statistics

functions:
  initialize_stats:
    type: initializer
    expression: {"views": 0, "unique_users": set(), "total_duration": 0, "window_start": None, "window_end": None}
    
  update_stats:
    type: aggregator
    code: |
      # Initialize if needed
      if aggregatedValue is None:
        aggregatedValue = {"views": 0, "unique_users": set(), "total_duration": 0, "window_start": None, "window_end": None}
      
      # Extract data
      user_id = value.get("user_id", "unknown")
      duration = value.get("duration", 0)
      
      # Update statistics
      aggregatedValue["views"] += 1
      aggregatedValue["unique_users"].add(user_id)
      aggregatedValue["total_duration"] += duration
      
      # Access window information
      if "window" in metadata:
        aggregatedValue["window_start"] = metadata["window"]["start"]
        aggregatedValue["window_end"] = metadata["window"]["end"]
      
      return aggregatedValue
      
  finalize_stats:
    type: valueTransformer
    code: |
      if value is None:
        return None
        
      # Convert set to count for serialization
      if "unique_users" in value and isinstance(value["unique_users"], set):
        value["unique_visitors"] = len(value["unique_users"])
        del value["unique_users"]  # Remove the set which can't be serialized
        
      # Calculate averages
      if value["views"] > 0:
        value["avg_duration"] = value["total_duration"] / value["views"]
      else:
        value["avg_duration"] = 0
        
      # Add timestamp for easier querying
      if "window_start" in value:
        value["timestamp"] = value["window_start"]
        
      return value

pipelines:
  # Hourly statistics pipeline
  hourly_analytics:
    from: page_views
    groupByKey:
    windowByTime:
      size: 1h
      advanceBy: 1h
      grace: 10m
    aggregate:
      initializer: initialize_stats
      aggregator: update_stats
    mapValues: finalize_stats
    to: hourly_stats
    
  # Daily statistics pipeline
  daily_analytics:
    from: page_views
    groupByKey:
    windowByTime:
      size: 1d
      advanceBy: 1d
      grace: 2h
    aggregate:
      initializer: initialize_stats
      aggregator: update_stats
    mapValues: finalize_stats
    to: daily_stats
```

This example:
1. Processes page view events
2. Creates both hourly and daily windows
3. Calculates statistics including view counts, unique visitors, and average duration
4. Handles the conversion of non-serializable data (sets) before output
5. Includes window timestamp information for easier querying

## Advanced Windowing Patterns

### Multi-Level Windowing

For complex analytics, you might want to implement multi-level windowing, where data is aggregated at different time granularities:

```yaml
pipelines:
  # Minute-level aggregation
  minute_aggregation:
    from: raw_events
    groupByKey:
    windowByTime:
      size: 1m
      advanceBy: 1m
    aggregate:
      initializer: initialize_minute_stats
      aggregator: update_minute_stats
    to: minute_stats
    
  # Hour-level aggregation from minute stats
  hour_aggregation:
    from: minute_stats
    groupByKey:
    windowByTime:
      size: 1h
      advanceBy: 1h
    aggregate:
      initializer: initialize_hour_stats
      aggregator: update_hour_stats
    to: hour_stats
```

### Combining Windows with Joins

Windowed operations can be combined with joins to correlate events from different streams within time boundaries:

```yaml
pipelines:
  correlate_events:
    from: stream_a
    join:
      stream: stream_b
      valueJoiner: combine_events
      windows:
        type: time
        size: 5m
        grace: 1m
    to: correlated_events
```

## Best Practices for Windowed Operations

### Performance Considerations

- **Window Size**: Larger windows require more state storage. Choose appropriate window sizes.
- **Grace Period**: Longer grace periods increase state storage requirements but improve handling of late data.
- **Serialization**: Be careful with complex objects in windowed aggregations, as they need to be serialized.

### Design Patterns

- **Pre-Aggregation**: For high-volume streams, consider pre-aggregating at a finer granularity before wider windows.
- **Downsampling**: For time series data, consider downsampling before windowing to reduce state size.
- **Window Alignment**: For easier analysis, align windows to natural time boundaries (start of hour, day, etc.).

### Error Handling

Always handle potential errors in your windowed aggregation functions:

```yaml
functions:
  safe_windowed_aggregator:
    type: aggregator
    code: |
      try:
        # Your windowed aggregation logic here
        return result
      except Exception as e:
        log.error("Error in windowed aggregation: {}", str(e))
        # Return previous state to avoid losing data
        return aggregatedValue
```

## Conclusion

Windowed operations are essential for time-based analytics and processing in streaming applications. KSML makes it easy to implement various types of windows while leveraging Python for the processing logic.

By understanding the different window types and their appropriate use cases, you can build powerful data pipelines that process data within meaningful time boundaries.

In the next tutorial, we'll explore [Error Handling and Recovery](error-handling.md) to build more robust KSML applications.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Reference: Windowing Operations](../../reference/operation-reference.md)