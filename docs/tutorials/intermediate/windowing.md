# Working with Windowed Operations in KSML

This tutorial explores how to implement time-based windowing operations in KSML for processing streaming data within specific time boundaries. Windowing is fundamental to stream processing analytics.

KSML windowing operations are built on Kafka Streams' windowing capabilities, providing exactly-once processing guarantees and fault-tolerant state management.

## Introduction to Windowed Operations

Windowing divides continuous data streams into finite chunks based on time, enabling:

- **Time-based aggregations**: Calculate metrics within time periods (hourly counts, daily averages)
- **Pattern detection**: Identify trends and anomalies over time windows
- **Late data handling**: Process out-of-order events with configurable grace periods
- **State management**: Maintain temporal state for complex analytics
- **Resource control**: Limit memory usage by automatically expiring old windows

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_clicks && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_click_counts_5min && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic click_counts && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_readings && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_moving_averages && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_averages && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_session_summary && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_sessions && \
    ```

## Window Types in KSML

KSML supports three types of time windows, each suited for different use cases:

### Tumbling Windows

**Concept**: Non-overlapping, fixed-size windows where each record belongs to exactly one window.

```yaml
- type: windowByTime
  windowType: tumbling
  duration: 5m      # Window size
  grace: 30s        # Accept late data up to 30 seconds
```

**Characteristics**:

- No overlap between windows
- Clear, distinct time boundaries
- Memory efficient (fewer windows to maintain)
- Deterministic results

**Use cases**:

- Hourly/daily reports
- Billing periods
- Compliance reporting
- Clear time-boundary analytics

**Kafka Streams equivalent**: `TimeWindows.of(Duration.ofMinutes(5))`

### Hopping Windows

**Concept**: Fixed-size windows that overlap by advancing less than the window size, creating a "sliding" effect.

```yaml
- type: windowByTime
  windowType: hopping
  duration: 10m     # Window size (how much data to include)
  advanceBy: 2m     # Advance every 2 minutes (overlap control)
  grace: 1m         # Late data tolerance
```

**Characteristics**:

- Windows overlap (each record appears in multiple windows)
- Smooth transitions between time periods
- Higher memory usage (more windows active)
- Good for trend analysis

**Use cases**:

- Moving averages
- Smooth metric transitions
- Real-time dashboards
- Anomaly detection with context

**Kafka Streams equivalent**: `TimeWindows.of(Duration.ofMinutes(10)).advanceBy(Duration.ofMinutes(2))`

### Session Windows

**Concept**: Dynamic windows that group events based on activity periods, automatically closing after inactivity gaps.

```yaml
- type: windowBySession
  inactivityGap: 30m  # Close window after 30 minutes of no activity
  grace: 10s          # Accept late arrivals
```

**Characteristics**:

- Variable window sizes based on activity
- Automatically merge overlapping sessions
- Perfect for user behavior analysis
- Complex state management

**Use cases**:

- User browsing sessions
- IoT device activity periods
- Fraud detection sessions
- Activity-based analytics

**Kafka Streams equivalent**: `SessionWindows.with(Duration.ofMinutes(30))`

## Windowing Examples

### Tumbling Window: Click Counting

This example demonstrates tumbling windows by counting user clicks in 5-minute non-overlapping windows.

**What it does**:

1. **Generates user clicks**: Simulates users clicking on different pages
2. **Groups by user**: Each user's clicks are processed separately
3. **Windows by time**: Creates 5-minute tumbling windows
4. **Counts events**: Uses simple count aggregation per window
5. **Handles window keys**: Converts WindowedString keys for output compatibility

**Key KSML concepts demonstrated**:

- `windowByTime` with tumbling windows
- Window state stores with retention policies
- `convertKey` for windowed key compatibility
- Grace periods for late-arriving data

??? info "User clicks producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/windowing/producer-user-clicks.yaml"
    %}
    ```

??? info "Tumbling window count processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml"
    %}
    ```

**Understanding the window key type**:

The `convertKey` operation transforms the internal `WindowedString` to `json:windowed(string)` format, which contains:

- `key`: Original key (user ID)
- `start`/`end`: Window boundaries in milliseconds
- `startTime`/`endTime`: Human-readable timestamps

**Verifying tumbling window data:**

The Kowl UI cannot display the value because a long stored in binary format is not automatically deserialized.
To view the complete data for the target topic user_click_counts_5min, run:
```bash
# View click counts per 5-minute window
docker exec broker kafka-console-consumer.sh \
  --bootstrap-server broker:9093 \
  --topic user_click_counts_5min \
  --from-beginning \
  --max-messages 5 \
  --property print.key=true \
  --property key.separator=" | " \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.LongDeserializer
```

**Example output:**
```
{"end":1755042000000,"endTime":"2025-08-12T23:40:00Z","key":"alice","start":1755041700000,"startTime":"2025-08-12T23:35:00Z"} | 8
{"end":1755042000000,"endTime":"2025-08-12T23:40:00Z","key":"bob","start":1755041700000,"startTime":"2025-08-12T23:35:00Z"} | 12
```

### Hopping Window: Moving Averages

This example calculates moving averages using overlapping 2-minute windows that advance every 30 seconds.

**What it does**:

1. **Generates sensor readings**: Simulates temperature, humidity, and pressure sensors
2. **Groups by sensor**: Each sensor's readings are processed independently
3. **Overlapping windows**: 2-minute windows advance every 30 seconds (4x overlap)
4. **Calculates averages**: Maintains sum and count, then computes final average
5. **Smooth transitions**: Provides continuous updates every 30 seconds

**Key KSML concepts demonstrated**:

- `windowByTime` with hopping windows and `advanceBy`
- Custom aggregation with initialization and aggregator functions
- `mapValues` for post-aggregation processing
- Multiple overlapping windows for the same data

??? info "Sensor data producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/windowing/producer-sensor-data.yaml"
    %}
    ```

??? info "Hopping window average processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/windowing/processor-hopping-average-working.yaml"
    %}
    ```

**Example output:**
```
# Raw sensor readings:
temp_01 | {"reading_id":1,"sensor_id":"temp_01","value":21.57,"unit":"celsius","timestamp":1755042846167}

# Moving averages:
{"end":1755043080000,"endTime":"2025-08-12T23:58:00Z","key":"temp_01","start":1755042960000,"startTime":"2025-08-12T23:56:00Z"} | {"average":21.49,"sample_count":4,"total_sum":85.96}
```

**Understanding the hopping window output:**

- **Key structure**: The output key contains window boundaries and the original record key
  - `start`: Window start time (23:56:00Z)
  - `end`: Window end time (23:58:00Z) 
  - `key`: Original sensor ID (temp_01)
- **Window duration**: 2-minute window covering 23:56:00Z to 23:58:00Z
- **Value aggregation**: Contains calculated average (21.49°C) from 4 sensor readings
- **Overlapping nature**: Since windows advance every 30 seconds, this sensor will appear in multiple overlapping windows, each with slightly different averages as new data arrives and old data expires

**Why hopping windows for averages?**

- **Smooth updates**: New average every 30 seconds instead of waiting 2 minutes
- **Trend detection**: Easier to spot gradual changes
- **Real-time dashboards**: Continuous data flow for visualization
- **Reduced noise**: 2-minute window smooths out brief spikes

### Session Window: User Activity Analysis

This example uses session windows to analyze user browsing patterns by grouping clicks within activity periods.

**What it does**:

1. **Tracks user clicks**: Monitors page visits and click patterns
2. **Detects sessions**: Groups clicks separated by no more than 2 minutes
3. **Aggregates activity**: Counts clicks, tracks unique pages, measures duration
4. **Session boundaries**: Automatically closes sessions after inactivity
5. **Rich analytics**: Provides comprehensive session summaries

**Key KSML concepts demonstrated**:

- `windowBySession` with inactivity gap detection
- Session state stores for variable-length windows
- Complex aggregation state with lists and timestamps
- Automatic session merging and boundary detection

**Session window behavior**:

- **Dynamic sizing**: Windows grow and shrink based on activity
- **Automatic merging**: Late-arriving data can extend or merge sessions
- **Activity-based**: Perfect for user behavior analysis
- **Variable retention**: Different sessions can have different lifespans

??? info "User clicks producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/windowing/producer-user-clicks.yaml"
    %}
    ```

??? info "User session analysis processor (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml"
    %}
    ```

**Session window characteristics observed**:

- **Activity-driven boundaries**: Sessions start with first click and extend with each new click
- **Inactivity-based closure**: Sessions close after 2 minutes of no activity
- **Variable duration**: Sessions can be seconds to hours depending on user behavior
- **Real-time updates**: Each click updates the session end time and click count
- **User-specific**: Each user maintains independent session windows

**Example output**:
```
USER SESSION - user=alice, clicks=15  (session: 23:44:05Z to 23:46:12Z)
USER SESSION - user=bob, clicks=8    (session: 23:44:01Z to 23:45:33Z) 
USER SESSION - user=alice, clicks=23 (session: 23:47:01Z to 23:49:15Z)
```

This shows how Alice had two separate sessions with an inactivity gap between them, while Bob had one continuous session.

**Example output:**
```
# Input clicks (user_clicks topic):
alice | {"click_id":"click_000001","user_id":"alice","page":"/home","session_id":"session_alice_1","timestamp":1755043944188}
alice | {"click_id":"click_000002","user_id":"alice","page":"/products","session_id":"session_alice_1","timestamp":1755043945189}

# Session summary (user_session_summary topic):  
{"end":1755043970189,"endTime":"2025-08-13T00:12:50.189Z","key":"alice","start":1755043944188,"startTime":"2025-08-13T00:12:24.188Z"} | 15
{"end":1755043938275,"endTime":"2025-08-13T00:12:18.275Z","key":"diana","start":1755043938275,"startTime":"2025-08-13T00:12:18.275Z"} | 1
```

The session summary shows:

- **Key**: Windowed key with session boundaries and original user key
- **Value**: Total click count for that session (15 clicks for Alice, 1 click for Diana)

**Understanding the output:**

- Alice had a session lasting ~26 seconds (00:12:24Z to 00:12:50Z) with 15 clicks
- Diana had a brief session (single timestamp) with 1 click
- Sessions automatically close after 2 minutes of inactivity

## Advanced Windowing Concepts

### Grace Periods and Late Data

**Problem**: In distributed systems, data doesn't always arrive in chronological order. Network delays, system failures, and clock skew can cause "late" data.

**Solution**: Grace periods allow windows to accept late-arriving data for a specified time after the window officially closes.

```yaml
- type: windowByTime
  windowType: tumbling
  duration: 5m
  grace: 1m        # Accept data up to 1 minute late
```

**Configuration guidelines**:

- **grace**: How long to wait for late data (trade-off: accuracy vs. latency)
- **retention**: How long to keep window state (must be ≥ window size + grace period)
- **caching**: Enable for better performance with frequent updates

**Example**: A 5-minute window ending at 10:05 AM will accept data timestamped before 10:05 AM until 10:06 AM (1-minute grace), then close permanently.

### Window State Management

Windowed operations require persistent state to:

- Track aggregations across window boundaries
- Handle late-arriving data
- Provide exactly-once processing guarantees
- Enable fault tolerance and recovery

**State Store Configuration**:

```yaml
store:
  name: my_window_store
  type: window             # Required for windowed operations
  windowSize: 5m           # Must match window duration
  retention: 30m           # Keep expired windows (≥ windowSize + grace)
  caching: true            # Reduce downstream updates
  retainDuplicates: false  # Keep only latest value per window
```

## Performance Considerations

### Memory Usage

**Window count calculation**:

- **Tumbling**: `retention / window_size` windows per key
- **Hopping**: `retention / advance_by` windows per key  
- **Session**: Variable, depends on activity patterns

**Example**: 1-hour retention with 5-minute tumbling windows = 12 windows per key

### Optimization Strategies

1. **Right-size windows**:
   - **Too small (10 seconds)**: Creates excessive overhead with frequent window updates and high CPU usage
   - **Too large (24 hours)**: Consumes excessive memory and delays results until window closes
   - **Balanced approach**: Choose window size that matches your business requirements (e.g., 5 minutes for real-time dashboards, 1 hour for reporting)

2. **Tune grace periods**:
   - **Minimal grace (5 seconds)**: Provides fast processing but may lose legitimate late-arriving data
   - **Conservative grace (5 minutes)**: Handles most network delays and clock skew but slows down result publication
   - **Best practice**: Set grace period based on your network characteristics and data source reliability

3. **Enable caching**:
   - **Purpose**: Reduces the number of downstream updates by batching window changes
   - **Benefit**: Lower CPU usage and fewer Kafka messages when windows are frequently updated
   - **Trade-off**: Slightly higher memory usage for improved throughput

4. **Optimize retention**:
   - **Minimum requirement**: Window size plus grace period (e.g., 5-minute window + 30-second grace = 5.5 minutes minimum)
   - **Memory impact**: Longer retention keeps more data in memory for join operations and late data handling
   - **Performance balance**: Set retention just long enough to handle your latest acceptable late data

## Troubleshooting Common Issues

### Missing Data in Windows

**Symptoms**: Expected data doesn't appear in window results

**Causes & Solutions**:

1. **Clock skew**: Ensure producer/consumer clocks are synchronized
2. **Grace period too short**: Increase grace period for late data
3. **Wrong timestamps**: Verify timestamp field extraction
4. **Retention too short**: Data expired before processing

### High Memory Usage

**Symptoms**: OutOfMemory errors, slow processing

**Solutions**:

1. **Reduce retention periods**
2. **Increase window sizes** (fewer windows)
3. **Enable caching** to reduce state store pressure
4. **Filter data earlier** in the pipeline

### Inconsistent Results

**Symptoms**: Different runs produce different window contents

**Causes**:

1. **Late data**: Some runs receive different late arrivals
2. **Grace period timing**: Data arrives just at grace boundary
3. **System clock differences**: Inconsistent time sources

**Solutions**:

1. Use consistent time sources
2. Implement proper grace periods
3. Consider event-time vs processing-time semantics

### Window Type Selection Guide

| Window Type | Best For | Key Benefits | Trade-offs |
|-------------|----------|--------------|------------|
| **Tumbling** | Periodic reports, billing cycles, compliance | Clear boundaries, memory efficient, deterministic | Less granular, potential data delays |
| **Hopping** | Moving averages, real-time dashboards, trend analysis | Smooth updates, continuous metrics | Higher memory usage, more computation |
| **Session** | User behavior, IoT device activity, fraud detection | Activity-driven, variable length, natural boundaries | Complex state management, harder to predict |

### Getting Started

1. **Start simple**: Begin with tumbling windows for periodic analytics
2. **Add smoothness**: Use hopping windows when you need continuous updates
3. **Handle activity**: Implement session windows for behavior-driven analysis
4. **Optimize gradually**: Tune grace periods, retention, and caching based on requirements

## Further Reading

- [Reference: Windowing Operations](../../reference/operation-reference.md#windowing-operations)