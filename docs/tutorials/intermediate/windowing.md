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

Each event falls into exactly one window, based on its timestamp:

```mermaid
--8<-- "snippets/mermaid-init.txt"
gantt
    title Tumbling windows (duration 5m)
    dateFormat HH:mm
    axisFormat %H:%M

    section Windows
    Window 1 :w1, 10:00, 5m
    Window 2 :w2, 10:05, 5m
    Window 3 :w3, 10:10, 5m

    section Events
    Event A (window 1) :milestone, 10:01, 0m
    Event B (window 1) :milestone, 10:03, 0m
    Event C (window 2) :milestone, 10:07, 0m
    Event D (window 3) :milestone, 10:12, 0m
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

A new window starts every 2 minutes and each one lasts 10 minutes, so a single event falls into `duration / advanceBy` = 5 windows at the same time:

```mermaid
--8<-- "snippets/mermaid-init.txt"
gantt
    title Hopping windows (duration 10m, advanceBy 2m)
    dateFormat HH:mm
    axisFormat %H:%M

    section Windows
    Window 1 :w1, 10:00, 10m
    Window 2 :w2, 10:02, 10m
    Window 3 :w3, 10:04, 10m
    Window 4 :w4, 10:06, 10m
    Window 5 :w5, 10:08, 10m

    section Events
    Event A (windows 1 to 5) :milestone, 10:09, 0m
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

A session starts with the first event for a key and grows with every event that follows within 30 minutes. A gap longer than 30 minutes closes the session, and the next event starts a new one:

```mermaid
--8<-- "snippets/mermaid-init.txt"
gantt
    title Session windows for one user (inactivityGap 30m)
    dateFormat HH:mm
    axisFormat %H:%M

    section Sessions
    Session 1 :s1, 10:00, 25m
    Gap longer than 30m :crit, gap, 10:25, 55m
    Session 2 :s2, 11:20, 10m

    section Events
    User click :milestone, 10:00, 0m
    User click :milestone, 10:10, 0m
    User click :milestone, 10:25, 0m
    User click :milestone, 11:20, 0m
    User click :milestone, 11:30, 0m
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

The diagram below follows six example clicks through the same pipeline, showing how the messages are grouped at each step: first by user, then by 5-minute window. Every click sends an updated count for its window. The diagram shows the last one:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with six clicks
    Topic[(user_clicks)]
    Clicks@{ shape: docs, label: "<b>key: alice</b> click at 10:01<br/><b>key: bob</b> click at 10:02<br/><b>key: alice</b> click at 10:03<br/><b>key: alice</b> click at 10:06<br/><b>key: bob</b> click at 10:07<br/><b>key: bob</b> click at 10:08" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Alice@{ shape: docs, label: "<b>key: alice</b><br/>10:01, 10:03, 10:06" }
        Bob@{ shape: docs, label: "<b>key: bob</b><br/>10:02, 10:07, 10:08" }
    end

    %% Step 2: split each group into 5-minute windows
    Window["<code>windowByTime</code><br/>tumbling 5m"]
    subgraph ByWindow["Grouped by key and window"]
        Alice1@{ shape: docs, label: "<b>alice</b> 10:00 to 10:05<br/>10:01, 10:03" }
        Alice2@{ shape: doc, label: "<b>alice</b> 10:05 to 10:10<br/>10:06" }
        Bob1@{ shape: doc, label: "<b>bob</b> 10:00 to 10:05<br/>10:02" }
        Bob2@{ shape: docs, label: "<b>bob</b> 10:05 to 10:10<br/>10:07, 10:08" }
    end

    %% Step 3: one count per key and window
    Count[<code>count</code>]
    subgraph Counts["Latest result per key and window"]
        AliceCount1["<b>alice</b> 10:00 to 10:05<br/>count: 2"]
        AliceCount2["<b>alice</b> 10:05 to 10:10<br/>count: 1"]
        BobCount1["<b>bob</b> 10:00 to 10:05<br/>count: 1"]
        BobCount2["<b>bob</b> 10:05 to 10:10<br/>count: 2"]
    end

    %% Step 4: write to the output topic with a JSON window key
    Convert["<code>toStream</code> + <code>convertKey</code><br/>json:windowed(string)"]
    Output[(user_click_counts_5min)]

    %% Data Flow
    Topic --> Clicks
    Clicks --> Group
    Group --> Alice & Bob
    Alice & Bob --> Window
    Window --> Alice1 & Alice2 & Bob1 & Bob2
    Alice1 & Alice2 & Bob1 & Bob2 --> Count
    Count --> AliceCount1 & AliceCount2 & BobCount1 & BobCount2
    AliceCount1 & AliceCount2 & BobCount1 & BobCount2 --> Convert
    Convert -->|"key: {key: alice, startTime: 10:00, endTime: 10:05, ...}<br/>value: 2"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Window,Count,Convert operation
    class Clicks,Alice,Bob,Alice1,Alice2,Bob1,Bob2 message
    class AliceCount1,AliceCount2,BobCount1,BobCount2 result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style ByWindow fill:none,stroke:#616161,stroke-dasharray:5 5
    style Counts fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "User clicks producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/producer-user-clicks.yaml" %}
    ```

??? info "Tumbling window count processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml" %}
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

The diagram below follows five example readings through the same pipeline. Because the windows overlap, the same reading is counted in several windows, so each window has its own average. Every reading sends an updated average for each of its windows, and the diagram shows the last one. To keep the diagram readable, it only follows `temp_01` and shows three of its windows:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with five readings
    Topic[(sensor_readings)]
    Readings@{ shape: docs, label: "<b>temp_01</b>: 21.0 at 10:00:10<br/><b>humidity_01</b>: 45.0 at 10:00:20<br/><b>temp_01</b>: 22.0 at 10:00:40<br/><b>temp_01</b>: 23.0 at 10:01:20<br/><b>temp_01</b>: 24.0 at 10:02:10" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Temp@{ shape: docs, label: "<b>key: temp_01</b><br/>21.0, 22.0, 23.0, 24.0" }
        Humidity@{ shape: doc, label: "<b>key: humidity_01</b><br/>45.0" }
    end

    %% Step 2: overlapping 2-minute windows, a new one every 30 seconds
    Window["<code>windowByTime</code><br/>hopping 2m, advanceBy 30s"]
    subgraph ByWindow["Grouped by key and window (overlapping)"]
        Temp1@{ shape: docs, label: "<b>temp_01</b> 10:00:00 to 10:02:00<br/>21.0, 22.0, 23.0" }
        Temp2@{ shape: docs, label: "<b>temp_01</b> 10:00:30 to 10:02:30<br/>22.0, 23.0, 24.0" }
        Temp3@{ shape: docs, label: "<b>temp_01</b> 10:01:00 to 10:03:00<br/>23.0, 24.0" }
        NotShown["Other windows and<br/>humidity_01: not shown"]
    end

    %% Step 3: sum and count per window
    Aggregate["<code>aggregate</code><br/>update_average"]
    subgraph Sums["One sum and count per key and window"]
        Sum1["<b>temp_01</b> 10:00:00 to 10:02:00<br/>sum: 66.0, count: 3"]
        Sum2["<b>temp_01</b> 10:00:30 to 10:02:30<br/>sum: 69.0, count: 3"]
        Sum3["<b>temp_01</b> 10:01:00 to 10:03:00<br/>sum: 47.0, count: 2"]
    end

    %% Step 4: turn sum and count into an average
    Average["<code>toStream</code> + <code>convertKey</code><br/>+ <code>mapValues</code> calculate_final_average"]
    subgraph Averages["Latest average per key and window"]
        Avg1["<b>temp_01</b> 10:00:00 to 10:02:00<br/>average: 22.0"]
        Avg2["<b>temp_01</b> 10:00:30 to 10:02:30<br/>average: 23.0"]
        Avg3["<b>temp_01</b> 10:01:00 to 10:03:00<br/>average: 23.5"]
    end

    %% Output
    Output[(sensor_moving_averages)]

    %% Data Flow
    Topic --> Readings
    Readings --> Group
    Group --> Temp & Humidity
    Temp & Humidity --> Window
    Window --> Temp1 & Temp2 & Temp3
    Window -.-> NotShown
    Temp1 & Temp2 & Temp3 --> Aggregate
    Aggregate --> Sum1 & Sum2 & Sum3
    Sum1 & Sum2 & Sum3 --> Average
    Average --> Avg1 & Avg2 & Avg3
    Avg1 -->|"key: {key: temp_01, startTime: 10:00:00, endTime: 10:02:00, ...}<br/>value: {average: 22.0, sample_count: 3, total_sum: 66.0}"| Output
    Avg2 & Avg3 --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Window,Aggregate,Average operation
    class Readings,Temp,Humidity,Temp1,Temp2,Temp3 message
    class Sum1,Sum2,Sum3,Avg1,Avg2,Avg3 result
    class NotShown note
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style ByWindow fill:none,stroke:#616161,stroke-dasharray:5 5
    style Sums fill:none,stroke:#616161,stroke-dasharray:5 5
    style Averages fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "Sensor data producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/producer-sensor-data.yaml" %}
    ```

??? info "Hopping window average processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/processor-hopping-average-working.yaml" %}
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

The diagram below follows seven example clicks through the same pipeline. Alice pauses for more than 2 minutes between 10:01:30 and 10:05:00, so her clicks form two separate sessions. Bob never pauses that long, so all his clicks stay in one session that keeps growing. Every click sends an updated count for its session, and the diagram shows the last one. When a click extends a session, the session gets a new end time and so a new key. The old session key then gets a `null` value:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with seven clicks
    Topic[(user_clicks)]
    Clicks@{ shape: docs, label: "<b>key: alice</b> click at 10:00:00<br/><b>key: bob</b> click at 10:00:20<br/><b>key: alice</b> click at 10:00:40<br/><b>key: alice</b> click at 10:01:30<br/><b>key: bob</b> click at 10:01:50<br/><b>key: bob</b> click at 10:03:10<br/><b>key: alice</b> click at 10:05:00" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Alice@{ shape: docs, label: "<b>key: alice</b><br/>10:00:00, 10:00:40,<br/>10:01:30, 10:05:00" }
        Bob@{ shape: docs, label: "<b>key: bob</b><br/>10:00:20, 10:01:50, 10:03:10" }
    end

    %% Step 2: split each group into sessions at gaps longer than 2 minutes
    Window["<code>windowBySession</code><br/>inactivityGap 2m"]
    subgraph BySession["Grouped by key and session"]
        Alice1@{ shape: docs, label: "<b>alice</b> 10:00:00 to 10:01:30<br/>10:00:00, 10:00:40, 10:01:30" }
        Alice2@{ shape: doc, label: "<b>alice</b> 10:05:00 to 10:05:00<br/>10:05:00" }
        Bob1@{ shape: docs, label: "<b>bob</b> 10:00:20 to 10:03:10<br/>10:00:20, 10:01:50, 10:03:10" }
    end

    %% Step 3: one count per key and session
    Count[<code>count</code>]
    subgraph Counts["Latest result per key and session"]
        AliceCount1["<b>alice</b> 10:00:00 to 10:01:30<br/>count: 3"]
        AliceCount2["<b>alice</b> 10:05:00 to 10:05:00<br/>count: 1"]
        BobCount1["<b>bob</b> 10:00:20 to 10:03:10<br/>count: 3"]
    end

    %% Step 4: write to the output topic with a JSON session key
    Convert["<code>toStream</code> + <code>convertKey</code><br/>json:windowed(string)"]
    Output[(user_session_summary)]

    %% Data Flow
    Topic --> Clicks
    Clicks --> Group
    Group --> Alice & Bob
    Alice & Bob --> Window
    Window -->|"gap under 2m"| Alice1
    Window -->|"gap of 3m30s:<br/>new session"| Alice2
    Window -->|"gaps under 2m"| Bob1
    Alice1 & Alice2 & Bob1 --> Count
    Count --> AliceCount1 & AliceCount2 & BobCount1
    AliceCount1 & AliceCount2 & BobCount1 --> Convert
    Convert -->|"key: {key: alice, startTime: 10:00:00, endTime: 10:01:30, ...}<br/>value: 3"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Window,Count,Convert operation
    class Clicks,Alice,Bob,Alice1,Alice2,Bob1 message
    class AliceCount1,AliceCount2,BobCount1 result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style BySession fill:none,stroke:#616161,stroke-dasharray:5 5
    style Counts fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "User clicks producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/producer-user-clicks.yaml" %}
    ```

??? info "User session analysis processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml" %}
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

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Late event
    LateEvent[/"Late event<br/>timestamp 10:04"/]

    %% Decision
    Check{"Stream time before<br/>10:06?<br/>(window end + grace)"}

    %% Outcomes
    Accepted[Added to window<br/>10:00 to 10:05]
    Drop[❌ Dropped]

    %% Data Flow
    LateEvent --> Check
    Check -->|"yes"| Accepted
    Check -.->|"no: window closed"| Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Check,Accepted operation
    class LateEvent external
    class Drop dropped
```

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