# Custom State Stores in KSML

This tutorial explores how to implement and optimize custom state stores in KSML, allowing you to maintain and manage state in your stream processing applications with greater flexibility and control.

## Introduction to State Stores

State stores are a critical component of stateful stream processing applications. They allow your application to:

- **Maintain data** across multiple messages and events
- **Track historical information** for context-aware processing  
- **Implement stateful operations** like aggregations and joins
- **Build sophisticated business logic** that depends on previous events
- **Persist state** for fault tolerance and recovery

KSML provides built-in state store capabilities that integrate seamlessly with Kafka Streams, offering exactly-once processing guarantees and automatic state management.

All examples in this tutorial use the same pipeline shape. A Python function reads and updates one or more state stores for every incoming message, and returns either a result or `None`. A `filter` then drops the `None` results:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(Input topic)]

    %% Pipeline Operations
    Function["<code>mapValues</code><br/>Python function<br/>declares <code>stores:</code>"]
    Store[("keyValue state store<br/>JSON string per key")]
    Filter{"<code>filter</code><br/>value is not None"}

    %% Outputs
    Output[(Output topic)]
    Drop[❌ Dropped]

    %% Data Flow
    Input --> Function
    Function <-.->|"get & put"| Store
    Function -->|"result or None"| Filter
    Filter -->|"result"| Output
    Filter -.->|"None"| Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Input,Output topic
    class Function,Filter operation
    class Store store
    class Drop dropped
```

All examples use a `keyValue` store, including the "Window Store" and "Session Store" examples: those implement time windows and sessions themselves in Python, by choosing the store key and deciding when a window or session ends.

## Prerequisites

Before starting this tutorial:

- **Complete the [State Stores Tutorial](../intermediate/state-stores.md)** - This tutorial builds on fundamental state store concepts, configuration methods, and basic patterns covered in the intermediate tutorial
- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_activity && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_session_stats && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic server_metrics && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic windowed_metrics && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_clicks && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic session_analytics && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic device_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic device_alerts && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic order_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic order_processing_results && \
    ```

## Advanced Key-Value Store Patterns

Building on the basic key-value concepts from the [State Stores Tutorial](../intermediate/state-stores.md), this example demonstrates advanced user session tracking with complex business logic.

**What it does**:

- **Produces user activities**: Creates events like login, page_view, click with user IDs, session IDs (changes every 10 events), browser/device info
- **Stores user profiles**: Keeps running JSON data per user including total sessions, action counts, time spent, devices/browsers used 
- **Detects session changes**: When session_id changes from previous event, increments session counter and logs the transition
- **Tracks comprehensive stats**: Updates action counters, adds new pages/devices/countries to lists, calculates total time across all sessions
- **Outputs session updates**: Returns enriched user profile showing current session, lifetime statistics, and behavioral patterns whenever activity occurs

The diagram below follows three example activities for one user. Every activity updates the stored profile and produces an updated profile as output:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(user_activity)]
    Events@{ shape: docs, label: "<b>key: alice</b><br/>login, session_alice_0, 2s<br/>page_view, session_alice_0, 5s<br/>click, session_alice_1, 1s" }

    %% Processing function and its store
    Function["<code>mapValues</code><br/>track_user_sessions"]
    Store[("user_session_store<br/>key: alice<br/>value: profile as JSON")]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>login</b>, session_alice_0<br/>current_session: session_alice_0<br/>total_sessions: 0<br/>actions: login 1<br/>total_time: 2s"]
        Step2["<b>page_view</b>, session_alice_0<br/>same session<br/>actions: login 1, page_view 1<br/>total_time: 7s"]
        Step3["<b>click</b>, session_alice_1<br/>session changed: session_alice_0 ended<br/>total_sessions: 1<br/>current_session: session_alice_1<br/>total_time: 8s"]
        Step1 --> Step2 --> Step3
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(user_session_stats)]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function <-.->|"read & update"| Store
    Function --> Steps
    Steps --> Filter
    Filter -->|"3 updated profiles,<br/>one per activity"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class Store store
    class Step1,Step2 message
    class Step3 partial
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

The `total_sessions` counter only counts sessions that have ended: it goes up when a new `session_id` arrives for the user.

??? info "User Activity Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/producer-user-sessions.yaml" %}
    ```

??? info "Basic Key-Value Store Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/processor-basic-keyvalue.yaml" %}
    ```

**Key concepts demonstrated**:

- JSON serialization for complex state objects
- Session boundary detection
- Persistent state with caching enabled

## Window Store

Window stores organize data by time windows, enabling time-based aggregations and analytics.

**What it does**:

- **Produces server metrics**: Creates metrics like cpu_usage, memory_usage, disk_io with varying values (base + sine wave + noise) for different servers
- **Creates time windows**: Divides timestamps into 5-minute buckets (300,000ms), creates unique window keys like "server_001:cpu_usage:1640995200000" 
- **Accumulates window stats**: For each metric in a time window, stores running count, sum, min, max, and recent sample list in state store
- **Calculates aggregates**: When outputting, computes average from sum/count, range from max-min, tracks categorical data like datacenter/environment
- **Outputs window results**: Returns the current window statistics for every metric, with alerting thresholds and metadata

The diagram below follows four example metrics for one server. The function puts the 5-minute window start into the store key, so every combination of server, metric and window gets its own entry in a `keyValue` store. The diagram shows times as `10:00`; the real keys contain the window start in epoch milliseconds:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(server_metrics)]
    Events@{ shape: docs, label: "<b>key: server_001</b><br/>cpu_usage 40 at 10:01<br/>memory_usage 70 at 10:02<br/>cpu_usage 60 at 10:03<br/>cpu_usage 50 at 10:06" }

    %% Processing function and its store
    Function["<code>mapValues</code><br/>aggregate_metrics"]
    Store[("metrics_window_store<br/>key: server:metric:window_start<br/>value: window statistics as JSON")]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>cpu_usage 40</b> at 10:01<br/>key server_001:cpu_usage:10:00<br/>new window<br/>count 1, avg 40"]
        Step2["<b>memory_usage 70</b> at 10:02<br/>key server_001:memory_usage:10:00<br/>new window<br/>count 1, avg 70"]
        Step3["<b>cpu_usage 60</b> at 10:03<br/>key server_001:cpu_usage:10:00<br/>same window<br/>count 2, avg 50, min 40, max 60"]
        Step4["<b>cpu_usage 50</b> at 10:06<br/>key server_001:cpu_usage:10:05<br/>new window<br/>count 1, avg 50"]
        Step1 --> Step2 --> Step3 --> Step4
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(windowed_metrics)]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function <-.->|"read & update"| Store
    Function --> Steps
    Steps --> Filter
    Filter -->|"4 window updates,<br/>one per metric"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class Store store
    class Step1,Step2,Step4 message
    class Step3 partial
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

Every metric produces an output message with the current statistics of its window, so the same window can appear several times in the output topic, each time with more samples.

??? info "Metrics Data Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/producer-metrics-data.yaml" %}
    ```

??? info "Window Store Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/processor-window-store.yaml" %}
    ```

**Key concepts demonstrated**:

- Time window calculation and management
- Running aggregations (min, max, sum, count)
- Memory-efficient value storage

## Session Store

Session stores organize data by session windows, automatically handling session boundaries based on inactivity gaps.

**What it does**:

- **Produces click events**: Creates user page visits with timestamps, occasionally adding 1-5 second gaps (20% chance) to simulate session breaks
- **Tracks session timeouts**: Uses 30-second inactivity threshold - if time since last click > 30s, starts new session and increments session counter
- **Stores session state**: Keeps running data per user including current session ID, start time, page count, total duration, devices used
- **Detects session boundaries**: When timeout exceeded, logs session end, resets counters, starts fresh session with new session ID
- **Outputs session analytics**: Returns comprehensive session data showing current session metrics, lifetime totals, device/page patterns, and conversion events

The diagram below follows four example clicks for one user. The function compares each click with the time of the previous one: a gap of more than 30 seconds ends the current session and starts a new one:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(user_clicks)]
    Events@{ shape: docs, label: "<b>key: user_A</b><br/>click at 10:00:00<br/>click at 10:00:20<br/>click at 10:01:30<br/>click at 10:01:40" }

    %% Processing function and its store
    Function["<code>mapValues</code><br/>track_user_sessions"]
    Store[("user_session_store<br/>key: user_A<br/>value: session state as JSON")]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>10:00:00</b><br/>no session yet<br/>start session_user_A_10:00:00<br/>page_count 1"]
        Step2["<b>10:00:20</b><br/>20s since last click<br/>same session<br/>page_count 2"]
        Step3["<b>10:01:30</b><br/>70s since last click<br/>more than 30s: session ended<br/>start session_user_A_10:01:30<br/>total_sessions 1, page_count 1"]
        Step4["<b>10:01:40</b><br/>10s since last click<br/>same session<br/>page_count 2"]
        Step1 --> Step2 --> Step3 --> Step4
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(session_analytics)]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function <-.->|"read & update"| Store
    Function --> Steps
    Steps --> Filter
    Filter -->|"4 session updates,<br/>one per click"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class Store store
    class Step1,Step2,Step4 message
    class Step3 partial
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

The new session ID is built from the user and the timestamp of the click that starts it.

??? info "User Clicks Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/producer-user-clicks.yaml" %}
    ```

??? info "Session Store Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/processor-session-store.yaml" %}
    ```

**Key concepts demonstrated**:

- Session timeout handling
- Automatic session boundary detection
- Session lifecycle management

## Optimized Store Configuration

For high-volume scenarios, proper store configuration is crucial for performance.

**What it does**:

- **Produces device events**: Creates high-frequency events (sensor_reading, status_update, error, heartbeat) for multiple devices with facility/zone info
- **Stores compact state**: Keeps minimal JSON per device with just current status, last_temp, error_count, heartbeat_count, location info
- **Processes selectively**: Updates state for all events, but only outputs alerts when specific conditions met (temp >75°C, errors, status changes)
- **Optimizes for volume**: Uses efficient JSON storage, processes fast, emits only critical alerts to reduce downstream message volume  
- **Tracks device health**: Monitors temperature trends, error accumulation, heartbeat patterns, status transitions with location context

The diagram below follows five example events for one device. The device state is updated for every event, but only some events produce an alert:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(device_events)]
    Events@{ shape: docs, label: "<b>key: device_001</b><br/>sensor_reading 72<br/>sensor_reading 78<br/>heartbeat<br/>error<br/>status_update: maintenance" }

    %% Processing function and its store
    Function["<code>mapValues</code><br/>process_device_events"]
    Store[("device_state_store<br/>key: device_001<br/>value: compact state as JSON")]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>sensor_reading 72</b><br/>last_temp 72, status active<br/>75 or lower: returns None"]
        Step2["<b>sensor_reading 78</b><br/>last_temp 78<br/>above 75:<br/>TEMPERATURE_ALERT"]
        Step3["<b>heartbeat</b><br/>heartbeat_count 1, status online<br/>not a 10th heartbeat:<br/>returns None"]
        Step4["<b>error</b><br/>error_count 1, status error<br/>ERROR alert"]
        Step5["<b>status_update</b><br/>status maintenance<br/>STATUS_UPDATE alert"]
        Step1 --> Step2 --> Step3 --> Step4 --> Step5
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(device_alerts)]
    Drop["❌ Dropped<br/>sensor_reading 72, heartbeat"]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function <-.->|"read & update"| Store
    Function --> Steps
    Steps --> Filter
    Filter -->|"3 alerts"| Output
    Filter -.->|"None"| Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class Store store
    class Step1,Step3 message
    class Step2,Step4,Step5 result
    class Drop dropped
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

Every 10th heartbeat also produces a `HEARTBEAT_SUMMARY`. Because the state is updated even when nothing is emitted, the next alert for the device always has the latest temperature, error count and status.

??? info "High Volume Events Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/producer-high-volume.yaml" %}
    ```

??? info "Optimized Store Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/processor-optimized-store.yaml" %}
    ```

**Key concepts demonstrated**:

- Compact state representation for performance
- Selective event emission
- Error counting and alerting

## Multi-Store Pattern

Complex applications often require multiple state stores working together to manage different aspects of state.

**What it does**:

- **Produces order events**: Creates order status updates (created, shipped, delivered) with product IDs, quantities, prices as colon-separated strings
- **Uses three state stores**: Updates order_state_store (current order status), customer_metrics_store (totals per customer), product_inventory_store (stock levels)
- **Coordinates updates**: For each order event, atomically updates all three stores - order status, customer spending totals, inventory levels
- **Tracks relationships**: Maps orders to customers via hash function, maintains order history lists, tracks inventory changes per product
- **Outputs comprehensive results**: Returns formatted string combining data from all three stores showing order details, customer analytics, and inventory impact

The diagram below follows two example events for the same order. Each event is a colon-separated string, and the function updates all three stores for every event:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with two events for one order
    Topic[(order_events)]
    Events@{ shape: docs, label: "<b>key: order_0001</b><br/>created:prod_A:2:19.99:...<br/>shipped:prod_A:2:19.99:..." }

    %% Processing function
    Function["<code>mapValues</code><br/>process_order_with_multiple_stores"]

    %% Store updates per event
    subgraph Steps["Event by event, all three stores are updated"]
        direction LR
        Created["<b>created</b><br/><br/><b>order_state_store</b><br/>order_0001: created<br/><br/><b>customer_metrics_store</b><br/>customer of order_0001: total_orders + 1<br/><br/><b>product_inventory_store</b><br/>prod_A: reserved 2, available 100 → 98"]
        Shipped["<b>shipped</b><br/><br/><b>order_state_store</b><br/>order_0001: shipped<br/><br/><b>customer_metrics_store</b><br/>no change<br/><br/><b>product_inventory_store</b><br/>prod_A: reserved 2 → 0, sold 2"]
        Created --> Shipped
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(order_processing_results)]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function --> Steps
    Steps --> Filter
    Filter -->|"ORDER_PROCESSED:order_0001:status=shipped:<br/>customer_orders=...:stock_left=98"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class Created,Shipped result
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

Each event also produces one output string, so the two events above produce two `ORDER_PROCESSED` messages. The customer is derived from a hash of the order ID, and a product that is seen for the first time starts with a stock of 100.

??? info "Order Events Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/producer-order-events.yaml" %}
    ```

??? info "Multi-Store Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/custom-state-stores/processor-multi-store.yaml" %}
    ```

**Key concepts demonstrated**:

- Multiple state stores in single function
- Coordinated state updates
- Cross-store data correlation

## State Store Types Summary

| Store Type | Use Case | Key Features |
|------------|----------|--------------|
| **Key-Value** | General state, caching, counters | Simple key-to-value mapping |
| **Window** | Time-based aggregations | Automatic time partitioning |
| **Session** | User sessions, activity tracking | Inactivity-based boundaries |

## Conclusion

Custom state stores in KSML provide powerful capabilities for building stateful stream processing applications. By understanding the different store types, configuration options, and optimization techniques, you can build efficient and scalable applications that maintain state effectively across events.

For foundational concepts and basic configuration patterns, refer back to the [State Stores Tutorial](../intermediate/state-stores.md).