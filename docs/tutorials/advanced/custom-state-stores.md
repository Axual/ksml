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
- **Creates time windows**: Divides timestamps into 5-minute buckets (300,000ms), creates unique window keys like "server1:cpu_usage:1640995200000" 
- **Accumulates window stats**: For each metric in a time window, stores running count, sum, min, max, and recent sample list in state store
- **Calculates aggregates**: When outputting, computes average from sum/count, range from max-min, tracks categorical data like datacenter/environment
- **Outputs window results**: Returns complete window statistics only when window has enough samples, showing aggregated metrics with alerting thresholds and metadata

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

- **Produces order events**: Creates order status updates (created, shipped, delivered) with product IDs, quantities, prices as pipe-delimited strings
- **Uses three state stores**: Updates order_state_store (current order status), customer_metrics_store (totals per customer), product_inventory_store (stock levels)
- **Coordinates updates**: For each order event, atomically updates all three stores - order status, customer spending totals, inventory levels
- **Tracks relationships**: Maps orders to customers via hash function, maintains order history lists, tracks inventory changes per product
- **Outputs comprehensive results**: Returns formatted string combining data from all three stores showing order details, customer analytics, and inventory impact

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