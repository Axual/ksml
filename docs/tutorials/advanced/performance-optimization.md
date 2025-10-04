# Performance Optimization in KSML

This tutorial covers optimization techniques for KSML applications to improve throughput, reduce latency, and minimize resource usage.

## Introduction

Performance optimization helps you process more data efficiently while reducing costs and maintaining low latency under high loads.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic mixed_quality_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic filtered_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic enriched_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic final_processed_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic binary_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic optimized_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic readable_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_activity && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_metrics_summary && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic high_volume_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic processed_events && \
    ```

## Core Performance Concepts

### Identifying Bottlenecks

Before optimizing, identify where performance bottlenecks exist:

- **Processing time**: How long it takes to process each message
- **Throughput**: Messages processed per second
- **State store size**: How much data is stored in state stores
- **Memory usage**: JVM heap and non-heap memory
- **GC activity**: Frequency and duration of garbage collection

### Key Optimization Areas

1. **Python Function Efficiency**: Optimize code for minimal object creation and fast execution
2. **State Store Configuration**: Configure stores for your specific workload
3. **Pipeline Design**: Structure pipelines to filter early and process efficiently
4. **Data Serialization**: Choose efficient formats and minimize serialization overhead

## Efficient Processing Patterns

This example demonstrates optimized Python code and efficient data handling techniques.

**What it does:**

- **Produces high-volume events**: Creates user events (click, purchase, view) with categories, values, but also includes low-value events (scroll, hover)
- **Filters early**: Immediately discards uninteresting events (scroll, hover) to avoid processing overhead downstream
- **Pre-computes efficiently**: Uses global dictionaries for priority events and category multipliers, calculates scores with minimal object creation
- **Processes with JSON**: Extracts fields from JSON using `.get()`, builds result objects for Kowl UI readability
- **Logs selectively**: Only logs high-score events to reduce I/O overhead while maintaining performance visibility

??? info "High-volume producer (compact format) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/producer-high-volume.yaml" %}
    ```

??? info "Efficient processor (optimized Python) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/processor-efficient-processing.yaml" %}
    ```

**Key optimization techniques:**

- **Global code optimization**: Pre-compute expensive operations outside processing loops
- **Early filtering**: Discard unwanted events immediately to reduce downstream processing
- **Efficient logging**: Log only important events to reduce I/O overhead

### State Store Optimization

This example shows how to configure and use state stores efficiently for high-performance scenarios.

**What it does:**

- **Produces activity events**: Creates user activities (login, page_view, click, purchase, logout) with scores, durations, timestamps in JSON format
- **Stores metrics compactly**: Converts JSON to compact string format "total_count:login_count:page_view_count:click_count:purchase_count:logout_count:total_score"
- **Updates efficiently**: Parses compact string to array, updates counters by index mapping, avoids object creation during updates
- **Uses optimized store**: Configures state store with increased cache size (16MB) and segments (32) for better performance
- **Outputs JSON summaries**: Returns readable JSON results with averages and totals while keeping internal storage compact

??? info "User metrics producer (activity data) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/producer-user-metrics.yaml" %}
    ```

??? info "Optimized state processor (compact storage) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/processor-optimized-state.yaml" %}
    ```

**State store optimizations:**

- **JSON input/output**: Uses JSON for better debugging while maintaining compact string storage internally
- **Hybrid approach**: JSON for input/output messages, compact strings for state storage efficiency
- **Increased cache**: Configure larger cache sizes (50MB) for better performance  
- **Direct field access**: Extract JSON fields using `value.get()` instead of string parsing
- **Conditional logging**: Log only significant changes to reduce I/O

### Pipeline Optimization

This example demonstrates optimized pipeline design with early filtering and staged processing.

**What it does:**

- **Produces mixed quality events**: Creates events with valid/invalid data, various priorities (high, low, spam), different event types for filtering tests
- **Filters early**: Uses predicate function to immediately discard invalid events, spam, and bot traffic before expensive processing
- **Processes in stages**: Stage 1 = lightweight enrichment (add status, extract fields), Stage 2 = heavy processing (complex calculations)
- **Separates concerns**: Lightweight operations (field extraction) happen first, expensive operations (calculations) happen on filtered data only
- **Outputs progressively**: filtered_events → enriched_events → final_processed_events, each stage adds more data while maintaining JSON readability

??? info "Mixed events producer (quality testing) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/producer-mixed-events.yaml" %}
    ```

??? info "Pipeline optimization processor (staged processing) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/processor-pipeline-optimization.yaml" %}
    ```

**Pipeline design principles:**

- **Filter early**: Remove unwanted data before expensive processing using JSON field checks
- **Staged processing**: Separate lightweight and heavy operations into different stages
- **Efficient predicates**: Use `value.get()` for fast field-based filtering

### Serialization Optimization

This example shows efficient data format usage and minimal serialization overhead.

**What it does:**

- **Produces compact data**: Creates events with numeric event_type_ids (1=view, 2=click, 3=purchase) instead of strings for efficiency
- **Uses lookup tables**: Pre-computes event type mappings in globalCode for fast ID-to-name conversions without string operations
- **Filters by score**: Early filtering discards events with score <10 to reduce processing volume
- **Processes by type**: Applies different logic based on event_type_id (purchases get 10% value bonus + 20 score bonus)

??? info "Binary data producer (compact format) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/producer-binary-data.yaml" %}
    ```

??? info "Serialization optimization processor (efficient parsing) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/performance-optimization/processor-serialization-optimization.yaml" %}
    ```

**Serialization best practices:**

- **Lookup tables**: Pre-compute mappings to avoid repeated string operations
- **Progressive transformation**: Transform data from raw → optimized → final readable formats
- **Field-based access**: Use `value.get()` instead of string parsing for cleaner, faster code

## Configuration Optimization

### Kafka Streams Configuration

Optimize Kafka Streams configuration for your workload based on KSML's internal configuration system:

```yaml
runner:
  type: streams
  config:
    application.id: optimized-ksml-app
    bootstrap.servers: kafka:9092

    # Threading Configuration
    num.stream.threads: 8                    # Parallel processing threads (default: 1)
    
    # State Store Caching
    cache.max.bytes.buffering: 104857600     # 100MB total cache (default: 10MB)
    commit.interval.ms: 30000                # Commit frequency in ms (default: 30000)
    
    # Topology Optimization
    topology.optimization: all               # Enable all optimizations (default: all)
    
    # Producer Performance Settings
    producer.linger.ms: 100                  # Wait for batching (default: 0)
    producer.batch.size: 16384               # Batch size in bytes (default: 16384)
    producer.buffer.memory: 33554432         # 32MB producer buffer (default: 32MB)
    producer.acks: 1                         # Acknowledgment level (1 = leader only)
    
    # Consumer Performance Settings
    consumer.fetch.max.bytes: 52428800       # 50MB max fetch (default: 52428800)
    consumer.max.poll.records: 500           # Records per poll (default: 500)
    consumer.session.timeout.ms: 45000       # Session timeout (default: 45000)
    consumer.heartbeat.interval.ms: 3000     # Heartbeat frequency (default: 3000)
    
    # Processing Guarantees
    processing.guarantee: at_least_once       # Options: at_least_once, exactly_once_v2
    
    # Error Handling
    default.deserialization.exception.handler: org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
    default.production.exception.handler: org.apache.kafka.streams.errors.DefaultProductionExceptionHandler
```

**Key Configuration Details:**

- **num.stream.threads**: Controls parallelism. Set based on CPU cores and partition count
- **cache.max.bytes.buffering**: KSML automatically enables caching when `caching: true` is set on stores
- **topology.optimization**: KSML defaults to `StreamsConfig.OPTIMIZE` for performance
- **commit.interval.ms**: Balance between throughput and latency. Higher values = better throughput
- **processing.guarantee**: `exactly_once_v2` provides stronger guarantees but lower performance

**KSML-Specific Optimizations:**

KSML automatically configures several optimizations:

- Custom exception handlers for production and deserialization errors
- Automatic header cleanup interceptors for all consumers
- Optimized client suppliers for resolving configurations
- Built-in metrics reporters with topology enrichment

### State Store Configuration

Configure state stores for optimal performance based on KSML's store configuration system:

```yaml
stores:
  # High-performance key-value store
  optimized_keyvalue_store:
    type: keyValue
    keyType: string
    valueType: string                # Use string over JSON for performance-critical stores
    persistent: true                 # Enable durability (uses RocksDB)
    caching: true                    # Enable caching for frequent access
    logging: true                    # Enable changelog for fault tolerance
    timestamped: false               # Disable if timestamps not needed
    versioned: false                 # Disable if versioning not needed
    
  # Memory-optimized store for temporary data
  temp_cache_store:
    type: keyValue
    keyType: string
    valueType: string
    persistent: false                # In-memory only for speed
    caching: true                    # Still enable caching
    logging: false                   # No changelog needed for temp data
    
  # Window store with retention
  metrics_window_store:
    type: window
    keyType: string
    valueType: json                  # JSON acceptable for metrics
    windowSize: PT5M                 # 5-minute windows
    retention: PT1H                  # Keep 1 hour of data
    persistent: true
    caching: true
    logging: true
    
  # Session store for user sessions  
  user_session_store:
    type: session
    keyType: string
    valueType: string                # Compact string format for sessions
    retention: PT30M                 # 30-minute session timeout
    persistent: true
    caching: true
    logging: true
```

**Store Type Performance Characteristics:**

| Store Type | Use Case | Performance Notes |
|------------|----------|-------------------|
| **keyValue** | General caching, counters, lookups | Fastest access, least memory overhead |
| **window** | Time-based aggregations | Automatic cleanup, good for time-series |
| **session** | User sessions, activity tracking | Session-aware, handles gaps automatically |

**Configuration Parameter Details:**

- **persistent**: 
      - `true`: Uses RocksDB for disk persistence, survives restarts
      - `false`: In-memory only, faster but data lost on restart
  
- **caching**: 
      - `true`: KSML enables write caching to reduce downstream traffic
      - `false`: Direct writes, lower latency but higher network usage
  
- **logging**: 
      - `true`: Creates changelog topic for fault tolerance and exactly-once processing
      - `false`: No changelog, faster but no fault tolerance
  
- **timestamped**: 
      - `true`: Stores values with timestamps for time-aware processing
      - `false`: Plain values, more efficient when timestamps not needed
  
- **versioned**: 
      - `true`: Maintains multiple versions of values with configurable retention
      - `false`: Single version only, more memory efficient

**KSML Store Optimizations:**

KSML automatically optimizes store configuration:

- Uses efficient Serde types based on configured data types
- Applies proper supplier selection (persistent vs. in-memory)
- Configures caching and logging based on specified options
- Handles timestamped and versioned variants automatically

## Python Function Best Practices

### Optimize Code Structure

```yaml
functions:
  optimized_function:
    type: valueTransformer
    globalCode: |
      # Pre-compute expensive operations
      MULTIPLIERS = {"high": 3.0, "medium": 2.0, "low": 1.0}
      
      def efficient_calculation(value, priority):
        return value * MULTIPLIERS.get(priority, 1.0)
    
    code: |
      # Use pre-computed values and avoid object creation
      priority = value.get("priority", "low")
      result = efficient_calculation(value.get("amount", 0), priority)
      return f"processed:{result:.2f}"
```

Key techniques:

- **Use globalCode**: Pre-compute expensive operations outside the processing loop
- **Minimize object creation**: Reuse objects and data structures when possible
- **Use built-in functions**: They're typically faster than custom implementations
- **Avoid unnecessary computations**: Compute values only when needed

### Efficient Data Structures

Choose optimal data structures based on KSML's data type system and your use case:

#### KSML Data Types (Performance Ranking)

```text
# Fastest - Use for high-throughput scenarios
keyType: string         # Most efficient for keys
valueType: string       # Fastest serialization

# Fast - Good balance of performance and functionality  
keyType: long          # Efficient numeric keys
valueType: avro:Schema # Efficient binary format with schema

# Moderate - Flexible but more overhead
keyType: json          # Flexible structure  
valueType: json        # Easy to work with, good for Kowl UI

# Slower - Use only when necessary
valueType: xml         # XML processing overhead
valueType: soap        # SOAP envelope overhead
```

#### Python Data Structure Guidelines

Optimal patterns for different scenarios:

1. Fast counters/accumulators
```python
count = int(store.get(key) or "0")  # String storage, int operations
count += 1
store.put(key, str(count))
```

2. Compact State Representation
```python
# Instead of JSON: {"count": 5, "sum": 100, "avg": 20}
compact_state = f"{count}:{total_sum}:{average}"  # String format
store.put(key, compact_state)
```

3. Efficient Collections (limit size)
```python
items = json.loads(store.get(key) or "[]")
items.append(new_item)
items = items[-100:]  # Keep only last 100 items
store.put(key, json.dumps(items))
```

4. Fast Lookup Tables (use globalCode)
```python
STATUS_CODES = {1: "active", 2: "inactive", 3: "pending"}  # Pre-computed
status = STATUS_CODES.get(status_id, "unknown")  # O(1) lookup
```

5. Efficient String Operations
```python
# avoid
Avoid: result = f"processed:{type}:{user}:{score}"
# use this instead
Use: result = "processed:" + type + ":" + user + ":" + str(score)
```

#### Memory-Efficient Patterns

Optimal memory usage techniques:

1. Reuse Objects
```python
result = {"status": "ok", "count": 0}  # Create once in globalCode
result["count"] = new_count            # Reuse, don't recreate
```

2. Generator Patterns
```python
def process_batch(items):
    for item in items:  # Memory efficient iteration
        yield process_item(item)
```
3. Lazy Evaluation 
```python
expensive_result = None
if condition_needs_it:  # Only compute when needed
    expensive_result = expensive_calculation()
```

4. Compact Data Types
```python
# use this:
timestamp = int(time.time())      # 4-8 bytes
# instead of this
timestamp = str(time.time())      # ~20 bytes + overhead
```

**KSML-Specific Optimizations:**

- **Serde Selection**: KSML automatically chooses optimal serializers based on data type
- **Union Types**: For flexible schemas, KSML uses efficient UnionSerde implementation  
- **Type Flattening**: Complex types are automatically flattened for performance
- **Caching**: Serde instances are cached and reused across operations

**Data Type Performance Comparison:**

| Data Type                 | Serialization Speed | Size Efficiency | Schema Evolution | Use Case |
|---------------------------|-------------------|-----------------|------------------|----------|
| **string**                | Fastest | Good | Limited | Simple values, IDs, compact formats |
| **long/int**              | Fastest | Excellent | None | Counters, timestamps, numeric keys |
| **avro**                  | Fast | Excellent | Excellent | Complex schemas, production systems |
| **json**                  | Moderate | Good | Good | Development, debugging, flexible data |
| **protobuf(coming soon)** | Fast | Excellent | Good | High-performance, cross-language |
| **xml/soap**              | Slow | Poor | Limited | Legacy systems, specific protocols |

## Monitoring Performance

### Key Metrics to Track

Monitor these metrics to identify performance issues:

```yaml
functions:
  performance_monitor:
    type: forEach
    code: |
      # Record processing time
      start_time = time.time()
      process_message(key, value)
      processing_time = (time.time() - start_time) * 1000
      
      # Log performance metrics periodically
      if processing_time > 100:  # Log slow operations
        log.warn("Slow processing detected: {:.2f}ms for key {}", processing_time, key)
```

Important metrics:

- **Processing latency**: Time to process individual messages
- **Throughput**: Messages processed per second
- **Memory usage**: JVM heap utilization
- **State store size**: Growth rate and total size
- **Error rates**: Failed processing attempts

## Best Practices Summary

### Do's

- **Measure first**: Establish baselines before optimizing
- **Filter early**: Remove unwanted data as soon as possible
- **Use caching**: Configure appropriate cache sizes for state stores
- **Optimize hot paths**: Focus on frequently executed code
- **Pre-compute values**: Move expensive operations to globalCode
- **Choose efficient formats**: Use compact data representations

### Don'ts

- **Don't over-optimize**: Focus on actual bottlenecks, not theoretical ones
- **Don't ignore trade-offs**: Balance throughput, latency, and resource usage
- **Don't optimize without measuring**: Always measure the impact of changes
- **Don't create unnecessary objects**: Reuse data structures when possible
- **Don't log excessively**: Reduce I/O overhead from logging
- **Don't use complex serialization**: Avoid JSON when simple formats suffice

## Conclusion

Performance optimization in KSML involves a combination of efficient Python code, optimized configuration, and smart pipeline design. By applying these techniques systematically and measuring their impact, you can build KSML applications that efficiently process high volumes of data with consistent performance.

The key is to start with measurement, identify actual bottlenecks, and apply optimizations incrementally while monitoring their effectiveness.