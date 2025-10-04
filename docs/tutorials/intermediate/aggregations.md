# Working with Aggregations in KSML

This tutorial explores how to compute statistics, summaries, and time-based analytics from streaming data using KSML's aggregation operations.

## Introduction

Aggregations are stateful operations that combine multiple records into summary values. They are fundamental to stream processing, enabling real-time analytics from continuous data streams.

KSML aggregations(stateful processing capabilities) are built on top of Kafka Streams aggregation operations.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic financial_transactions && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic transaction_sums && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic payment_amounts && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic payment_totals && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic payment_stream && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic payment_statistics && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_actions && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_action_counts && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic retail_sales && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sales_by_region && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_temperatures && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_window_stats && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_refunds && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_bonuses && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_totals && \
    ```

## Core Aggregation Concepts

### Grouping Requirements

All aggregations require data to be grouped by key first:

```yaml
# Group by existing key
- type: groupByKey

# Group by new key using mapper function
- type: groupBy
  mapper:
    expression: value.get("category")
    resultType: string
```

**Kafka Streams equivalents:**

- `groupByKey` → `KStream.groupByKey()`
- `groupBy` → `KStream.groupBy()`

### State Stores

Aggregations maintain state in local stores that are fault-tolerant through changelog topics. A changelog is a compacted Kafka topic that records every state change, allowing the state to be rebuilt if an instance fails or restarts. This provides exactly-once processing guarantees and enables automatic state recovery.

#### Key-Value Stores

Used for regular (non-windowed) aggregations:

```yaml
store:
  name: my_aggregate_store
  type: keyValue
  caching: true           # Enable caching to reduce downstream updates
  loggingDisabled: false  # Keep changelog for fault tolerance (default: false)
  persistent: true        # Use RocksDB for persistence (default: true)
```

#### Window Stores

Required for windowed aggregations to store time-based state:

```yaml
store:
  name: my_window_store
  type: window
  windowSize: 1h          # Must match the window duration
  retention: 24h          # How long to keep expired windows (>= windowSize + grace)
  retainDuplicates: false # Keep only latest value per window (default: false)
  caching: true           # Enable caching for better performance
```

**Important considerations:**

- `windowSize` must match your `windowByTime` duration
- `retention` should be at least `windowSize + grace period` to handle late-arriving data
- Longer retention uses more disk space but allows querying historical windows
- Caching reduces the number of downstream updates and improves performance

### Function Types in Aggregations

**Initializer Function**
Creates the initial state value when a key is seen for the first time.
```yaml
initializer:
  expression: {"count": 0, "sum": 0}  # Initial state
  resultType: json
```

**Aggregator Function**
Updates the aggregate state by combining the current aggregate with a new incoming value.
```yaml
aggregator:
  code: |
    # aggregatedValue: current aggregate
    # value: new record to add
    result = {
      "count": aggregatedValue["count"] + 1,
      "sum": aggregatedValue["sum"] + value["amount"]
    }
  expression: result
  resultType: json
```

**Reducer Function** (for reduce operations)
Combines two values of the same type into a single value, used when no initialization is needed.
```yaml
reducer:
  code: |
    # value1, value2: values to combine
    combined = value1 + value2
  expression: combined
  resultType: long
```

## Types of Aggregations in KSML

KSML supports several aggregation types, each with specific use cases:

### 1. Count
Counts the number of records per key.

**Kafka Streams equivalent:** `KGroupedStream.count()`

**When to use:**

- Tracking event frequencies
- Monitoring activity levels
- Simple counting metrics

### 2. Reduce
Combines values using a reducer function without an initial value.

**Kafka Streams equivalent:** `KGroupedStream.reduce()`

**When to use:**

- Summing values
- Finding min/max
- Combining values of the same type

### 3. Aggregate
Builds complex aggregations with custom initialization and aggregation logic.

**Kafka Streams equivalent:** `KGroupedStream.aggregate()`

**When to use:**

- Computing statistics (avg, stddev)
- Building complex state
- Transforming value types during aggregation

### 4. Cogroup
Aggregates multiple grouped streams together into a single result.

**Kafka Streams equivalent:** `CogroupedKStream`

**When to use:**

- Combining data from multiple sources
- Building unified aggregates from different streams
- Complex multi-stream analytics

## Count Example

Simple counting of events per key:

??? info "User actions producer (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-user-actions.yaml"
    %}
    ```

??? info "Count user actions processor (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-count.yaml"
    %}
    ```

### How Count Works

The count operation:

1. Groups messages by key using `groupByKey`
2. Maintains a counter per unique key
3. Increments the counter for each message
4. Outputs the current count as a KTable

## Reduce Example

The `reduce` operation combines values without initialization, making it perfect for operations like summing, finding minimums/maximums, or concatenating strings. This section shows two approaches: a simple binary format for efficiency, and a JSON format for human readability.

### Simple Reduce (Binary Format)

This example demonstrates the core reduce concept with minimal complexity, using binary long values for efficiency.

**What it does:**

1. **Generates transactions**: Creates random transaction amounts as long values (cents)
2. **Groups by account**: Groups transactions by account_id (the message key)  
3. **Reduces values**: Sums all transaction amounts using a simple reducer
4. **Outputs totals**: Writes aggregated totals as long values

**Key KSML concepts demonstrated:**

- `groupByKey` for partitioning data by key
- `reduce` operation for stateful aggregation
- Binary data types for processing efficiency

??? info "Simple producer (binary long values) - click to expand"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/producer-simple.yaml"
    %}
    ```

??? info "Simple processor (reduce only) - click to expand"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/processor-simple.yaml"
    %}
    ```

**Verifying the results:**

Since binary data isn't human-readable in Kowl UI, use command-line tools to verify:

```bash
# Check current totals
kcat -b localhost:9092 -t transaction_sums -C -o end -c 5 -f 'Key: %k, Total: %s\n' -s value=Q

# Verify by summing all transactions for one account
kcat -b localhost:9092 -t financial_transactions -C -o beginning -f '%k,%s\n' -s value=Q -e | \
  grep "ACC001" | cut -d',' -f2 | awk '{sum += $1} END {print "Sum:", sum}'
```

> **Note:** Binary formats like `long` are common in production for performance and storage efficiency.

### Human-Readable Reduce (JSON Format)

This example shows the same reduce logic but with JSON messages for better visibility in Kafka UI tools.

**Additional concepts demonstrated:**

- `transformValue` for data extraction and formatting  
- Type handling (JSON → long → JSON) for processing efficiency
- Human-readable message formats

??? info "JSON producer (human-readable) - click to expand"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-transactions.yaml"
    %}
    ```

??? info "JSON processor (with transformations) - click to expand"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml"
    %}
    ```

### Reduce vs Aggregate

Choose **reduce** when:

- Values are of the same type as the result
- No initialization is needed
- Simple combination logic (sum, min, max)

Choose **aggregate** when:

- Result type differs from input type
- Custom initialization is required
- Complex state management is needed

## Aggregate Example

The `aggregate` operation provides custom initialization and aggregation logic, making it perfect for building complex statistics or when input and output types differ. This section shows two approaches: a simple binary format for core concepts, and a JSON format for comprehensive statistics.

### Simple Aggregate (Binary Format)

This example demonstrates the core aggregate concept with minimal complexity, using binary long values.

**What it does:**

1. **Initializes to zero**: Starts aggregation with a zero value using simple expression
2. **Groups by customer**: Groups payment amounts by customer_id (the message key)
3. **Sums amounts**: Adds each payment amount to the running total
4. **Outputs totals**: Writes aggregated totals as long values

**Key KSML concepts demonstrated:**

- `initializer` with simple expression (no custom function needed)
- `aggregator` with simple arithmetic expression  
- Binary data types for processing efficiency

??? info "Simple producer (binary long values) - click to expand"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/producer-simple-aggregate.yaml"
    %}
    ```

??? info "Simple processor (aggregate only) - click to expand"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/processor-simple-aggregate.yaml"
    %}
    ```

**Verifying the results:**

Since binary data isn't human-readable in Kowl UI, use command-line tools to verify:

```bash
# Check current totals (convert cents to dollars)
kcat -b localhost:9092 -t payment_totals -C -o end -c 5 -f 'Key: %k, Total cents: %s\n' -s value=Q

# Calculate expected total for one customer
kcat -b localhost:9092 -t payment_amounts -C -o beginning -f '%k,%s\n' -s value=Q -e | \
  grep "CUST001" | cut -d',' -f2 | awk '{sum += $1} END {print "Expected:", sum}'
```

### Complex Aggregate (JSON Format)  

This example shows advanced aggregation with comprehensive statistics using JSON for human readability.

**Additional concepts demonstrated:**

- Custom `initializer` function for complex state initialization
- Custom `aggregator` function for multi-field updates
- JSON state management for rich aggregations
- Dynamic calculations (average) within aggregation logic

**What it does:**

1. **Initializes statistics**: Creates a JSON structure to track multiple metrics (count, total, min, max, average)
2. **Groups by customer**: Groups payment events by account_id (the message key)
3. **Updates statistics**: For each payment, updates all metrics in the aggregated state
4. **Calculates average**: Dynamically computes the average amount per customer
5. **Outputs comprehensive stats**: Produces JSON messages with complete payment statistics

??? info "JSON payment events producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/producer-payments.yaml"
    %}
    ```

??? info "JSON statistics processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/processor-aggregate-stats.yaml"
    %}
    ```

Both approaches demonstrate the flexibility of the `aggregate` operation. The simple version focuses on the core concept, while the complex version shows real-world statistical aggregation with human-readable JSON output.

### Aggregate Components

1. **Initializer**: Creates empty/initial state
2. **Aggregator**: Updates state with each new value
3. **Result**: Continuously updated aggregate in state store

## Windowed Aggregations

Aggregations can be windowed to compute time-based analytics:

### Window Types

#### Tumbling Windows
Non-overlapping, fixed-size time windows.

```yaml
- type: windowByTime
  windowType: tumbling
  duration: 1h  # 1-hour windows
  grace: 5m     # Allow 5 minutes for late data
```

**Use cases:** Hourly reports, daily summaries

#### Hopping Windows
Overlapping, fixed-size windows that advance by a hop interval.

```yaml
- type: windowByTime
  windowType: hopping
  duration: 1h    # Window size
  advance: 15m    # Hop interval
  grace: 5m
```

**Use cases:** Moving averages, overlapping analytics

#### Session Windows
Dynamic windows based on periods of inactivity.

```yaml
- type: windowBySession
  inactivityGap: 30m  # Close window after 30 min of inactivity
  grace: 5m
```

**Use cases:** User sessions, activity bursts

### Windowed Aggregation Example

This example demonstrates windowed aggregation by calculating temperature statistics (count, average, min, max) for each sensor within 30-second time windows. The windowed key contains both the original sensor ID and the window time boundaries.

**What it does:**

1. **Groups by sensor**: Each sensor's readings are processed separately
2. **Windows by time**: Creates 30-second tumbling windows for aggregation
3. **Calculates statistics**: Tracks count, sum, average, min, and max temperature
4. **Transforms window key**: Converts WindowedString to a regular string key for output
5. **Writes to topic**: Outputs windowed statistics to `sensor_window_stats` topic

**Key KSML concepts demonstrated:**

- `windowByTime` with tumbling windows for time-based aggregation
- Window stores with configurable retention
- Accessing window metadata (start/end times) from the WindowedString key
- Transforming WindowedString keys using `map` with `keyValueMapper`
- Complex aggregation state using JSON

??? info "Temperature sensor producer (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-windowed.yaml"
    %}
    ```

??? info "Windowed temperature statistics processor (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml"
    %}
    ```

**Understanding the WindowedString key:**

When using windowed aggregations, the key becomes a `WindowedString` object containing:

- `key`: The original key (sensor ID)
- `start`/`end`: Window boundaries in milliseconds
- `startTime`/`endTime`: Human-readable UTC timestamps

This allows you to know exactly which time window each aggregation result belongs to.

**Output format:**

The output topic contains messages with:

- **Key**: `{sensor_id}_{window_start}_{window_end}` (e.g., `temp001_2025-08-12T18:58:00Z_2025-08-12T18:58:30Z`)
- **Value**: JSON object containing:
      - `sensor_id`: The sensor identifier
      - `window_start`/`window_end`: Window boundaries in UTC
      - `stats`: Aggregated statistics (count, avg, min, max)

Example output message:
```json
{
  "sensor_id": "temp001",
  "window_start": "2025-08-12T18:58:00Z",
  "window_end": "2025-08-12T18:58:30Z",
  "stats": {
    "count": 3,
    "avg": 24.5,
    "min": 19.6,
    "max": 28.7,
    "sum": 73.5
  }
}
```

**Verifying the input data:**

To check the raw sensor temperature readings (binary double values), use:

```bash
docker exec broker kafka-console-consumer.sh \
  --bootstrap-server broker:9093 \
  --topic sensor_temperatures \
  --from-beginning \
  --max-messages 10 \
  --property print.key=true \
  --property key.separator=" | " \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.DoubleDeserializer
```

Example input messages:
```
temp001 | 24.5
temp002 | 31.2
temp003 | 18.7
temp001 | 26.3
temp002 | 29.8
```

## Advanced: Cogroup Operation

Cogroup allows combining multiple grouped streams into a single aggregation. This is useful when you need to aggregate data from different sources into one unified result.

??? info "Orders, Refunds, and Bonuses Producer (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-cogroup.yaml"
    %}
    ```

??? info "Cogroup Processor (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml"
    %}
    ```

### How Cogroup Works

The cogroup operation:

1. Groups each stream independently by key
2. Combines the grouped streams using cogroup operations
3. Each stream contributes to the aggregate with its own aggregator function
4. Final aggregate operation computes the combined result

> **Note:** Cogroup is an advanced feature that requires careful coordination between multiple streams. Ensure all streams are properly grouped and that aggregator functions handle null values appropriately.

## Complex Example: Regional Sales Analytics

This example demonstrates rekeying (changing the grouping key) and windowed aggregation to calculate regional sales statistics:

**What it does:**

1. **Receives sales events** keyed by product ID with region, amount, and quantity data
2. **Rekeys by region** - changes the grouping from product to geographical region
3. **Windows by time** - creates 1-minute tumbling windows for aggregation
4. **Aggregates metrics** - tracks total sales, quantities, transaction counts, and per-product breakdowns
5. **Outputs regional statistics** - writes windowed regional summaries to output topic

**Key KSML concepts demonstrated:**

- `map` with `keyValueMapper` to change the message key (rekeying)
- `groupByKey` after rekeying to group by the new key
- `windowByTime` for time-based regional analytics
- Complex aggregation state with nested data structures
- Tracking multiple metrics in a single aggregation

??? info "Sales events producer (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/producer-sales.yaml"
    %}
    ```

??? info "Regional sales analytics processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/aggregations/processor-sales-analytics.yaml"
    %}
    ```

**Pipeline flow:**

1. **Input**: Sales events with product_id as key
2. **Rekey**: Map operation changes key to region
3. **Group**: Group by the new region key
4. **Window**: Apply 1-minute tumbling windows
5. **Aggregate**: Calculate regional statistics per window
6. **Output**: Windowed regional sales summaries

## Performance Considerations

### State Store Types

**RocksDB (default)**

- Persistent, can handle large state
- Slower than in-memory
- Survives restarts

**In-Memory**

- Fast but limited by heap size
- Lost on restart (rebuilt from changelog)
- Good for small, temporary state

### Optimization Strategies

1. **Enable Caching**
   ```yaml
   store:
     caching: true  # Reduces downstream updates
   ```

2. **Tune Commit Intervals**
       - Longer intervals = better throughput, higher latency
       - Shorter intervals = lower latency, more overhead
   
       To configure commit intervals, change your Kafka broker settings:
       ```yaml
       # In your ksml-runner.yaml or application config
       kafka:
         commit.interval.ms: 30000  # 30 seconds for better throughput
         # or
         commit.interval.ms: 100    # 100ms for lower latency
       ```

3. **Pre-filter Data**
       - Filter before grouping to reduce state size
       - Remove unnecessary fields early

4. **Choose Appropriate Window Sizes**
       - Smaller windows = less memory
       - Consider business requirements vs resources

### Memory Management

Monitor state store sizes:

- Each unique key requires memory
- Windowed aggregations multiply by number of windows
- Use retention policies to limit window history

## Common Pitfalls and Solutions

### Forgetting to Group

**Problem:** Aggregation operations require grouped streams

**Solution:** Always use `groupByKey` or `groupBy` before aggregating

### Null Value Handling
**Problem:** Null values can cause aggregation failures

**Solution:** Check for nulls in aggregator functions:
```yaml
code: |
  if value is None:
    return aggregatedValue
  # ... rest of logic
```

### Type Mismatches
**Problem:** Result type doesn't match expression output

**Solution:** Ensure `resultType` matches what your expression returns

### Window Size vs Retention

**Problem:** Confusion between window size and retention

**Solution:** 

- Window size = duration of each window
- Retention = how long to keep old windows
- Retention should be > window size

### Late Arriving Data
**Problem:** Data arrives after window closes

**Solution:** Configure appropriate grace periods:
```yaml
grace: 5m  # Allow 5 minutes for late data
```

## Conclusion

KSML aggregations enable powerful real-time analytics:

- **Count** for frequency analysis
- **Reduce** for simple combinations
- **Aggregate** for complex statistics
- **Windowed operations** for time-based analytics
- **Cogroup** for multi-stream aggregations

Choose the appropriate aggregation type based on your use case, and always consider state management and performance implications.

## Further Reading

- [Reference: Stateful Operations](../../reference/operation-reference.md/#stateful-aggregation-operations)