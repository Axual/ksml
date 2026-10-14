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

Every aggregation in KSML follows the same flow: group the messages by key, optionally split them into time windows, and then aggregate them. The aggregation keeps its running result in a state store and emits an updated result for each change. If the store caches updates, the topic only gets the latest result at each commit (by default every 30 seconds). You turn the results back into a stream with `toStream` to write them to a topic:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    InputTopic[(Input topic)]

    %% Pipeline Operations
    Group[<code>groupByKey</code><br/>or <code>groupBy</code>]
    Window[<code>windowByTime</code><br/>or <code>windowBySession</code>]
    Aggregate[<code>count</code>, <code>reduce</code><br/>or <code>aggregate</code>]
    ToStream[<code>toStream</code>]

    %% State
    Store[(State store)]
    Changelog[(Changelog topic)]

    %% Output
    OutputTopic[(Output topic)]

    %% Data Flow
    InputTopic --> Group
    Group -->|"regular"| Aggregate
    Group -.->|"windowed"| Window
    Window -.-> Aggregate
    Aggregate -->|"updated result"| ToStream
    ToStream --> OutputTopic
    Aggregate <-->|"read & update"| Store
    Store -.->|"every change"| Changelog

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class InputTopic,OutputTopic,Changelog topic
    class Group,Window,Aggregate,ToStream operation
    class Store store
```

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
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-user-actions.yaml" %}
    ```

??? info "Count user actions processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-count.yaml" %}
    ```

### How Count Works

The count operation:

1. Groups messages by key using `groupByKey`
2. Maintains a counter per unique key
3. Increments the counter for each message
4. Outputs the current count as a KTable

The diagram below follows five example actions through the pipeline. Unlike a windowed count, the count for a user never resets: every new action updates the stored count. This store caches updates, so the topic gets the latest count about every 30 seconds:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with five actions
    Topic[(user_actions)]
    Actions@{ shape: docs, label: "<b>alice</b>: login<br/><b>bob</b>: view<br/><b>alice</b>: click<br/><b>alice</b>: purchase<br/><b>bob</b>: logout" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Alice@{ shape: docs, label: "<b>key: alice</b><br/>login, click, purchase" }
        Bob@{ shape: docs, label: "<b>key: bob</b><br/>view, logout" }
    end

    %% Step 2: running count per key
    Count[<code>count</code>]
    subgraph Counts["Running count per key, updated with every action"]
        AliceCount["<b>alice</b><br/>count 1, then 2, then 3"]
        BobCount["<b>bob</b><br/>count 1, then 2"]
    end

    %% Step 3: write to the output topic
    Convert["<code>toStream</code> + <code>convertValue</code><br/>into string"]
    Output[(user_action_counts)]

    %% Data Flow
    Topic --> Actions
    Actions --> Group
    Group --> Alice & Bob
    Alice & Bob --> Count
    Count --> AliceCount & BobCount
    AliceCount & BobCount --> Convert
    Convert -->|"latest message for alice<br/>key: alice, value: 3"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Count,Convert operation
    class Actions,Alice,Bob message
    class AliceCount,BobCount result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style Counts fill:none,stroke:#616161,stroke-dasharray:5 5
```

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

The diagram below follows four example transactions (amounts in cents) through the pipeline. The reducer has no initial value: the first amount for an account is stored as is, and every next amount is added to it:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with four transactions
    Topic[(financial_transactions)]
    Transactions@{ shape: docs, label: "<b>ACC001</b>: 1500<br/><b>ACC002</b>: 2000<br/><b>ACC001</b>: 500<br/><b>ACC001</b>: 3000" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Acc1@{ shape: docs, label: "<b>key: ACC001</b><br/>1500, 500, 3000" }
        Acc2@{ shape: doc, label: "<b>key: ACC002</b><br/>2000" }
    end

    %% Step 2: combine values pairwise
    Reduce["<code>reduce</code><br/>sum_amounts: value1 + value2"]
    subgraph Totals["Running total per key"]
        Acc1Total["<b>ACC001</b><br/>1500 (first value, stored as is)<br/>1500 + 500 = 2000<br/>2000 + 3000 = 5000"]
        Acc2Total["<b>ACC002</b><br/>2000 (first value, stored as is)"]
    end

    %% Step 3: write to the output topic
    ToStream[<code>toStream</code>]
    Output[(transaction_sums)]

    %% Data Flow
    Topic --> Transactions
    Transactions --> Group
    Group --> Acc1 & Acc2
    Acc1 & Acc2 --> Reduce
    Reduce --> Acc1Total & Acc2Total
    Acc1Total & Acc2Total --> ToStream
    ToStream -->|"latest message for ACC001<br/>key: ACC001, value: 5000"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Reduce,ToStream operation
    class Transactions,Acc1,Acc2 message
    class Acc1Total,Acc2Total result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style Totals fill:none,stroke:#616161,stroke-dasharray:5 5
```

The JSON version below works the same way, with one `transformValue` before grouping to extract `amount_cents` from each message, and one after `toStream` to format the total as JSON.

??? info "Simple producer (binary long values) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-simple.yaml" %}
    ```

??? info "Simple processor (reduce only) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-simple.yaml" %}
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
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-transactions.yaml" %}
    ```

??? info "JSON processor (with transformations) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml" %}
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
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-simple-aggregate.yaml" %}
    ```

??? info "Simple processor (aggregate only) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-simple-aggregate.yaml" %}
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

The diagram below follows four example payments through the pipeline. The initializer creates an empty statistics object for each new account, and the aggregator updates all fields with every payment:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with four payments
    Topic[(payment_stream)]
    Payments@{ shape: docs, label: "<b>ACC001</b>: amount 25.00<br/><b>ACC002</b>: amount 40.00<br/><b>ACC001</b>: amount 75.00<br/><b>ACC001</b>: amount 50.00" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Acc1@{ shape: docs, label: "<b>key: ACC001</b><br/>25.00, 75.00, 50.00" }
        Acc2@{ shape: doc, label: "<b>key: ACC002</b><br/>40.00" }
    end

    %% Step 2: update statistics per key
    Aggregate["<code>aggregate</code><br/>init_stats + update_stats"]
    subgraph Stats["Statistics per key, updated with every payment"]
        Acc1Stats["<b>ACC001</b><br/>start: count 0, total 0.0<br/><br/><b>+ 25.00</b>: count 1, total 25.00<br/>min 25.00, max 25.00, avg 25.00<br/><br/><b>+ 75.00</b>: count 2, total 100.00<br/>min 25.00, max 75.00, avg 50.00<br/><br/><b>+ 50.00</b>: count 3, total 150.00<br/>min 25.00, max 75.00, avg 50.00"]
        Acc2Stats["<b>ACC002</b><br/>start: count 0, total 0.0<br/><br/><b>+ 40.00</b>: count 1, total 40.00<br/>min 40.00, max 40.00, avg 40.00"]
    end

    %% Step 3: write to the output topic
    ToStream[<code>toStream</code>]
    Output[(payment_statistics)]

    %% Data Flow
    Topic --> Payments
    Payments --> Group
    Group --> Acc1 & Acc2
    Acc1 & Acc2 --> Aggregate
    Aggregate --> Acc1Stats & Acc2Stats
    Acc1Stats & Acc2Stats --> ToStream
    ToStream -->|"latest message for ACC001<br/>key: ACC001<br/>value: {count: 3, total_amount: 150.0, min_amount: 25.0,<br/>max_amount: 75.0, average_amount: 50.0}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Aggregate,ToStream operation
    class Payments,Acc1,Acc2 message
    class Acc1Stats,Acc2Stats result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style Stats fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "JSON payment events producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-payments.yaml" %}
    ```

??? info "JSON statistics processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-aggregate-stats.yaml" %}
    ```

Both approaches demonstrate the flexibility of the `aggregate` operation. The simple version focuses on the core concept, while the complex version shows real-world statistical aggregation with human-readable JSON output.

### Aggregate Components

1. **Initializer**: Creates empty/initial state
2. **Aggregator**: Updates state with each new value
3. **Result**: Continuously updated aggregate in state store

The diagram below shows how this works for the simple sum example, with three payments arriving for the same customer. The initializer runs once for the first payment, and every payment after that updates the stored total and emits it:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Incoming payments for one key
    Payment1[/"CUST001: 5000"/]
    Payment2[/"CUST001: 2500"/]
    Payment3[/"CUST001: 1000"/]

    %% Aggregation steps
    Init["<code>initializer</code><br/>total = 0"]
    Step1["<code>aggregator</code><br/>0 + 5000 = 5000"]
    Step2["<code>aggregator</code><br/>5000 + 2500 = 7500"]
    Step3["<code>aggregator</code><br/>7500 + 1000 = 8500"]

    %% Output
    OutputTopic[(payment_totals)]

    %% Data Flow
    Init --> Step1
    Payment1 --> Step1
    Step1 -->|"state: 5000"| Step2
    Payment2 --> Step2
    Step2 -->|"state: 7500"| Step3
    Payment3 --> Step3
    Step1 -.->|"emit 5000"| OutputTopic
    Step2 -.->|"emit 7500"| OutputTopic
    Step3 -.->|"emit 8500"| OutputTopic

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class OutputTopic topic
    class Init,Step1,Step2,Step3 operation
    class Payment1,Payment2,Payment3 external
```

With caching enabled on the store, Kafka Streams may combine several updates and emit only the latest total.

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

The diagram below follows four example readings through the pipeline. Each sensor's readings are split into 30-second windows, and every window gets its own statistics. Every reading sends updated statistics for its window, and the diagram shows the last one:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with four readings
    Topic[(sensor_temperatures)]
    Readings@{ shape: docs, label: "<b>temp001</b>: 24.5 at 18:58:05<br/><b>temp002</b>: 31.2 at 18:58:10<br/><b>temp001</b>: 26.3 at 18:58:20<br/><b>temp001</b>: 22.7 at 18:58:40" }

    %% Step 1: group by key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by key"]
        Temp1@{ shape: docs, label: "<b>key: temp001</b><br/>24.5, 26.3, 22.7" }
        Temp2@{ shape: doc, label: "<b>key: temp002</b><br/>31.2" }
    end

    %% Step 2: split each group into 30-second windows
    Window["<code>windowByTime</code><br/>tumbling 30s"]
    subgraph ByWindow["Grouped by key and window"]
        Temp1W1@{ shape: docs, label: "<b>temp001</b> 18:58:00 to 18:58:30<br/>24.5, 26.3" }
        Temp1W2@{ shape: doc, label: "<b>temp001</b> 18:58:30 to 18:59:00<br/>22.7" }
        Temp2W1@{ shape: doc, label: "<b>temp002</b> 18:58:00 to 18:58:30<br/>31.2" }
    end

    %% Step 3: statistics per key and window
    Aggregate["<code>aggregate</code><br/>init_stats + update_stats"]
    subgraph Stats["Latest result per key and window"]
        Temp1W1Stats["<b>temp001</b> 18:58:00 to 18:58:30<br/>count 2, avg 25.4, min 24.5, max 26.3"]
        Temp1W2Stats["<b>temp001</b> 18:58:30 to 18:59:00<br/>count 1, avg 22.7, min 22.7, max 22.7"]
        Temp2W1Stats["<b>temp002</b> 18:58:00 to 18:58:30<br/>count 1, avg 31.2, min 31.2, max 31.2"]
    end

    %% Step 4: replace the windowed key with a string key
    Map["<code>toStream</code> + <code>map</code><br/>transform_window_key"]
    Output[(sensor_window_stats)]

    %% Data Flow
    Topic --> Readings
    Readings --> Group
    Group --> Temp1 & Temp2
    Temp1 & Temp2 --> Window
    Window --> Temp1W1 & Temp1W2 & Temp2W1
    Temp1W1 & Temp1W2 & Temp2W1 --> Aggregate
    Aggregate --> Temp1W1Stats & Temp1W2Stats & Temp2W1Stats
    Temp1W1Stats & Temp1W2Stats & Temp2W1Stats --> Map
    Map -->|"key: temp001_...T18:58:00Z_...T18:58:30Z<br/>value: {sensor_id: temp001, window_start, window_end,<br/>stats: {count: 2, avg: 25.4, min: 24.5, max: 26.3}}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Group,Window,Aggregate,Map operation
    class Readings,Temp1,Temp2,Temp1W1,Temp1W2,Temp2W1 message
    class Temp1W1Stats,Temp1W2Stats,Temp2W1Stats result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style ByWindow fill:none,stroke:#616161,stroke-dasharray:5 5
    style Stats fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "Temperature sensor producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-windowed.yaml" %}
    ```

??? info "Windowed temperature statistics processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml" %}
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

Cogroup is meant to combine several grouped streams into one aggregation, with a separate aggregator for each stream. KSML supports only one stream in a `cogroup` today (see [#728](https://github.com/Axual/ksml/issues/728)), so the example below uses one stream.

??? info "Customer orders producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-cogroup.yaml" %}
    ```

??? info "Cogroup Processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml" %}
    ```

### How Cogroup Works

The cogroup pipeline:

1. Groups the stream by key with `groupByKey`
2. Adds the grouped stream to the cogroup with its own aggregator function
3. Runs the final `aggregate` operation, which uses the initializer to compute the result

The example above uses only one stream, `customer_orders`. KSML does not support more than one stream in a `cogroup` yet (see [#728](https://github.com/Axual/ksml/issues/728)). The diagram below follows two example orders through the pipeline:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input topic with example orders
    Topic[(customer_orders)]
    Orders@{ shape: docs, label: "<b>key: alice</b> amount 50.0<br/><b>key: alice</b> amount 30.0" }

    %% Grouping and cogrouping
    Group[<code>groupByKey</code>]
    Cogroup["<code>cogroup</code><br/>aggregator: aggregate_orders"]
    Aggregate["<code>aggregate</code><br/>initializer: init_order_stats"]

    %% State and output
    Store[(customer_totals_store)]
    Result["<b>alice</b><br/>total_amount: 80.0<br/>order_count: 2"]
    OutputTopic[(customer_totals)]

    %% Data Flow
    Topic --> Orders --> Group --> Cogroup --> Aggregate
    Aggregate <-->|"one result per key"| Store
    Aggregate --> Result
    Result -->|"<code>toStream</code>"| OutputTopic

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,OutputTopic topic
    class Group,Cogroup,Aggregate operation
    class Store store
    class Orders message
    class Result result
```

> **Note:** A second `cogroup` step in the same pipeline fails at startup with a `TopologyException`. Make sure the aggregator function handles null values.

## Complex Example: Regional Sales Analytics

This example demonstrates rekeying (changing the grouping key) and windowed aggregation to calculate regional sales statistics:

**What it does:**

1. **Receives sales events** keyed by product ID with region, amount, and quantity data
2. **Rekeys by region** - changes the grouping from product to geographical region
3. **Windows by time** - creates 1-minute tumbling windows for aggregation
4. **Aggregates metrics** - tracks total sales, quantities, transaction counts, and per-product breakdowns
5. **Outputs regional statistics** - writes windowed regional summaries to output topic

**Key KSML concepts demonstrated:**

- `selectKey` to change the message key (rekeying)
- `groupByKey` after rekeying to group by the new key
- `windowByTime` for time-based regional analytics
- Complex aggregation state with nested data structures
- Tracking multiple metrics in a single aggregation

??? info "Sales events producer (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/producer-sales.yaml" %}
    ```

??? info "Regional sales analytics processor (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/aggregations/processor-sales-analytics.yaml" %}
    ```

**Pipeline flow:**

1. **Input**: Sales events with product_id as key
2. **Rekey**: `selectKey` changes the key to the region
3. **Group**: Group by the new region key
4. **Window**: Apply 1-minute tumbling windows
5. **Aggregate**: Calculate regional statistics per window
6. **Output**: Windowed regional sales summaries

The diagram below follows four example sales through the pipeline. The messages arrive keyed by product, so the pipeline first replaces the key with the region. Only then can `groupByKey` collect all sales of one region together. Every sale sends an updated result for its region and window, and the diagram shows the last one:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with four sales, keyed by product
    Topic[(retail_sales)]
    Sales@{ shape: docs, label: "<b>key: laptop</b> North, 1200.00 at 10:00:10<br/><b>key: phone</b> South, 800.00 at 10:00:20<br/><b>key: tablet</b> North, 500.00 at 10:00:40<br/><b>key: mouse</b> North, 25.00 at 10:01:15" }

    %% Step 1: rekey by region
    SelectKey["<code>selectKey</code><br/>extract_region"]
    Rekeyed@{ shape: docs, label: "<b>key: North</b> laptop, 1200.00<br/><b>key: South</b> phone, 800.00<br/><b>key: North</b> tablet, 500.00<br/><b>key: North</b> mouse, 25.00" }

    %% Step 2: group by the new key
    Group[<code>groupByKey</code>]
    subgraph ByKey["Grouped by region"]
        North@{ shape: docs, label: "<b>key: North</b><br/>laptop, tablet, mouse" }
        South@{ shape: doc, label: "<b>key: South</b><br/>phone" }
    end

    %% Step 3: split each region into 1-minute windows
    Window["<code>windowByTime</code><br/>tumbling 1m"]
    subgraph ByWindow["Grouped by region and window"]
        NorthW1@{ shape: docs, label: "<b>North</b> 10:00 to 10:01<br/>laptop, tablet" }
        NorthW2@{ shape: doc, label: "<b>North</b> 10:01 to 10:02<br/>mouse" }
        SouthW1@{ shape: doc, label: "<b>South</b> 10:00 to 10:01<br/>phone" }
    end

    %% Step 4: sales statistics per region and window
    Aggregate["<code>aggregate</code><br/>aggregate_sales"]
    subgraph Stats["Latest result per region and window"]
        NorthW1Stats["<b>North</b> 10:00 to 10:01<br/>total_sales 1700.00, transactions 2<br/>products: laptop, tablet"]
        NorthW2Stats["<b>North</b> 10:01 to 10:02<br/>total_sales 25.00, transactions 1<br/>products: mouse"]
        SouthW1Stats["<b>South</b> 10:00 to 10:01<br/>total_sales 800.00, transactions 1<br/>products: phone"]
    end

    %% Step 5: write to the output topic
    Convert["<code>toStream</code> + <code>convertKey</code><br/>+ <code>transformValue</code> transform_window_key"]
    Output[(sales_by_region)]

    %% Data Flow
    Topic --> Sales
    Sales --> SelectKey
    SelectKey --> Rekeyed
    Rekeyed --> Group
    Group --> North & South
    North & South --> Window
    Window --> NorthW1 & NorthW2 & SouthW1
    NorthW1 & NorthW2 & SouthW1 --> Aggregate
    Aggregate --> NorthW1Stats & NorthW2Stats & SouthW1Stats
    NorthW1Stats & NorthW2Stats & SouthW1Stats --> Convert
    Convert -->|"key: {key: North, startTime: 10:00, endTime: 10:01, ...}<br/>value: {region: North, window_start, window_end, stats: {...}}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class SelectKey,Group,Window,Aggregate,Convert operation
    class Sales,Rekeyed,North,South,NorthW1,NorthW2,SouthW1 message
    class NorthW1Stats,NorthW2Stats,SouthW1Stats result
    style ByKey fill:none,stroke:#616161,stroke-dasharray:5 5
    style ByWindow fill:none,stroke:#616161,stroke-dasharray:5 5
    style Stats fill:none,stroke:#616161,stroke-dasharray:5 5
```

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