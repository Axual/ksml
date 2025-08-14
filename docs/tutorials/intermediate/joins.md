# Implementing Joins in KSML

This tutorial explores how to implement join operations in KSML, allowing you to combine data from multiple streams or tables to create enriched datasets.

## Introduction

Joins are fundamental operations in stream processing that combine data from multiple sources based on common keys. KSML provides three main types of joins, each serving different use cases in real-time data processing.

KSML joins are built on top of Kafka Streams join operations, providing a YAML-based interface to powerful stream processing capabilities without requiring Java code.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)

## Core Join Concepts

### ValueJoiner Functions

All join operations require a valueJoiner function to combine data from both sides:

```yaml
valueJoiner:
  type: valueJoiner
  code: |
    # value1: left stream/table value
    # value2: right stream/table value
    result = {"combined": value1, "enriched": value2}
  expression: result
  resultType: json
```

**Requirements:**

- Must include `expression` and `resultType` fields
- Handle null values gracefully (especially for left/outer joins)
- Return appropriate data structure for downstream processing

### Co-partitioning Requirements

Stream-stream and stream-table joins require co-partitioning:

- **Same number of partitions** in both topics
- **Same partitioning strategy** (how keys map to partitions)
- **Same key type** and serialization format

GlobalTable joins don't require co-partitioning since data is replicated to all instances.

## Types of Joins in KSML

KSML supports three main categories of joins, each with specific characteristics and use cases:

### 1. Stream-Stream Joins
Join two event streams within a time window to correlate related events.

**Kafka Streams equivalent:** `KStream.join()`, `KStream.leftJoin()`, `KStream.outerJoin()`

**When to use:**

- Correlating events from different systems
- Tracking user behavior across multiple actions
- Detecting patterns that span multiple event types

**Key characteristics:**

- Requires time windows for correlation
- Both streams must be co-partitioned
- Results are emitted when matching events occur within the window
- Supports inner, left, and outer join semantics

### 2. Stream-Table Joins
Enrich a stream of events with the latest state from a changelog table.

**Kafka Streams equivalent:** `KStream.join()`, `KStream.leftJoin()` with KTable

**When to use:**

- Enriching events with reference data
- Adding current state information to events
- Looking up the latest value for a key

**Key characteristics:**

- Stream events are enriched with the latest table value
- Table provides point-in-time lookups
- Requires co-partitioning between stream and table
- Supports inner and left join semantics

### 3. Stream-GlobalTable Joins
Enrich events using replicated reference data available on all instances.

**Kafka Streams equivalent:** `KStream.join()`, `KStream.leftJoin()` with GlobalKTable

**When to use:**

- Joining with reference data (product catalogs, configuration)
- Foreign key joins where keys don't match directly
- Avoiding co-partitioning requirements

**Key characteristics:**

- GlobalTable is replicated to all application instances
- Supports foreign key extraction via mapper functions
- No co-partitioning required
- Supports inner and left join semantics

## Stream-Stream Join

Stream-stream joins correlate events from two streams within a specified time window. This is essential for detecting patterns and relationships between different event types.

### Use Case: User Behavior Analysis

Track user shopping behavior by correlating clicks and purchases within a 30-minute window to understand the customer journey.

??? info "Producer: Clicks and Purchases (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/producer-clicks-purchases.yaml" %}
    ```

??? info "Processor: Stream-Stream Join (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/processor-stream-stream-join-working.yaml" %}
    ```

### Key Configuration Points

The aim here is to show how time windows must be used to correlate events from different streams. The configuration demonstrates:

- **timeDifference**: 30m - Maximum time gap between correlated events
- **Window Stores**: Both streams need stores with `retainDuplicates: true`
- **Window Size**: Must be `2 × timeDifference` (60m) to buffer events from both streams
- **Retention**: Must be `2 × timeDifference + grace` (65m) for proper state cleanup
- **Grace Period**: 5m allowance for late-arriving events

This configuration ensures events are only correlated within a reasonable time frame while managing memory efficiently.

## Stream-Table Join

Stream-table joins enrich streaming events with the latest state from a changelog table. This pattern is common for adding reference data to events.

### Use Case: Order Enrichment

Enrich order events with customer information by joining the orders stream with a customers table.

**Implementation Challenge:** Orders naturally use order_id as the key, but joining requires customer_id. The solution uses a three-step pattern:

1. **Rekey** orders from order_id to customer_id
2. **Join** with the customers table
3. **Rekey** back to order_id for downstream processing

??? info "Producer: Orders (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/producer-orders.yaml" %}
    ```

??? info "Producer: Customers (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/producer-customers.yaml" %}
    ```

??? info "Processor: Stream-Table Join (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/processor-stream-table-join.yaml" %}
    ```

### Rekeying Pattern

The rekeying pattern is essential when join keys don't match naturally:

- Use `transformKey` to extract the join key from the stream
- Perform the join operation
- Optionally restore the original key for downstream consistency

## Stream-GlobalTable Join

GlobalTable joins enable enrichment with reference data that's replicated across all instances, supporting foreign key relationships.

### Use Case: Product Catalog Enrichment

Enrich orders with product details using a foreign key join with a global product catalog.

??? info "Producer: Orders and Products (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/producer-orders-products.yaml" %}
    ```

??? info "Processor: Foreign Key Join (click to expand)"

    ```yaml
    {% include "../../definitions/intermediate-tutorial/joins/processor-foreign-key-join.yaml" %}
    ```

### Foreign Key Extraction

The `mapper` function extracts the foreign key from stream records:

- **Function Type**: `keyValueMapper` (not `foreignKeyExtractor`)
- **Input**: Stream's key and value
- **Output**: Key to lookup in the GlobalTable
- **Example**: Extract product_id from order to join with product catalog

## Join Type Variants

Each join type supports different semantics for handling missing matches:

### Inner Joins
```yaml
type: join  # Default inner join
```
Produces output **only** when both sides have matching keys.

### Left Joins
```yaml
type: leftJoin
```
Always produces output for the left side (stream), with null for missing right side values.

### Outer Joins (Stream-Stream only)
```yaml
type: outerJoin
```
Produces output whenever **either** side has data, with null for missing values.

**Note:** Table and GlobalTable joins don't support outer joins since tables represent current state, not events.

## Performance Considerations

### State Management
- **Window sizes**: Larger windows consume more memory but capture more correlations
- **Retention periods**: Balance between late data handling and resource usage
- **Grace periods**: Allow late arrivals while managing state cleanup

### Topology Optimization
- **Join order**: Join with smaller datasets first when chaining multiple joins
- **GlobalTable usage**: Use for frequently accessed reference data to avoid repartitioning
- **Rekeying overhead**: Minimize unnecessary rekeying operations

## Conclusion

KSML's join operations enable powerful data enrichment patterns:

- **Stream-stream joins** correlate events within time windows
- **Stream-table joins** enrich events with current state
- **Stream-GlobalTable joins** provide foreign key lookups without co-partitioning

Choose the appropriate join type based on your data characteristics and business requirements.

## Further Reading

- [Reference: Join Operations](../../reference/operation-reference.md/#join-operations)