# Implementing Joins in KSML

This tutorial explores how to implement join operations in KSML, allowing you to combine data from multiple streams or tables to create enriched datasets.

## Introduction

Joins are fundamental operations in stream processing that combine data from multiple sources based on common keys. KSML provides three main types of joins, each serving different use cases in real-time data processing.

KSML joins are built on top of Kafka Streams join operations, providing a YAML-based interface to powerful stream processing capabilities without requiring Java code.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_clicks && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_purchases && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic correlated_user_actions && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_conversions && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_clicks_by_product && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_purchases_by_product && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic new_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic enriched_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic orders_with_customer_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_catalog && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic orders_with_product_details && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_activity_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_location_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic activity_with_location && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_login_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_logout_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_session_analysis && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_profiles && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_preferences && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic enriched_customer_data && \
    ```

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

**What it does:**

- **Produces two streams**: Creates product_clicks (user clicks on products) and product_purchases (user buys products) with user_id keys and timestamps
- **Joins with time window**: Uses join operation with 30-minute timeDifference to correlate clicks with purchases that happen within 30 minutes
- **Stores in window stores**: Both streams use 60-minute window stores (2×30min) with retainDuplicates=true to buffer events for correlation
- **Combines with valueJoiner**: When matching user_id found in both streams within time window, combines click and purchase data into single result
- **Outputs correlations**: Returns JSON showing both events with conversion analysis (did click lead to purchase, what products involved)

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

**What it does:**

- **Produces orders and customers**: Creates order stream (keyed by order_id) and customer table (keyed by customer_id) with matching customer references
- **Rekeys for join**: Transforms order key from order_id to customer_id using extract_customer_id function to enable join with customers table
- **Joins stream with table**: Uses join operation to combine each order with latest customer data from customers table based on customer_id
- **Combines data**: valueJoiner function merges order details with customer information into enriched result containing both datasets
- **Restores original key**: Transforms key back from customer_id to order_id using restore_order_key for downstream processing consistency

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

**What it does:**

- **Produces orders and products**: Creates order stream with product_id references and product_catalog globalTable with product details  
- **Uses foreign key mapper**: Extracts product_id from order value using keyValueMapper to look up in product_catalog globalTable
- **Joins with globalTable**: No co-partitioning needed - globalTable replicated to all instances, joins orders with product details by product_id
- **Enriches with product data**: Combines order information with product details (name, price, category) from catalog
- **Outputs enriched orders**: Returns orders enhanced with full product information for downstream processing

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

## Stream-Table Left Join

Left joins preserve all records from the stream (left side) while adding data from the table (right side) when available. This is useful for enriching events with optional reference data.

### Use Case: Activity Enrichment with Location

Enrich user activity events with location data, preserving all activities even when location information is missing.

**What it does:**

- **Produces activities and locations**: Creates user_activity_events stream and user_location_data table, with some users having location data, others not
- **Uses leftJoin**: Joins activity stream with location table - preserves all activities even when no location data exists for user
- **Handles missing data**: valueJoiner receives null for location when user not in location table, includes null-check logic
- **Enriches optionally**: Adds location data when available, leaves location fields empty/null when not available  
- **Preserves all activities**: All activity events flow through to output regardless of whether location data exists, maintaining complete activity stream

??? info "Producer: User Activity and Locations (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/joins/producer-user-activity-locations.yaml"
    %}
    ```

??? info "Processor: Stream-Table Left Join (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/joins/processor-stream-table-left-join.yaml"
    %}
    ```

This example demonstrates:

- **Left join semantics**: All activity events are preserved, even when location data is missing
- **Graceful null handling**: Unknown locations get default values instead of causing errors
- **Enrichment metadata**: Tracks whether enrichment data was available
- **Real-world pattern**: Common scenario where reference data may be incomplete

**Expected Behavior:**

- Activities for users with location data are enriched with country/city information
- Activities for users without location data get "UNKNOWN" placeholders
- All activities are preserved regardless of location availability

## Stream-Stream Outer Join

Outer joins capture events from either stream, making them ideal for tracking incomplete or partial interactions between two event types.

### Use Case: Login/Logout Session Analysis

Track user sessions by correlating login and logout events, capturing incomplete sessions where only one event type occurs.

**What it does:**

- **Produces login and logout events**: Creates separate streams for user_login_events and user_logout_events with overlapping but not always matching users
- **Uses outerJoin**: Correlates events within 10-minute window, captures login-only, logout-only, and complete login+logout sessions
- **Handles three scenarios**: Complete sessions (both events), LOGIN_ONLY (user logged in, no logout captured), LOGOUT_ONLY (logout without login)
- **Calculates session data**: For complete sessions computes session duration, for incomplete sessions identifies the pattern and timing
- **Outputs all patterns**: Returns session analysis showing complete sessions with duration, or incomplete sessions with pattern classification

??? info "Producer: Login and Logout Events (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/joins/producer-login-logout-events.yaml"
    %}
    ```

??? info "Processor: Stream-Stream Outer Join (click to expand)"

    ```yaml
    {%
      include "../../definitions/intermediate-tutorial/joins/processor-stream-stream-outer-join.yaml"
    %}
    ```

This example demonstrates:

- **Outer join semantics**: Captures login-only, logout-only, and complete sessions
- **Session analysis**: Categorizes different session patterns for analytics
- **Time window correlation**: Uses 10-minute window to correlate related events
- **Business insights**: Identifies users who login but don't logout (active sessions) or logout without captured login

**Expected Behavior:**

- **COMPLETE sessions**: When both login and logout events occur within the time window
- **LOGIN_ONLY sessions**: Users who logged in but no logout was captured (active sessions)
- **LOGOUT_ONLY sessions**: Logout events without corresponding login (users already logged in)

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