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
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_loyalty && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic orders_with_customer_loyalty && \
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

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input streams
    Clicks[(product_clicks<br/>key: user_id)]
    Purchases[(product_purchases<br/>key: user_id)]

    %% Window stores buffering both sides
    ClickStore[(Window store<br/>clicks, 60m)]
    PurchaseStore[(Window store<br/>purchases, 60m)]

    %% Join
    Join["<code>join</code><br/>same user_id, within 30m<br/>correlate_click_and_purchase"]

    %% Output
    Output[(correlated_user_actions)]

    %% Data Flow
    Clicks --> Join
    Purchases --> Join
    Join -.->|"buffer & look up"| ClickStore
    Join -.->|"buffer & look up"| PurchaseStore
    Join -->|"click + purchase"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Clicks,Purchases,Output topic
    class Join operation
    class ClickStore,PurchaseStore store
```

The diagram below follows example clicks and purchases through the join. A click and a purchase are joined only when they have the same key (user) **and** happened at most 30 minutes apart. Everything else produces no output:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topics
    ClickTopic[(product_clicks)]
    PurchaseTopic[(product_purchases)]
    Clicks@{ shape: docs, label: "<b>user1</b>: click PROD001 at 10:00<br/><b>user2</b>: click PROD003 at 10:05<br/><b>user3</b>: click PROD002 at 10:10" }
    Purchases@{ shape: docs, label: "<b>user1</b>: buy PROD001 at 10:12<br/><b>user2</b>: buy PROD003 at 11:00<br/><b>user4</b>: buy PROD005 at 10:20" }

    %% Join
    Join["<code>join</code><br/>same user, at most 30m apart<br/>correlate_click_and_purchase"]

    %% Results
    subgraph Results["Join results"]
        User1["<b>user1</b><br/>click 10:00 + purchase 10:12<br/>converted, 12 minutes"]
        User2["<b>user2</b><br/>55 minutes apart:<br/>no output"]
        User3["<b>user3</b><br/>click without purchase:<br/>no output"]
        User4["<b>user4</b><br/>purchase without click:<br/>no output"]
    end

    %% Output
    Output[(correlated_user_actions)]

    %% Data Flow
    ClickTopic --> Clicks
    PurchaseTopic --> Purchases
    Clicks & Purchases --> Join
    Join --> User1 & User2 & User3 & User4
    User1 -->|"key: user1<br/>value: {click: {...}, purchase: {...},<br/>converted: true, conversion_time_ms: 720000}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class ClickTopic,PurchaseTopic,Output topic
    class Join operation
    class Clicks,Purchases message
    class User1 result
    class User2,User3,User4 dropped
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "Producer: Clicks and Purchases (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-clicks-purchases.yaml" %}
    ```

??? info "Processor: Stream-Stream Join (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-stream-stream-join-working.yaml" %}
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

The diagram shows how the message key changes along the pipeline, so that each order can be matched with the customer table:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Orders[(new_orders<br/>key: order_id)]
    Customers[(customer_data table<br/>key: customer_id)]

    %% Pipeline Operations
    Rekey["<code>transformKey</code><br/>extract_customer_id"]
    Join["<code>join</code> with table<br/>join_order_with_customer"]
    Restore["<code>transformKey</code><br/>restore_order_key"]

    %% Output
    Output[(orders_with_customer_data<br/>key: order_id)]

    %% Data Flow
    Orders --> Rekey
    Rekey -->|"key: customer_id"| Join
    Customers -.->|"latest customer<br/>for this key"| Join
    Join -->|"key: customer_id"| Restore
    Restore -->|"key: order_id"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Orders,Customers,Output topic
    class Rekey,Join,Restore operation
```

The diagram below follows four example orders through the pipeline. Each order is rekeyed by its customer, looked up in the customer table, and rekeyed back. Because this is an inner join, an order whose customer is not in the table yet produces no output:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input stream and table
    OrderTopic[(new_orders)]
    Orders@{ shape: docs, label: "<b>key: ORD0001</b> customer CUST001<br/><b>key: ORD0002</b> customer CUST003<br/><b>key: ORD0003</b> customer CUST001<br/><b>key: ORD0004</b> customer CUST005" }
    CustomerTopic[(customer_data)]
    Customers@{ shape: docs, label: "<b>Table contents</b><br/><b>CUST001</b>: Alice Johnson, US-WEST<br/><b>CUST003</b>: Carol Davis, EU-WEST" }

    %% Step 1: rekey by customer
    Rekey["<code>transformKey</code><br/>extract_customer_id"]
    Rekeyed@{ shape: docs, label: "<b>key: CUST001</b> ORD0001<br/><b>key: CUST003</b> ORD0002<br/><b>key: CUST001</b> ORD0003<br/><b>key: CUST005</b> ORD0004" }

    %% Step 2: look up the customer
    Join["<code>join</code> with table<br/>join_order_with_customer"]
    subgraph Results["Join results"]
        Order1["<b>key: CUST001</b><br/>ORD0001 + Alice Johnson"]
        Order2["<b>key: CUST003</b><br/>ORD0002 + Carol Davis"]
        Order3["<b>key: CUST001</b><br/>ORD0003 + Alice Johnson"]
        Order4["<b>key: CUST005</b><br/>not in table:<br/>no output"]
    end

    %% Step 3: restore the order key
    Restore["<code>transformKey</code><br/>restore_order_key"]
    Output[(orders_with_customer_data)]

    %% Data Flow
    OrderTopic --> Orders
    Orders --> Rekey
    Rekey --> Rekeyed
    CustomerTopic --> Customers
    Rekeyed --> Join
    Customers -.->|"lookup by key"| Join
    Join --> Order1 & Order2 & Order3 & Order4
    Order1 & Order2 & Order3 --> Restore
    Restore -->|"key: ORD0001<br/>value: {order_id: ORD0001, ...,<br/>customer: {name: Alice Johnson, ...}}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class OrderTopic,CustomerTopic,Output topic
    class Rekey,Join,Restore operation
    class Orders,Customers,Rekeyed message
    class Order1,Order2,Order3 result
    class Order4 dropped
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "Producer: Orders (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-orders.yaml" %}
    ```

??? info "Producer: Customers (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-customers.yaml" %}
    ```

??? info "Processor: Stream-Table Join (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-stream-table-join.yaml" %}
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

Unlike the stream-table join above, there is no rekeying: the order keeps its key, and the `mapper` only tells the join which product to look up:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Orders[(new_orders<br/>key: order_id)]
    Products[(product_catalog globalTable<br/>key: product_id<br/>copy on every instance)]

    %% Pipeline Operations
    Join["<code>join</code> with globalTable<br/>mapper: extract_product_id<br/>join_order_with_product"]

    %% Output
    Output[(orders_with_product_details<br/>key: order_id)]

    %% Data Flow
    Orders --> Join
    Join -.->|"look up product_id<br/>from order value"| Products
    Products -.->|"product details"| Join
    Join -->|"order + product"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Orders,Products,Output topic
    class Join operation
```

The diagram below follows two example orders through the join. The order keeps its key; the `mapper` only picks the `product_id` used for the lookup, and the `valueJoiner` adds the product details and computes the total price:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input stream and global table
    OrderTopic[(new_orders)]
    Orders@{ shape: docs, label: "<b>key: ORD0001</b> product PROD001, quantity 2<br/><b>key: ORD0002</b> product PROD003, quantity 1" }
    ProductTopic[(product_catalog)]
    Products@{ shape: docs, label: "<b>Global table contents</b><br/><b>PROD001</b>: Laptop, 999.99<br/><b>PROD003</b>: Coffee Maker, 79.99" }

    %% Join
    Join["<code>join</code> with globalTable<br/>mapper: extract_product_id<br/>join_order_with_product"]

    %% Results
    subgraph Results["Join results"]
        Order1["<b>key: ORD0001</b><br/>looked up PROD001: Laptop<br/>total_price: 2 x 999.99 = 1999.98"]
        Order2["<b>key: ORD0002</b><br/>looked up PROD003: Coffee Maker<br/>total_price: 1 x 79.99 = 79.99"]
    end

    %% Output
    Output[(orders_with_product_details)]

    %% Data Flow
    OrderTopic --> Orders
    ProductTopic --> Products
    Orders --> Join
    Products -.->|"lookup by product_id"| Join
    Join --> Order1 & Order2
    Order1 -->|"key: ORD0001<br/>value: {order_id: ORD0001, ...,<br/>product_details: {name: Laptop, ...}, total_price: 1999.98}"| Output
    Order2 --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class OrderTopic,ProductTopic,Output topic
    class Join operation
    class Orders,Products message
    class Order1,Order2 result
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

??? info "Producer: Orders and Products (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-orders-products.yaml" %}
    ```

??? info "Processor: Foreign Key Join (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-foreign-key-join.yaml" %}
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
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-user-activity-locations.yaml" %}
    ```

??? info "Processor: Stream-Table Left Join (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-stream-table-left-join.yaml" %}
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

The diagram below follows three example activities through the left join. `user004` has no location data, but unlike an inner join, its activity is still written to the output, with `UNKNOWN` location values:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input stream and table
    ActivityTopic[(user_activity_events)]
    Activities@{ shape: docs, label: "<b>key: user001</b> login<br/><b>key: user004</b> page_view<br/><b>key: user002</b> purchase" }
    LocationTopic[(user_location_data)]
    Locations@{ shape: docs, label: "<b>Table contents</b><br/><b>user001</b>: New York, USA<br/><b>user002</b>: London, UK<br/>(no entry for user004)" }

    %% Join
    Join["<code>leftJoin</code> with table<br/>join_activity_with_location"]

    %% Results
    subgraph Results["Join results: one per activity"]
        User1["<b>user001</b> login<br/>location: New York, USA<br/>enriched: true"]
        User4["<b>user004</b> page_view<br/>location: UNKNOWN<br/>enriched: false"]
        User2["<b>user002</b> purchase<br/>location: London, UK<br/>enriched: true"]
    end

    %% Output
    Output[(activity_with_location)]

    %% Data Flow
    ActivityTopic --> Activities
    LocationTopic --> Locations
    Activities --> Join
    Locations -.->|"lookup by key"| Join
    Join --> User1 & User4 & User2
    User4 -->|"key: user004<br/>value: {activity_type: page_view, ...,<br/>location: {country: UNKNOWN, city: UNKNOWN, ...}, enriched: false}"| Output
    User1 & User2 --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"


    class ActivityTopic,LocationTopic,Output topic
    class Join operation
    class Activities,Locations message
    class User1,User2 result
    class User4 partial
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

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
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-login-logout-events.yaml" %}
    ```

??? info "Processor: Stream-Stream Outer Join (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-stream-stream-outer-join.yaml" %}
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

The diagram below follows example logins and logouts through the outer join. A login and a logout for the same user at most 10 minutes apart become one `COMPLETE` session. Unlike an inner join, events without a match are not dropped: they produce a `LOGIN_ONLY` or `LOGOUT_ONLY` result once the time window (plus grace period) has passed:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topics
    LoginTopic[(user_login_events)]
    LogoutTopic[(user_logout_events)]
    Logins@{ shape: docs, label: "<b>alice</b>: login at 10:00<br/><b>bob</b>: login at 10:02" }
    Logouts@{ shape: docs, label: "<b>frank</b>: logout at 10:04<br/><b>alice</b>: logout at 10:08" }

    %% Join
    Join["<code>outerJoin</code><br/>same user, at most 10m apart<br/>analyze_user_session"]

    %% Results
    subgraph Results["Join results"]
        Alice["<b>alice</b><br/>login 10:00 + logout 10:08<br/>session_type: COMPLETE"]
        Bob["<b>bob</b><br/>login, no logout<br/>session_type: LOGIN_ONLY"]
        Frank["<b>frank</b><br/>logout, no login<br/>session_type: LOGOUT_ONLY"]
    end

    %% Output
    Output[(user_session_analysis)]

    %% Data Flow
    LoginTopic --> Logins
    LogoutTopic --> Logouts
    Logins & Logouts --> Join
    Join -->|"match"| Alice
    Join -->|"after window + grace"| Bob
    Join -->|"after window + grace"| Frank
    Alice -->|"key: alice<br/>value: {session_type: COMPLETE,<br/>login_event: {...}, logout_event: {...}, ...}"| Output
    Bob & Frank --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"


    class LoginTopic,LogoutTopic,Output topic
    class Join operation
    class Logins,Logouts message
    class Alice result
    class Bob,Frank partial
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

## Joining with the Result of Another Pipeline

The `stream` and `table` of `join`, `leftJoin` and `outerJoin` can also be the result of an earlier pipeline, saved with `as`. This lets you build a join in stages: compute an intermediate result once, then join other data with it. The same works for the `stream` of `merge`.

### Use Case: Orders with Customer Loyalty

Enrich orders with customer details and the customer's loyalty tier. The customer details and the loyalty records are two tables, so the first pipeline joins them and saves the result. The second pipeline joins each order with that saved table.

**What it does:**

- **Joins two tables**: The `enrich_customers` pipeline joins the `customers` table with the `loyalty` table on the customer ID, and saves the result with `as: customers_with_loyalty`
- **Joins with the saved table**: The `enrich_orders` pipeline rekeys each order by customer ID and joins it with `table: customers_with_loyalty`, the name saved by the first pipeline
- **Drops orders without a match**: Both joins are inner joins, so an order is only enriched when its customer has both customer details and a loyalty record
- **Restores the order key**: The order key is restored after the join, as in the [stream-table join](#use-case-order-enrichment)

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Customers[(customer_data table<br/>key: customer_id)]
    Loyalty[(customer_loyalty table<br/>key: customer_id)]
    Orders[(new_orders<br/>key: order_id)]

    %% Pipeline 1: enrich_customers
    CustomerJoin["<code>join</code> with table<br/>join_customer_with_loyalty"]
    Saved["as: customers_with_loyalty"]

    %% Pipeline 2: enrich_orders
    Rekey["<code>transformKey</code><br/>extract_customer_id"]
    OrderJoin["<code>join</code> with table<br/>join_order_with_customer"]
    Restore["<code>transformKey</code><br/>restore_order_key"]

    %% Output
    Output[(orders_with_customer_loyalty<br/>key: order_id)]

    %% Data Flow
    Customers --> CustomerJoin
    Loyalty -.->|"loyalty for<br/>this customer"| CustomerJoin
    CustomerJoin --> Saved
    Orders --> Rekey
    Rekey -->|"key: customer_id"| OrderJoin
    Saved -.->|"customer with loyalty<br/>for this key"| OrderJoin
    OrderJoin -->|"key: customer_id"| Restore
    Restore -->|"key: order_id"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Customers,Loyalty,Orders,Output topic
    class CustomerJoin,Rekey,OrderJoin,Restore operation
    class Saved store
```

??? info "Producer: Orders (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-orders.yaml" %}
    ```

??? info "Producer: Customers (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-customers.yaml" %}
    ```

??? info "Producer: Customer Loyalty (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/producer-customer-loyalty.yaml" %}
    ```

??? info "Processor: Join with a Saved Table (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/joins/processor-join-saved-table.yaml" %}
    ```

### Rules for Joining with a Saved Result

- **Define the saving pipeline first**: The pipeline with `as: <name>` must come before the pipeline that joins with `<name>`, in the same definition file. Otherwise KSML fails with the parse error `Unknown table "<name>"`.
- **Use the right key**: Use `stream:` for a saved stream and `table:` for a saved table. Joining with a saved stream as `table:`, or the other way round, fails with `must be a table` or `must be a stream`. To join a stream as a table, add `toTable` with a `store` before `as`. Without a `store`, the table can miss the serdes it needs, and the join fails at runtime:

    ```yaml
    via:
      - type: filter
        if:
          expression: value is not None
      - type: toTable
        store:
          type: keyValue
          name: valid_orders_store
    as: valid_orders_table
    ```

- **No saved globalTables**: Kafka Streams can not make a GlobalKTable from a pipeline result, so `globalTable:` only accepts names defined under `globalTables`. Use `table:` to join with a saved table.
- **Unique names**: An `as` name can not be the same as a name under `streams`, `tables` or `globalTables`. For a pipeline without a sink, which saves its result under the pipeline name, KSML logs a warning.
- **No store needed**: A table saved after a table-table join can be joined with directly. You only need a `store` on the join if you want to query the saved table.
- **Co-partitioning still applies**: If the saved result changed its key, for example with `transformKey` or `groupBy`, it must still be co-partitioned with the stream or table it is joined with. See [Co-partitioning Requirements](#co-partitioning-requirements).

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

The table below summarizes when each variant produces output:

| Incoming data                    | `join`         | `leftJoin`             | `outerJoin`            |
|----------------------------------|----------------|------------------------|------------------------|
| Match found on both sides        | Joined result  | Joined result          | Joined result          |
| Left side only (no match)        | No output      | Left value, right null | Left value, right null |
| Right side only (no match)       | No output      | No output              | Left null, right value |

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