# Branching in KSML: Conditional Message Routing

## What is Branching?

Branching in KSML allows you to split a stream into multiple paths based on conditions. Each branch can apply different transformations and route messages to different output topics. This is essential for building sophisticated data pipelines that need to handle different types of data or route messages based on business rules.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_input && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic datacenter_sensors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic warehouse_sensors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic office_sensors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic unknown_sensors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic order_input && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic priority_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic regional_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic international_orders && \
    ```

## Relationship to Kafka Streams

KSML branching is built on top of Kafka Streams' `BranchedKStream` functionality. When you define a `branch` section in KSML:

1. **KSML generates Kafka Streams code** that calls `KStream.split()` to create a `BranchedKStream`
2. **Each branch condition** becomes a predicate function passed to `BranchedKStream.branch()`
3. **Branch order matters** - messages are evaluated against conditions in the order they're defined
4. **Default branches** handle messages that don't match any specific condition

This abstraction allows you to define complex routing logic in YAML without writing Java code.

## Basic Branching Syntax

The basic structure of branching in KSML:

```yaml
pipelines:
  my_pipeline:
    from: input_stream
    via:
      # Optional transformations before branching
      - type: mapValues
        mapper: some_transformer
    branch:
      # Branch 1: Messages matching a condition
      - if: condition_predicate
        via:
          # Optional transformations for this branch
          - type: mapValues
            mapper: branch1_transformer
        to: output_topic_1
      
      # Branch 2: Another condition
      - if: another_predicate
        to: output_topic_2
      
      # Default branch: Everything else
      - to: default_output_topic
```

Each message goes to **one** branch only: the first branch whose condition matches. KSML checks the conditions from top to bottom, and the default branch (a branch without `if`) catches everything that matched none of them:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    InputStream[(input_stream)]
    Via[some_transformer]

    %% Branch conditions, checked in order
    Condition1{"<code>branch</code> 1<br/>if: condition_predicate"}
    Condition2{"<code>branch</code> 2<br/>if: another_predicate"}
    Branch1[branch1_transformer]

    %% Outputs
    Output1[(output_topic_1)]
    Output2[(output_topic_2)]
    DefaultOutput[(default_output_topic)]

    %% Data Flow
    InputStream --> Via
    Via --> Condition1
    Condition1 -->|"yes"| Branch1
    Branch1 --> Output1
    Condition1 -->|"no"| Condition2
    Condition2 -->|"yes"| Output2
    Condition2 -->|"no: default branch"| DefaultOutput

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class InputStream,Output1,Output2,DefaultOutput topic
    class Via,Condition1,Condition2,Branch1 operation
```

Without a default branch, messages that match no condition are dropped.

## Example 1: Simple Content-Based Routing

Let's start with a basic example that routes sensor data based on location.

### Producer Definition

??? info "Sensor Data Producer - click to expand"

    ```yaml
    --8<-- "docs-examples/intermediate-tutorial/branching/producer-sensor-locations.yaml"
    ```

### Processor Definition  

??? info "Location-Based Routing Processor - click to expand"

    ```yaml
    --8<-- "docs-examples/intermediate-tutorial/branching/processor-location-routing.yaml"
    ```

This example demonstrates:

- **Simple condition matching**: Routes messages based on sensor location
- **Multiple branches**: Separate paths for data center, warehouse, and office sensors
- **Default branch**: Handles sensors from unknown locations
- **Logging**: Each branch logs messages for monitoring

**Expected Behavior:**

- Messages from "data_center" are routed to `datacenter_sensors` topic
- Messages from "warehouse" are routed to `warehouse_sensors` topic  
- Messages from "office" are routed to `office_sensors` topic
- All other locations are routed to `unknown_sensors` topic

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    InputTopic[(sensor_input)]
    LogLocation[<code>peek</code><br/>Log location]
    Branch{"<code>branch</code><br/>on location"}

    %% Outputs
    DataCenter[(datacenter_sensors)]
    Warehouse[(warehouse_sensors)]
    Office[(office_sensors)]
    Unknown[(unknown_sensors)]

    %% Data Flow
    InputTopic --> LogLocation
    LogLocation --> Branch
    Branch -->|"is_datacenter"| DataCenter
    Branch -->|"is_warehouse"| Warehouse
    Branch -->|"is_office"| Office
    Branch -.->|"default"| Unknown

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class InputTopic,DataCenter,Warehouse,Office,Unknown topic
    class LogLocation,Branch operation
```

Each branch also has its own `peek` operation that logs the routed message before writing it to its topic.

## Example 2: Multi-Condition Data Processing Pipeline

This example shows more complex branching with data transformation and business logic.

### Producer Definition

??? info "Order Events Producer - click to expand"

    ```yaml
    --8<-- "docs-examples/intermediate-tutorial/branching/producer-order-events.yaml"
    ```

### Processor Definition

??? info "Order Processing Pipeline - click to expand"

    ```yaml
    --8<-- "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml"
    ```

This example demonstrates:

- **Complex conditions**: Multiple criteria (order value, customer type, region)
- **Transformations per branch**: Different processing logic for each order type
- **Business rules**: Priority routing based on customer tier and order value
- **Data enrichment**: Adding processing timestamps and status fields

**Expected Behavior:**

- High-value orders (>$1000) from premium customers are routed to `priority_orders` topic
- US/EU orders (not priority) are routed to `regional_orders` topic  
- International orders (APAC/LATAM) are routed to `international_orders` topic

Because the conditions are checked in order, a high-value order from a premium customer goes to `priority_orders` even if it comes from an international region:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    InputTopic[(order_input)]
    LogOrder[<code>peek</code><br/>Log order]

    %% Branch conditions, checked in order
    IsPriority{"<code>branch</code> 1: is_priority<br/>premium and > $1000"}
    IsRegional{"<code>branch</code> 2: is_regional<br/>US or EU"}
    IsInternational{"<code>branch</code> 3: is_international<br/>other regions"}

    %% Branch transformations
    Priority["<code>mapValues</code><br/>tier: priority, SLA 4h"]
    Regional["<code>mapValues</code><br/>tier: regional, SLA 24h"]
    International["<code>mapValues</code><br/>tier: international, SLA 72h,<br/>customs required"]

    %% Outputs
    PriorityOrders[(priority_orders)]
    RegionalOrders[(regional_orders)]
    InternationalOrders[(international_orders)]

    %% Data Flow
    InputTopic --> LogOrder
    LogOrder --> IsPriority
    IsPriority -->|"yes"| Priority
    Priority --> PriorityOrders
    IsPriority -->|"no"| IsRegional
    IsRegional -->|"yes"| Regional
    Regional --> RegionalOrders
    IsRegional -->|"no"| IsInternational
    IsInternational -->|"yes"| International
    International --> InternationalOrders

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class InputTopic,PriorityOrders,RegionalOrders,InternationalOrders topic
    class LogOrder,IsPriority,IsRegional,IsInternational,Priority,Regional,International operation
```

## Advanced Branching Patterns

### Nested Branching

You can create nested branching by having branches that contain their own branching logic:

```yaml
pipelines:
  nested_example:
    from: input_stream
    branch:
      - if: 
          expression: value.get("category") == "electronics"
        via:
          - type: transformValue
            mapper: enrich_electronics
        branch:
          - if:
              expression: value.get("price") > 500
            to: expensive_electronics
          - to: affordable_electronics
      
      - if:
          expression: value.get("category") == "clothing" 
        to: clothing_items
```

The electronics branch has its own `branch` section that splits electronics by price. The outer branching has no default branch, so messages from any other category are dropped:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    InputStream[(input_stream)]
    Category{"<code>branch</code><br/>on category"}
    Enrich[<code>transformValue</code><br/>enrich_electronics]
    Price{"nested <code>branch</code><br/>price > 500"}

    %% Outputs
    Expensive[(expensive_electronics)]
    Affordable[(affordable_electronics)]
    Clothing[(clothing_items)]

    %% Data Flow
    InputStream --> Category
    Category -->|"electronics"| Enrich
    Enrich --> Price
    Price -->|"yes"| Expensive
    Price -.->|"no: default"| Affordable
    Category -->|"clothing"| Clothing
    Category -.->|"other"| Drop[❌ Dropped]

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class InputStream,Expensive,Affordable,Clothing topic
    class Category,Enrich,Price operation
    class Drop dropped
```

### Multiple Output Topics

A single branch can route to multiple topics:

```yaml
branch:
  - if: urgent_condition
    to:
      - urgent_processing
      - audit_log
      - notifications
```

### Branch with Complex Via Operations

Branches can contain multiple transformation steps:

```yaml
branch:
  - if: needs_processing
    via:
      - type: filter
        if: additional_validation
      - type: transformValue
        mapper: enrich_data
      - type: mapKey
        mapper: generate_new_key
      - type: peek
        forEach:
          code: |
            log.info("Processed: {}", value)
    to: processed_output
```

## Error Handling in Branching

Branching is commonly used for error handling patterns:

```yaml
functions:
  is_valid:
    type: predicate
    code: |
      try:
        # Validation logic
        required_fields = ["id", "timestamp", "data"]
        return all(field in value for field in required_fields)
      except:
        return False

  is_error:
    type: predicate
    expression: "'error' in value or value.get('status') == 'failed'"

pipelines:
  error_handling:
    from: input_stream
    via:
      - type: mapValues
        mapper: safe_processor  # This adds error info if processing fails
    branch:
      # Route errors to dead letter queue
      - if: is_error
        to: error_topic
      
      # Route valid data for further processing  
      - if: is_valid
        to: processed_data
      
      # Route everything else to manual review
      - to: manual_review
```

## Best Practices

### 1. Order Matters
Branches are evaluated in the order they're defined. Place more specific conditions first:

```yaml
branch:
  # Specific condition first
  - if:
      expression: value.get("priority") == "urgent" and value.get("amount") > 10000
    to: urgent_high_value
  
  # General condition second  
  - if:
      expression: value.get("priority") == "urgent"
    to: urgent_orders
  
  # Default branch last
  - to: standard_orders
```

### 2. Use Meaningful Branch Names
When working with complex branching, use the name attribute for clarity:

```yaml
branch:
  - name: high_priority_branch
    if: high_priority_predicate
    to: priority_queue
```

### 3. Keep Conditions Simple
Complex logic should be in predicate functions, not inline expressions:

```yaml
functions:
  is_complex_condition:
    type: predicate
    code: |
      # Complex business logic here
      return (value.get("score") > 85 and 
              value.get("region") in ["US", "EU"] and
              len(value.get("tags", [])) > 2)

branch:
  - if: is_complex_condition  # Clean and readable
    to: qualified_items
```

### 4. Handle All Cases
Always ensure messages have somewhere to go:

```yaml
branch:
  - if: condition_a
    to: topic_a
  - if: condition_b  
    to: topic_b
  # Always include a default case
  - to: default_topic
```

### 5. Add Monitoring
Use `peek` operations to monitor branch behavior:

```yaml
branch:
  - if: important_condition
    via:
      - type: peek
        forEach:
          code: |
            log.info("Important message routed: {}", key)
    to: important_topic
```

## Performance Considerations

### Predicate Function Performance
- Keep predicate functions lightweight
- Avoid expensive operations in conditions
- Cache results when possible

### Branch Count
- Too many branches can impact performance
- Consider using lookup tables for many conditions
- Group similar conditions when possible

### Memory Usage
- Each branch creates a separate stream processing path
- Monitor memory usage with many concurrent branches

## Conclusion

Branching in KSML provides powerful conditional routing capabilities that map directly to Kafka Streams' branching functionality. It enables you to:

- **Route messages** based on content, business rules, or data quality
- **Apply different transformations** to different types of data
- **Implement error handling** patterns like dead letter queues
- **Build complex pipelines** with multiple processing paths

## Next Steps

- [Reference: Branch Operations](../../reference/operation-reference.md/#branching-and-merging)