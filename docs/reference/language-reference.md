# Language Reference

This document provides a comprehensive reference for the KSML (Kafka Streams Markup Language) syntax and structure. It
covers all aspects of the KSML language specification, including file structure, data types, and configuration options.

## KSML File Structure

A KSML definition file is written in YAML and consists of several top-level sections:

```yaml
{% include "../definitions/reference/template-definition.yaml" %}
```

## Application Metadata

### Basic Metadata

| Property      | Type   | Required | Description                          |
|---------------|--------|----------|--------------------------------------|
| `name`        | String | No       | The name of the KSML definition      |
| `version`     | String | No       | The version of the KSML definition   |
| `description` | String | No       | A description of the KSML definition |

Example:

```yaml
name: "order-processing-app"
version: "1.2.3"
description: "Processes orders from the order topic and enriches them with customer data"
```

## Data source and target definitions

### Streams

Streams represent unbounded sequences of records.

| Property             | Type   | Required | Description                                                                  |
|----------------------|--------|----------|------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to                                     |
| `keyType`            | String | Yes      | The type of the record key                                                   |
| `valueType`          | String | Yes      | The type of the record value                                                 |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`)                       |
| `timestampExtractor` | String | No       | The function to extract timestamps from records                              |
| `partitioner`        | String | No       | The function that determines to which topic partitions a message is produced |

Example:

```yaml
streams:
  orders:
    topic: "orders"
    keyType: "string"
    valueType: "avro:Order"
    offsetResetPolicy: "earliest"
```

### Tables

Tables represent changelog streams from a primary-keyed table.

| Property             | Type   | Required | Description                                                                  |
|----------------------|--------|----------|------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to                                     |
| `keyType`            | String | Yes      | The type of the record key                                                   |
| `valueType`          | String | Yes      | The type of the record value                                                 |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`)                       |
| `timestampExtractor` | String | No       | The function to extract timestamps from records                              |
| `partitioner`        | String | No       | The function that determines to which topic partitions a message is produced |
| `store`              | String | No       | The name of the key/value state store to use                                 |

Example:

```yaml
tables:
  customers:
    topic: "customers"
    keyType: "string"
    valueType: "avro:Customer"
    store: "customer-store"
```

### Global Tables

Global tables are similar to tables but are fully replicated on each instance of the application.

| Property             | Type   | Required | Description                                                                  |
|----------------------|--------|----------|------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from                                                 |
| `keyType`            | String | Yes      | The type of the record key                                                   |
| `valueType`          | String | Yes      | The type of the record value                                                 |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`)                       |
| `timestampExtractor` | String | No       | The function to extract timestamps from records                              |
| `partitioner`        | String | No       | The function that determines to which topic partitions a message is produced |
| `store`              | String | No       | The name of the key/value state store to use                                 |

Example:

```yaml
globalTables:
  products:
    topic: "products"
    keyType: "string"
    valueType: "avro:Product"
```

## Function Definitions

Functions define reusable pieces of logic that can be referenced in pipelines.

| Property     | Type      | Required  | Description                                                                                                                                                      |
|--------------|-----------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`       | String    | Yes       | The type of function (predicate, mapper, aggregator, etc.)                                                                                                       |
| `parameters` | Array     | No        | Parameters for the function                                                                                                                                      |
| `globalCode` | String    | No        | Python code executed once upon startup                                                                                                                           |
| `code`       | String    | No        | Python code implementing the function                                                                                                                            |
| `expression` | String    | No        | An expression that the function will return as value                                                                                                             |
| `resultType` | Data type | Sometimes | The data type returned by the function. This is sometimes derived from the function `type`, but where it is not, you need to explicitly declare the result type. |

Example:

```yaml
functions:
  is_valid_order:
    type: "predicate"
    code: |
      if value is None:
        return False

      if "orderId" not in value:
        return False

      if "items" not in value or not value["items"]:
        return False
    expression: True

  enrich_order:
    type: "mapper"
    expression: |
      {
        "order_id": value.get("orderId"),
        "customer_id": value.get("customerId"),
        "items": value.get("items", []),
        "total": sum(item.get("price", 0) * item.get("quantity", 0) for item in value.get("items", [])),
        "timestamp": value.get("timestamp", int(time.time() * 1000))
      }
```

## Pipeline Definitions

Pipelines define the flow of data through the application.

| Property | Type         | Required | Description                         |
|----------|--------------|----------|-------------------------------------|
| `from`   | String/Array | Yes      | The source stream(s) or table(s)    |
| `via`    | Array        | No       | The operations to apply to the data |
| `to`     | String/Array | Yes      | The destination stream(s)           |

Example:

```yaml
pipelines:
  process_orders:
    from: "orders"
    via:
      - type: "filter"
        if:
          code: "is_valid_order(key, value)"
      - type: "mapValues"
        mapper:
          code: "enrich_order(key, value)"
      - type: "peek"
        forEach:
          code: |
            log.info("Processing order: {}", value.get("order_id"))
    to: "processed_orders"
```

### Example

```yaml
pipelines:
  order_processing:
    from: orders
    via:
      - type: filter
        if:
          expression: value.get("total") > 100
      - type: mapValues
        mapper: calculate_total
    to: processed_orders
```

## Operations

Operations are the building blocks of pipelines. They transform, filter, or aggregate data.

### Common Operation Types

#### Stateless Operations

- **mapValues**: Transforms the value of each record.
- **map**: Transforms both the key and value of each record.
- **filter**: Keeps only records that satisfy a condition.
- **flatMap**: Transforms each record into zero or more records.
- **peek**: Performs a side effect on each record without changing it.

#### Stateful Operations

- **aggregate**: Aggregates records by key.
- **count**: Counts records by key.
- **reduce**: Combines records with the same key.
- **join**: Joins two streams based on key.
- **windowBySession**: Groups records into time windows.
- **windowByTime**: Groups records into time windows.

### Operation Parameters

Each operation type has its own set of parameters. Common parameters include:

- **mapper**: For map operations, specifies the transformation.
- **if**: For filter operations, specifies the condition.
- **initializer**: For aggregate operations, specifies the initial value.
- **aggregator**: For aggregate operations, specifies how to combine values.

## Data Types

KSML supports various [data types](data-type-reference.md) for stream keys and values.

### Type Conversion

KSML automatically handles type conversion between compatible types. For example, a JSON string can be converted to an
AVRO message if the structure (schema) matches.

## Returning a value from a function

KSML supports two methods to return a value from a function:

### Python Expressions

Simple expressions can be defined directly in YAML:

```yaml
functions:
  my_function:
    type: predicate
    expression: value.get("total") > 100
```

### Python Code Blocks

More complex logic can be implemented using Python code blocks:

```yaml
functions:
  my_function:
    type: predicate
    code: |
      if value.get("status") == "COMPLETED" and value.get("total") > 100:
        return True
      return False
```

## Best Practices

### Naming Conventions

- Use descriptive names for streams, functions, and pipelines.
- Use snake_case for state store, function and pipeline names.

### Code Organization

- Keep functions small and focused on a single task.
- Reuse functions across pipelines when possible.
- Group related pipelines together.

### Performance Considerations

- Use stateless operations when possible.
- Be mindful of window sizes in windowed operations.
- Consider partitioning when designing your pipelines.

## Resources

## Appendix: Full Example

```yaml
streams:
  orders:
    topic: incoming_orders
    keyType: string
    valueType: json

  customers:
    topic: customer_data
    keyType: string
    valueType: json

  enriched_orders:
    topic: processed_orders
    keyType: string
    valueType: json

functions:
  calculate_total:
    type: valueTransformer
    code: |
      total = 0
      for item in value.get("items", []):
        total += item.get("price", 0) * item.get("quantity", 0)
      return {**value, "total": total}
    resultType: struct

  join_with_customers:
    type: valueJoiner
    code: |
      return {
        **value1,
        "customer": value2
      }
    resultType: struct

  enrich_order:
    type: valueTransformer
    code: |
      customer = value.get("customer")
      if customer is None:
        return value
      return {
        **value,
        "customer_name": customer.get("name"),
        "customer_tier": customer.get("tier", "standard"),
        "loyalty_points": value.get("total", 0) * 0.1
      }
    resultType: struct

pipelines:
  order_processing:
    from: orders
    via:
      - type: transformValue
        mapper: calculate_total
      - type: join
        with: customers
        valueJoiner: join_with_customers
      - type: transformValue
        mapper: enrich_order
      - type: filter
        if:
          expression: value.get("total") > 0
    to: enriched_orders
```

This example demonstrates a complete KSML application that processes orders, calculates totals, enriches them with
customer data, and filters out zero-total orders.

