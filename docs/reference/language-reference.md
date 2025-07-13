# KSML Language Reference

This document provides a comprehensive reference for the KSML (Kafka Streams Markup Language) syntax and structure. It
covers all aspects of the KSML language specification, including file structure, data types, and configuration options.

## KSML File Structure

A KSML definition file is written in YAML and consists of several top-level sections:

```yaml
name: my-ksml-app
version: 0.1.2
description: This is what my app does

streams:
# Stream definitions

tables:
# Table definitions

globalTables:
# GlobalTable definitions

stores:
# State store definitions

functions:
# Function definitions

pipelines:
# Pipeline definitions

producers:
# Producer definitions
```

### Required and Optional Sections

- **name**: Optional. Defines the name of your application.
- **version**: Optional. Defines the version of your application.
- **description**: Optional. Defines the purpose of your application.
- **streams**: Optional. Defines the input and output Kafka streams.
- **tables**: Optional. Defines the input and output Kafka tables.
- **globalTables**: Optional. Defines the input and output Kafka globalTables.
- **stores**: Optional. Defines the state stores used in your functions and pipelines.
- **functions**: Optional. Defines reusable functions that can be called from pipelines.
- **pipelines**: Optional. Defines the processing logic of your application.
- **producers**: Optional. Defines the producers that generate data for your output topics.

## Metadata Section

The `name`, `version`, and `description` fields hold informational data for the developer of the application.

## Streams, Tables, and GlobalTables Section

The `streams`, `tables`, and `globalTables` sections defines the input and output Kafka topics and their data types.

### Definition

```yaml
streams:
  stream_name:
    topic: kafka_topic_name
    keyType: key_data_type
    valueType: value_data_type
    # Additional configuration options

tables:
  table_name:
    topic: kafka_topic_name
    keyType: key_data_type
    valueType: value_data_type
    # Additional configuration options

globalTables:
  global_table_name:
    topic: kafka_topic_name
    keyType: key_data_type
    valueType: value_data_type
    # Additional configuration options
```

### Required Properties

- `topic`: (Required) The name of the Kafka topic.
- `keyType`: (Required) The data type of the message key.
- `valueType`: (Required) The data type of the message value.
- `offsetResetPolicy`: (Optional) The policy to use when there is no (valid) consumer group offset in Kafka. Choice of
  `earliest`, `latest`, `none`, or `by_duration:<duration>`. In the latter case, you can pass in a custom duration.
- `timestampExtractor`: (Optional) A function that is able to extract a timestamp from a consumed message, to be used by
  Kafka Streams as the message timestamp in all pipeline processing.
- `partitioner`: (Optional) A stream partitioner that determines to which topic partitions an output record needs to be
  written.

### Example

```yaml
streams:
  orders:
    topic: incoming_orders
    keyType: string
    valueType: json
```

## Functions Section

The `functions` section defines reusable functions that can be called from pipelines.

### Function Definition

```yaml
functions:
  function_name:
    type: function_type
    parameters:
      - name: parameter_name
        type: parameter_type
    code: |
      # Python code here
    expression: |
      # Python expression here
    resultType: data_type
```

### Required Properties

- **type**: The type of function (e.g., `predicate`, `aggregator`, `keyValueMapper`).
- **parameters**: List of custom parameters, which may be passed in next to the default parameters determined by the
  function type.
- **code**: Python code that implements the function.
- **expression**: Python expression for the return value of the function.
- **resultType**: The return value data type of the function.

### Parameter Definition

- **name**: The name of the parameter.
- **type**: The data type of the parameter.

### Example

```yaml
functions:
  calculate_total:
    type: mapValues
    parameters:
      - name: order
        type: struct
    code: |
      total = 0
      for item in order.get("items", []):
        total += item.get("price", 0) * item.get("quantity", 0)
    expression: |
      {"order_id": order.get("id"), "total": total}
    resultType: struct
```

## Pipelines Section

The `pipelines` section defines the processing logic that connects streams.

### Pipeline Definition

```yaml
pipelines:
  pipeline_name:
    from: input_stream
    via:
      - type: operation_type
        # Operation-specific parameters
    to: output_stream
```

### Required Properties

- **from**: The input stream.
- **to**: The output stream.

### Optional Properties

- **via**: List of operations to apply to the stream.

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
        mapper:
          code: calculate_total(value)
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
- **windowedBy**: Groups records into time windows.

### Operation Parameters

Each operation type has its own set of parameters. Common parameters include:

- **mapper**: For map operations, specifies the transformation.
- **if**: For filter operations, specifies the condition.
- **initializer**: For aggregate operations, specifies the initial value.
- **aggregator**: For aggregate operations, specifies how to combine values.

## Data Types

KSML supports various [data types](data-types-reference.md) for stream keys and values.

### Type Conversion

KSML automatically handles type conversion between compatible types. For example, a JSON string can be converted to an
AVRO message if the structure (schema) matches.

## Expressions

KSML supports two types of expressions:

### YAML Expressions

Simple expressions can be defined directly in YAML:

```yaml
expression: value.get("total") > 100
```

### Python Code Blocks

More complex logic can be implemented using Python code blocks:

```yaml
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
    parameters:
      - name: order
        type: struct
    code: |
      total = 0
      for item in order.get("items", []):
        total += item.get("price", 0) * item.get("quantity", 0)
      return {**order, "total": total}
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
    parameters:
      - name: order
        type: struct
      - name: customer
        type: struct
    code: |
      customer = value.get("customer")
      if customer is None:
        return order
      return {
        **order,
        "customer_name": customer.get("name"),
        "customer_tier": customer.get("tier", "standard"),
        "loyalty_points": order.get("total", 0) * 0.1
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
