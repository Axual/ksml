# KSML Language Reference

This document provides a comprehensive reference for the KSML (Kafka Streams Markup Language) syntax and structure. It covers all aspects of the KSML language specification, including file structure, data types, and configuration options.

## KSML File Structure

A KSML definition file is written in YAML and consists of several top-level sections:

```yaml
streams:
  # Stream definitions

functions:
  # Function definitions

pipelines:
  # Pipeline definitions

configuration:
  # Optional global configuration
```

### Required and Optional Sections

- **streams**: Required. Defines the input and output Kafka topics.
- **pipelines**: Required. Defines the processing logic that connects streams.
- **functions**: Optional. Defines reusable functions that can be called from pipelines.
- **configuration**: Optional. Provides global configuration settings.

## Streams Section

The `streams` section defines the input and output Kafka topics and their data types.

### Stream Definition

```yaml
streams:
  stream_name:
    topic: kafka_topic_name
    keyType: key_data_type
    valueType: value_data_type
    # Additional configuration options
```

### Required Properties

- **topic**: The name of the Kafka topic.
- **keyType**: The data type of the message key.
- **valueType**: The data type of the message value.

### Optional Properties

- **timestampExtractor**: Specifies how to extract timestamps from messages.
- **consumed**: Configuration for consuming from the topic.
- **produced**: Configuration for producing to the topic.

### Example

```yaml
streams:
  orders:
    topic: incoming_orders
    keyType: string
    valueType: json
    consumed:
      keySerde: org.apache.kafka.common.serialization.Serdes$StringSerde
      valueSerde: io.axual.ksml.data.json.JsonSerde
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
```

### Required Properties

- **type**: The type of function (e.g., `mapValues`, `filter`, `aggregate`).
- **parameters**: List of parameters the function accepts.
- **code**: Python code that implements the function.

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
        type: object
    code: |
      total = 0
      for item in order.get("items", []):
        total += item.get("price", 0) * item.get("quantity", 0)
      return {"order_id": order.get("id"), "total": total}
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

KSML supports various data types for stream keys and values.

### Primitive Types

- **string**: Text data.
- **long**: 64-bit integer.
- **integer**: 32-bit integer.
- **double**: 64-bit floating point.
- **boolean**: True/false value.

### Complex Types

- **json**: JSON data.
- **avro**: Avro data (requires schema).
- **xml**: XML data.
- **csv**: Comma-separated values.
- **binary**: Binary data.

### Type Conversion

KSML automatically handles type conversion between compatible types. For example, a JSON string can be converted to a POJO if the structure matches.

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

## Configuration Section

The `configuration` section provides global settings for the KSML application.

### Common Configuration Options

- **application.id**: The Kafka Streams application ID.
- **bootstrap.servers**: Kafka bootstrap servers.
- **default.key.serde**: Default key serializer/deserializer.
- **default.value.serde**: Default value serializer/deserializer.

### Example

```yaml
configuration:
  application.id: order-processing-app
  bootstrap.servers: kafka:9092
  default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
  default.value.serde: io.axual.ksml.data.json.JsonSerde
```

## Error Handling

KSML provides several mechanisms for error handling:

### Try-Catch Operations

```yaml
- type: try
  operations:
    - type: mapValues
      mapper:
        code: parse_complex_data(value)
  catch:
    - type: mapValues
      mapper:
        code: handle_error(value, exception)
```

### Dead Letter Queues

```yaml
streams:
  errors:
    topic: processing_errors
    keyType: string
    valueType: json

pipelines:
  main_pipeline:
    from: input
    via:
      - type: mapValues
        mapper:
          code: process_data(value)
        onError:
          sendTo: errors
          withKey: "error-" + key
          withValue: {"original": value, "error": exception.getMessage()}
    to: output
```

## Best Practices

### Naming Conventions

- Use descriptive names for streams, functions, and pipelines.
- Use camelCase for function and pipeline names.
- Use snake_case for parameter names.

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
    type: mapValues
    parameters:
      - name: order
        type: object
    code: |
      total = 0
      for item in order.get("items", []):
        total += item.get("price", 0) * item.get("quantity", 0)
      return {**order, "total": total}

  enrich_order:
    type: mapValues
    parameters:
      - name: order
        type: object
      - name: customer
        type: object
    code: |
      if customer is None:
        return order
      return {
        **order,
        "customer_name": customer.get("name"),
        "customer_tier": customer.get("tier", "standard"),
        "loyalty_points": order.get("total", 0) * 0.1
      }

pipelines:
  order_processing:
    from: orders
    via:
      - type: mapValues
        mapper:
          code: calculate_total(value)
      - type: join
        with: customers
      - type: mapValues
        mapper:
          code: enrich_order(value, foreignValue)
      - type: filter
        if:
          expression: value.get("total") > 0
    to: enriched_orders

configuration:
  application.id: order-processing-app
  bootstrap.servers: kafka:9092
  default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
  default.value.serde: io.axual.ksml.data.json.JsonSerde
```

This example demonstrates a complete KSML application that processes orders, calculates totals, enriches them with customer data, and filters out zero-total orders.
