# KSML Migration Guide

This document provides guidance for migrating between different versions of KSML, as well as migrating from other stream processing frameworks to KSML.

## Migrating Between KSML Versions

### Migrating from 1.x to 2.x

KSML 2.x introduces several breaking changes and new features compared to KSML 1.x. This section outlines the key differences and provides guidance on how to update your KSML applications.

#### Key Changes

1. **New Pipeline Syntax**

   KSML 2.x introduces a more concise and expressive pipeline syntax:

   **KSML 1.x:**
   ```yaml
   pipelines:
     my_pipeline:
       source:
         stream: input_stream
       processors:
         - type: filter
           condition:
             expression: value.get("amount") > 0
         - type: mapValues
           mapper:
             code: transform_value(value)
       sink:
         stream: output_stream
   ```

   **KSML 2.x:**
   ```yaml
   pipelines:
     my_pipeline:
       from: input_stream
       via:
         - type: filter
           if:
             expression: value.get("amount") > 0
         - type: mapValues
           mapper:
             code: transform_value(value)
       to: output_stream
   ```

2. **Function Definitions**

   KSML 2.x requires explicit function types:

   **KSML 1.x:**
   ```yaml
   functions:
     transform_value:
       code: |
         return {"id": value.get("id"), "amount": value.get("amount") * 2}
   ```

   **KSML 2.x:**
   ```yaml
   functions:
     transform_value:
       type: mapper
       code: |
         return {"id": value.get("id"), "amount": value.get("amount") * 2}
   ```

3. **Stream Definitions**

   KSML 2.x uses a more structured approach for defining streams:

   **KSML 1.x:**
   ```yaml
   streams:
     input_stream: 
       topic: input-topic
       key-type: string
       value-type: json
   ```

   **KSML 2.x:**
   ```yaml
   streams:
     input_stream:
       topic: input-topic
       keyType: string
       valueType: json
   ```

4. **Error Handling**

   KSML 2.x introduces improved error handling capabilities:

   **KSML 1.x:**
   ```yaml
   # Limited error handling capabilities
   ```

   **KSML 2.x:**
   ```yaml
   pipelines:
     my_pipeline:
       from: input_stream
       via:
         - type: try
           operations:
             - type: mapValues
               mapper:
                 code: process_data(value)
           catch:
             - type: mapValues
               mapper:
                 code: |
                   log.error("Error processing data: {}", exception)
                   return {"error": str(exception), "original": value}
       to: output_stream
   ```

#### Migration Steps

1. **Update Stream Definitions**
   - Replace hyphenated property names with camelCase (e.g., `key-type` → `keyType`)
   - Review and update data type definitions

2. **Update Function Definitions**
   - Add explicit function types to all functions
   - Review function parameters and return types

3. **Update Pipeline Definitions**
   - Replace `source` and `sink` with `from` and `to`
   - Replace `processors` with `via`
   - Update operation syntax (e.g., `condition` → `if`)

4. **Update Error Handling**
   - Implement the new error handling capabilities where appropriate

5. **Test Thoroughly**
   - Run tests to ensure the migrated application behaves as expected
   - Monitor for any performance differences

### Migrating from 2.x to 3.x

KSML 3.x focuses on performance improvements, enhanced state management, and better integration with external systems.

#### Key Changes

1. **State Store Management**

   KSML 3.x introduces a more flexible state store configuration:

   **KSML 2.x:**
   ```yaml
   tables:
     user_profiles:
       topic: user-profiles
       keyType: string
       valueType: avro:UserProfile
       store: user_profiles_store
   ```

   **KSML 3.x:**
   ```yaml
   tables:
     user_profiles:
       topic: user-profiles
       keyType: string
       valueType: avro:UserProfile
       store:
         name: user_profiles_store
         type: persistent
         config:
           retention.ms: 604800000  # 7 days
   ```

2. **Enhanced Schema Management**

   KSML 3.x provides better schema evolution support:

   **KSML 2.x:**
   ```yaml
   streams:
     sensor_data:
       topic: sensor-readings
       keyType: string
       valueType: avro:SensorReading
       schemaRegistry: http://schema-registry:8081
   ```

   **KSML 3.x:**
   ```yaml
   streams:
     sensor_data:
       topic: sensor-readings
       keyType: string
       valueType: avro:SensorReading
       schema:
         registry: http://schema-registry:8081
         compatibility: BACKWARD
         subject: sensor-readings-value
   ```

3. **Improved Metrics**

   KSML 3.x offers more comprehensive metrics and monitoring:

   **KSML 2.x:**
   ```yaml
   config:
     metrics:
       reporters:
         - type: jmx
   ```

   **KSML 3.x:**
   ```yaml
   config:
     metrics:
       reporters:
         - type: jmx
         - type: prometheus
           port: 8080
           path: /metrics
       tags:
         application: "ksml-app"
         environment: "${ENV:-dev}"
   ```

#### Migration Steps

1. **Update State Store Configurations**
   - Expand state store configurations with the new options
   - Consider retention periods and cleanup policies

2. **Update Schema Management**
   - Enhance schema configurations with compatibility settings
   - Review subject naming strategies

3. **Enhance Metrics Configuration**
   - Add additional metrics reporters as needed
   - Configure tags for better metrics organization

4. **Test Thoroughly**
   - Verify state store behavior
   - Check schema evolution handling
   - Monitor metrics collection

## Migrating from Other Frameworks to KSML

### Migrating from Kafka Streams Java API

If you're currently using the Kafka Streams Java API directly, migrating to KSML can significantly reduce code complexity while maintaining the same processing capabilities.

#### Example Migration

**Kafka Streams Java API:**
```
// Example Java code (simplified for illustration)
StreamsBuilder builder = new StreamsBuilder();

// Define streams
KStream<String, Order> orders = builder.stream(
    "orders",
    Consumed.with(Serdes.String(), orderSerde)
);

// Filter and transform
KStream<String, EnrichedOrder> processedOrders = orders
    .filter((key, order) -> order.getAmount() > 0)
    .mapValues(order -> {
        EnrichedOrder enriched = new EnrichedOrder();
        enriched.setOrderId(order.getOrderId());
        enriched.setAmount(order.getAmount());
        enriched.setProcessedTime(System.currentTimeMillis());
        return enriched;
    });

// Write to output topic
processedOrders.to(
    "processed-orders",
    Produced.with(Serdes.String(), enrichedOrderSerde)
);
```

**Equivalent KSML:**
```yaml
streams:
  orders:
    topic: orders
    keyType: string
    valueType: avro:Order

  processed_orders:
    topic: processed-orders
    keyType: string
    valueType: avro:EnrichedOrder

functions:
  enrich_order:
    type: mapper
    code: |
      return {
        "orderId": value.get("orderId"),
        "amount": value.get("amount"),
        "processedTime": int(time.time() * 1000)
      }

pipelines:
  process_orders:
    from: orders
    via:
      - type: filter
        if:
          expression: value.get("amount") > 0
      - type: mapValues
        mapper:
          code: enrich_order(key, value)
    to: processed_orders
```

#### Migration Steps

1. **Identify Streams and Tables**
   - Map each `KStream` and `KTable` to KSML stream and table definitions
   - Determine key and value types

2. **Extract Processing Logic**
   - Convert stateless operations (filter, map, etc.) to KSML operations
   - Extract complex logic into KSML functions

3. **Define Pipelines**
   - Create KSML pipelines that connect streams and operations
   - Ensure the processing order is maintained

4. **Configure Serialization**
   - Set up appropriate serialization formats in KSML
   - Configure schema registry if using AVRO

5. **Test and Validate**
   - Compare the output of both implementations
   - Verify performance characteristics

### Migrating from Kafka Connect Transformations

If you're using Kafka Connect with SMTs (Single Message Transformations), you can migrate some of this logic to KSML for more flexible processing.

#### Example Migration

**Kafka Connect SMT Configuration:**
```properties
transforms=extractField,filterNull
transforms.extractField.type=org.apache.kafka.connect.transforms.ExtractField$Value
transforms.extractField.field=payload
transforms.filterNull.type=org.apache.kafka.connect.transforms.Filter
transforms.filterNull.predicate=isNull
transforms.filterNull.negate=true
```

**Equivalent KSML:**
```yaml
streams:
  input_stream:
    topic: connect-input
    keyType: string
    valueType: json

  output_stream:
    topic: processed-output
    keyType: string
    valueType: json

pipelines:
  process_connect_data:
    from: input_stream
    via:
      - type: mapValues
        mapper:
          expression: value.get("payload")
      - type: filter
        if:
          expression: value is not None
    to: output_stream
```

#### Migration Steps

1. **Identify Connect Pipelines**
   - Map Kafka Connect source and sink connectors to KSML streams
   - Identify the transformations being applied

2. **Convert Transformations to KSML Operations**
   - Map each SMT to an equivalent KSML operation
   - Combine multiple SMTs into a single KSML pipeline

3. **Enhance with KSML Capabilities**
   - Add additional processing that wasn't possible with SMTs
   - Implement stateful processing if needed

4. **Test and Validate**
   - Verify that the KSML pipeline produces the same output as the Connect pipeline
   - Check for any performance differences

### Migrating from Apache Flink

Migrating from Apache Flink to KSML involves mapping Flink's DataStream operations to KSML operations.

#### Example Migration

**Apache Flink:**
```
// Example Java code (simplified for illustration)
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Create a stream from a Kafka source
DataStream<Order> orders = env
    .addSource(new FlinkKafkaConsumer<>("orders", new OrderDeserializationSchema(), properties));

// Process the stream
DataStream<EnrichedOrder> processedOrders = orders
    .filter(order -> order.getAmount() > 0)
    .map(order -> {
        EnrichedOrder enriched = new EnrichedOrder();
        enriched.setOrderId(order.getOrderId());
        enriched.setAmount(order.getAmount());
        enriched.setProcessedTime(System.currentTimeMillis());
        return enriched;
    });

// Write to Kafka sink
processedOrders
    .addSink(new FlinkKafkaProducer<>("processed-orders", new EnrichedOrderSerializationSchema(), properties));
```

**Equivalent KSML:**
```yaml
streams:
  orders:
    topic: orders
    keyType: string
    valueType: avro:Order

  processed_orders:
    topic: processed-orders
    keyType: string
    valueType: avro:EnrichedOrder

functions:
  enrich_order:
    type: mapper
    code: |
      return {
        "orderId": value.get("orderId"),
        "amount": value.get("amount"),
        "processedTime": int(time.time() * 1000)
      }

pipelines:
  process_orders:
    from: orders
    via:
      - type: filter
        if:
          expression: value.get("amount") > 0
      - type: mapValues
        mapper:
          code: enrich_order(key, value)
    to: processed_orders
```

#### Migration Steps

1. **Identify Stream Sources and Sinks**
   - Map Flink sources to KSML stream definitions
   - Map Flink sinks to KSML stream destinations

2. **Convert DataStream Operations**
   - Map Flink's DataStream operations to KSML operations
   - Extract complex logic into KSML functions

3. **Handle State Differently**
   - Redesign stateful operations to use Kafka Streams state stores
   - Consider windowing differences between Flink and Kafka Streams

4. **Adjust for Processing Guarantees**
   - Configure KSML for appropriate processing guarantees
   - Be aware of the differences in exactly-once semantics

5. **Test and Validate**
   - Compare the output of both implementations
   - Monitor performance differences

## Best Practices for Migration

1. **Incremental Migration**
   - Migrate one pipeline or component at a time
   - Run old and new implementations in parallel initially

2. **Thorough Testing**
   - Create comprehensive test cases before migration
   - Verify that the migrated application produces the same results

3. **Performance Monitoring**
   - Monitor performance metrics before and after migration
   - Tune configurations as needed

4. **Documentation**
   - Document the migration process and decisions
   - Update operational procedures for the new implementation

5. **Training**
   - Provide training for team members on KSML
   - Highlight the differences from the previous framework

## Related Resources

- [KSML Language Reference](../reference/language-reference.md)
- [Operations Reference](../reference/operations-reference.md)
- [Functions Reference](../reference/functions-reference.md)
- [Configuration Reference](../reference/configuration-reference.md)
- [Troubleshooting Guide](troubleshooting.md)
