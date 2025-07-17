# KSML Notations

This document provides a comprehensive reference for all notations available in KSML. Each notation may handle schema
and/or interaction with a schema registry differently.

## Introduction to notations

KSML uses _notations_ to allow reading/writing different message formats to Kafka topics. However, some notations
may require different backends (_serializers_ and _deserializers_, or _serdes_ for short).

### Using Notations

Notations are specified as a prefix to the schema name:

```yaml
# Example of AVRO notation with a specific schema
avroStream:
  valueType: "avro:SensorReading"

# Example of JSON notation (schemaless)
jsonStream:
  valueType: "json"

# Example of XML notation with a schema
xmlStream:
  valueType: "xml:CustomerData"
```

### Choosing the Right Notation

The choice of notation depends on your specific requirements:

| If you need...                              | Consider using... |
|---------------------------------------------|-------------------|
| Schema evolution and backward compatibility | AVRO or Protobuf  |
| Human-readable data for debugging           | JSON              |
| Integration with legacy systems             | XML or SOAP       |
| Simple tabular data                         | CSV               |
| Compact binary format                       | AVRO or Protobuf  |

## Using notations in your KSML definitions

### AVRO

AVRO is a binary format that supports schema evolution.

#### Syntax

```yaml
valueType: "avro:<schema_name>"
```

Where `<schema_name>` is the name of an AVRO schema.

#### Example

```yaml
streams:
  sensor_readings:
    topic: sensor-data
    keyType: string
    valueType: avro:SensorData
```

### CSV

CSV (Comma-Separated Values) is a simple tabular data format.

#### Syntax

```yaml
# For schemaless CSV:
valueType: "csv"
# For CSV with a schema:
resultType: "csv:<schema_name>"
```

#### Example

```yaml
streams:
  sales_data:
    topic: sales-data
    keyType: string
    valueType: "csv"
  inventory_data:
    topic: inventory-data
    keyType: string
    valueType: "csv:InventoryRecord"
```

### JSON

JSON is a text-based, human-readable format for data transfer. It can be used with and without a schema.

#### Syntax

```yaml
# For schemaless JSON:
valueType: "json"
# For JSON with a schema:
resultType: "json:<schema_name>"
```

#### Example

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "json"

  orders:
    topic: orders
    keyType: string
    valueType: "json:Order"
```

Python functions can also result in a JSON return value by returning a dictionary:

```yaml
functions:
  merge_key_value_data:
    type: valueTransformer
    expression: |
      { 'key': key, 'value': value }
    resultType: json

  convert_order_info:
    type: valueTransformer
    expression: |
      { 'order_id': key, 'customer_id': value.get('customer_id'), 'order_value': value.get('amount') }
    resultType: json:OrderSchema
```

### JSON Schema

JSON Schema adds vendor-specific schema support to the above JSON serialization format. This notation needs a schema
for its operations, which it may get from schema registry (upon consuming a message) or through explicit schema naming.

#### Syntax

```yaml
# For any JSON topic, where the concrete schema is fetched from SR
valueType: "jsonschema"
# For JSON with a schema:
resultType: "jsonschema:<schema_name>"
```

#### Example

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "jsonschema"

  orders:
    topic: orders
    keyType: string
    valueType: "jsonschema:Order"
```

### Protobuf

Protobuf is a popular encoding format (developed by Google) to allow for easy data exchange between applications of
any kind and written in any language. It always needs a schema for its operations, which it may get from a schema
registry (upon consuming a message) or through explicit schema naming.

#### Syntax

```yaml
# For any Protobuf topic, where the concrete schema is fetched from SR
valueType: "protobuf"
# For Protobuf with a schema:
resultType: "protobuf:<schema_name>"
```

#### Example

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "protobuf"

  orders:
    topic: orders
    keyType: string
    valueType: "protobuf:Order"
```

### SOAP

SOAP (Simple Object Access Protocol) is an XML-based messaging protocol.

#### Syntax

```yaml
valueType: "soap"
```

#### Example

```yaml
streams:
  service_requests:
    topic: service-requests
    keyType: string
    valueType: "soap"
```

### XML

XML (Extensible Markup Language) is used for complex hierarchical data.

#### Syntax

```yaml
# For schemaless XML:
valueType: "xml"
# For XML with a schema:
resultType: "xml:<schema_name>"
```

#### Example

```yaml
streams:
  customer_data:
    topic: customer-data
    keyType: string
    valueType: "xml:CustomerData"
```

## Schema Management

When working with structured data, it's important to manage your schemas effectively.

### Local files vs. Schema Registry

When a schema is specified, KSML will load the schema from a local file from the `schemaDirectory`. The notation
determines the type of schema and its filename extension. For instance, AVRO schemas will always have the `.avsc`
file extension, while XML schemas have the `.xsd` extension.

KSML always loads schemas from local files:

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: "avro:SensorReading"
```

In this example, the `SensorReading.avsc` file is looked up in the configured `schemaDirectory`.

Whenever a notation is used without specifying the concrete schema, KSML assumes the schema is loadable from Schema
Registry.

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: "avro"
```

In this example there is no schema specified, so KSML will use the schema registered in Schema Registry for the topic
value.

## Type Conversion

KSML automatically performs type conversion wherever required and possible. This holds for numbers (integer to long,
etc.) but also for string representation of certain notations. For instance between strings and CSV, JSON and XML. The
following example shows how a string value gets converted into a struct with specific fields by declaring it as CSV with
a specific schema.

```yaml
functions:
  generate_message:
    type: generator
    globalCode: |
      import random
      sensorCounter = 0
    code: |
      global sensorCounter

      # Generate key
      key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
      sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

      # Generate temperature data as CSV, Temperature.csv file contains: "type,unit,value"
      value = "TEMPERATURE,C,"+str(random.randrange(-100, 100)
    expression: (key, value)                      # Return a message tuple with the key and value
    resultType: (string, csv:Temperature)         # Value is converted to {"type":"TEMPERATURE", "unit":"C", "value":"83"}
```
