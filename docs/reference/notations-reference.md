# KSML Notations

This document provides a comprehensive reference for all notations available in KSML. Each notation may handle schema
and/or interaction with a schema registry differently.

## Introduction to notations

KSML uses _notations_ to allow reading/writing different message formats to Kafka topics. However, some notations
may require different backends (_serializers_ and _deserializers_, or _serdes_ for short).

## How to configure notations

To accommodate for variations in Kafka setups and/or ecosystem variations, the following table lists the available
options you can configure. The `notation name` lists the notation you use in your KSML definitions, for example
`avro:SensorData`. The configuration of all notations is done in the `ksml-runner.yaml` file, from which KSML reads its
technical configuration parameters.

## List of available supported variations

The table below provides a complete list of all notations, their possible implementation alternatives and corresponding
characteristics, such as what data types a notation maps to.

| Notation name | KSML defined serde name | Serde supplier | With(out) schema | Requires SR |  Schema source  | KSML data type | Loaded in KSML | Remarks                                             |
|---------------|-------------------------|:--------------:|:----------------:|:-----------:|:---------------:|----------------|:--------------:|-----------------------------------------------------|
| avro          | apicurio_avro           |    Apicurio    |     Without      |      Y      |       SR        | struct         | If configured  | Any AVRO, schema loaded from SR upon consume        |
| avro          | confluent_avro          |   Confluent    |     Without      |      Y      |       SR        | struct         |     Always     | Any AVRO, schema loaded from SR upon consume        |
| avro          | apicurio_avro           |    Apicurio    |       With       |      Y      | File or dynamic | struct         | If configured  | Local file or Python-code-generated schema          |
| avro          | confluent_avro          |   Confluent    |       With       |      Y      | File or dynamic | struct         |     Always     | Local file or Python-code-generated schema          |
| csv           | csv                     |      ksml      |     Without      |      N      |       N/A       | struct         |     Always     | CSV Schemaless                                      |
| csv           | csv                     |      ksml      |       With       |      N      | File or dynamic | struct         |     Always     | Local file or Python-code-generated schema          |
| json          | json                    |      ksml      |     Without      |      N      |       N/A       | list or struct |     Always     | JSON Schemaless                                     |
| json          | json                    |      ksml      |       With       |      N      | File or dynamic | list or struct |     Always     | Local file or Python-code-generated schema          |
| jsonschema    | apicurio_jsonschema     |    Apicurio    |     Without      |      Y      |       SR        | struct         | If configured  | Any JSON Schema, schema loaded from SR upon consume |
| jsonschema    | confluent_jsonschema    |   Confluent    |     Without      |      Y      |       SR        | struct         | If configured  | Any JSON Schema, schema loaded from SR upon consume |
| jsonschema    | apicurio_jsonschema     |    Apicurio    |       With       |      Y      | File or dynamic | struct         | If configured  | Local file or Python-code-generated schema          |
| jsonschema    | confluent_jsonschema    |   Confluent    |       With       |      Y      | File or dynamic | struct         | If configured  | Local file or Python-code-generated schema          |
| protobuf      | apicurio_protobuf       |    Apicurio    |     Without      |      Y      |       SR        | struct         | If configured  | Any Protobuf, schema loaded from SR upon consume    |
| protobuf      | confluent_protobuf      |   Confluent    |     Without      |      Y      |       SR        | struct         | If configured  | Any Protobuf, schema loaded from SR upon consume    |
| protobuf      | apicurio_protobuf       |    Apicurio    |       With       |      Y      | File or dynamic | struct         | If configured  | Local file or Python-code-generated schema          |
| protobuf      | confluent_protobuf      |   Confluent    |       With       |      Y      | File or dynamic | struct         | If configured  | Local file or Python-code-generated schema          |
| soap          | soap                    |      ksml      |     Without      |      N      |       N/A       | struct         |     Always     | SOAP Schemaless                                     |
| soap          | soap                    |      ksml      |       With       |      N      | File or dynamic | struct         |     Always     | SOAP with Schema                                    |
| xml           | xml                     |      ksml      |     Without      |      N      |       N/A       | struct         |     Always     | XML Schemaless                                      |
| xml           | xml                     |      ksml      |       With       |      N      | File or dynamic | struct         |     Always     | XML with Schema                                     |

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

JSON is a text-based, human-readable format for data transfer.

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

### AVRO Schemas

AVRO schemas are typically defined in `.avsc` files and registered with a schema registry. KSML can work with AVRO
schemas in two ways:

1. **Using a schema registry**: KSML can retrieve schemas from a schema registry at runtime.

   ```yaml
   streams:
     sensor_readings:
       topic: sensor-data
       keyType: string
       valueType: avro:SensorReading
   ```

2. **Using local schema files**: KSML can load schemas from local files.

   ```yaml
   streams:
     sensor_readings:
       topic: sensor-data
       keyType: string
       valueType: avro:SensorReading
   ```

### XML Schemas

XML schemas are defined in `.xsd` files and can be loaded by KSML:

```yaml
streams:
  customer_data:
    topic: customer-data
    keyType: string
    valueType: xml:Customer
```

### JSON Schemas

While JSON is often used without a schema, KSML also supports JSON Schema:

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: json:UserProfile
```
