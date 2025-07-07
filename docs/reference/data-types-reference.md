# KSML Data Types Reference

This document provides a comprehensive reference for all data types available in KSML. Each data type is described with its syntax, behavior, and examples.

## Primitive Types

KSML supports the following primitive types:

| Type | Description | Example | Java Equivalent |
|------|-------------|---------|----------------|
| `boolean` | True or false values | `true`, `false` | `Boolean` |
| `byte` | 8-bit integer | `42` | `Byte` |
| `short` | 16-bit integer | `1000` | `Short` |
| `int` | 32-bit integer | `1000000` | `Integer` |
| `long` | 64-bit integer | `9223372036854775807` | `Long` |
| `float` | Single-precision floating point | `3.14` | `Float` |
| `double` | Double-precision floating point | `3.141592653589793` | `Double` |
| `string` | Text string | `"Hello, World!"` | `String` |
| `bytes` | Array of bytes | Binary data | `byte[]` |
| `null` | Null value | `null` | `null` |

## Complex Types

KSML also supports several complex types that can contain multiple values:

### Struct

A struct is a key-value map where all keys are strings. This is the most common complex type and is used for JSON objects, AVRO records, etc.

#### Syntax

```yaml
valueType: struct
```

#### Example

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: struct
```

In Python code, a struct is represented as a dictionary:

```yaml
functions:
  create_user:
    type: mapper
    code: |
      return {
        "id": value.get("user_id"),
        "name": value.get("first_name") + " " + value.get("last_name"),
        "email": value.get("email"),
        "age": value.get("age")
      }
```

### List

A list contains multiple elements of the same type.

#### Syntax

```yaml
valueType: "[<element_type>]"
```

Where `<element_type>` can be any valid KSML type.

#### Examples

```yaml
# Example 1: List of strings
streams:
  tags_stream:
    topic: tags
    keyType: string
    valueType: "[string]"

# Example 2: List of structs (in a different KSML file)
# streams:
#   orders_stream:
#     topic: orders
#     keyType: string
#     valueType: "[struct]"
```

In Python code, a list is represented as a Python list:

```yaml
functions:
  extract_tags:
    type: mapper
    code: |
      return value.get("tags", [])
```

### Tuple

A tuple combines multiple elements of different types into a single value.

#### Syntax

```yaml
valueType: "(<type1>, <type2>, ...)"
```

Where `<type1>`, `<type2>`, etc. can be any valid KSML type.

#### Example

```yaml
# Tuple with a string and an integer
streams:
  user_age_stream:
    topic: user-ages
    keyType: string
    valueType: "(string, int)"
```

In Python code, a tuple is represented as a Python tuple:

```yaml
functions:
  create_user_age_pair:
    type: mapper
    code: |
      return (value.get("name"), value.get("age"))
```

### Enum

An enumeration defines a set of allowed values.

#### Syntax

```yaml
valueType: "enum(<value1>, <value2>, ...)"
```

Where `<value1>`, `<value2>`, etc. are the allowed values.

#### Example

```yaml
# Enum type for status values
streams:
  order_status_stream:
    topic: order-statuses
    keyType: string
    valueType: "enum(PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED)"
```

In Python code, an enum value is represented as a string:

```yaml
functions:
  update_status:
    type: mapper
    code: |
      if value.get("shipped"):
        return "SHIPPED"
      elif value.get("processing"):
        return "PROCESSING"
      else:
        return "PENDING"
```

### Union

A union type can be one of several possible types.

#### Syntax

```yaml
valueType: "union(<type1>, <type2>, ...)"
```

Where `<type1>`, `<type2>`, etc. can be any valid KSML type.

#### Example

```yaml
# Union type that can be either null or a string
streams:
  optional_message_stream:
    topic: optional-messages
    keyType: string
    valueType: "union(null, string)"
```

In Python code, a union is represented as a value of one of the specified types:

```yaml
functions:
  get_message:
    type: mapper
    code: |
      message = value.get("message")
      if message:
        return message
      else:
        return None
```

### Windowed

Some operations in Kafka Streams create windowed keys, which group records by time windows.

#### Syntax

```yaml
keyType: "windowed(<base_type>)"
```

Where `<base_type>` is the type of the key within the window.

#### Example

```yaml
# Windowed key type
streams:
  windowed_counts:
    topic: windowed-counts
    keyType: "windowed(string)"
    valueType: long
```

In Python code, a windowed key provides access to both the key and the window:

```yaml
functions:
  format_windowed_result:
    type: mapper
    code: |
      return {
        "key": window.key(),
        "start": window.start(),
        "end": window.end(),
        "count": value
      }
```

## The Any Type

The special type `?` or `any` can be used when the exact type is unknown or variable. Code that deals with this type should always perform proper type checking.

#### Syntax

```yaml
# Either of these can be used:
# valueType: "?"
# valueType: "any"
```

#### Example

```yaml
# Any type
streams:
  generic_stream:
    topic: generic-data
    keyType: string
    valueType: "?"
```

In Python code, you should check the type before using values of the any type:

```yaml
functions:
  process_any:
    type: mapper
    code: |
      if isinstance(value, dict):
        return {"type": "object", "value": value}
      elif isinstance(value, list):
        return {"type": "array", "value": value}
      elif isinstance(value, str):
        return {"type": "string", "value": value}
      elif isinstance(value, (int, float)):
        return {"type": "number", "value": value}
      else:
        return {"type": "unknown", "value": str(value)}
```

## Data Formats and Notations

In KSML, data can be serialized and deserialized in various formats using notations. A notation specifies how data is read from or written to Kafka topics.

### AVRO Notation

AVRO is a binary format with schema evolution support.

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
    valueType: "avro:SensorReading"
    schemaRegistry: "http://schema-registry:8081"
```

### JSON Notation

JSON is a text-based, human-readable format.

#### Syntax

```yaml
# For schemaless JSON:
valueType: "json"

# For JSON with a schema:
# valueType: "json:<schema_name>"
```

#### Example

```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "json"
```

### CSV Notation

CSV (Comma-Separated Values) is a simple tabular data format.

#### Syntax

```yaml
valueType: "csv"
```

#### Example

```yaml
streams:
  sales_data:
    topic: sales-data
    keyType: string
    valueType: "csv"
    csvConfig:
      headers: ["date", "product", "quantity", "price"]
      separator: ","
```

### XML Notation

XML (Extensible Markup Language) is used for complex hierarchical data.

#### Syntax

```yaml
# For schemaless XML:
valueType: "xml"

# For XML with a schema:
# valueType: "xml:<schema_name>"
```

#### Example

```yaml
streams:
  customer_data:
    topic: customer-data
    keyType: string
    valueType: "xml:CustomerData"
    schemaPath: "schemas/CustomerData.xsd"
```

### SOAP Notation

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
    soapConfig:
      wsdlPath: "schemas/service.wsdl"
```

## Schema Management

When working with structured data, it's important to manage your schemas effectively.

### Schema Registry

KSML can retrieve schemas from a schema registry at runtime:

```yaml
streams:
  sensor_readings:
    topic: sensor-data
    keyType: string
    valueType: "avro:SensorReading"
    schemaRegistry: "http://schema-registry:8081"
```

### Local Schema Files

KSML can also load schemas from local files:

```yaml
streams:
  sensor_readings:
    topic: sensor-data
    keyType: string
    valueType: "avro:SensorReading"
    schemaPath: "schemas/SensorReading.avsc"
```

## Type Conversion

KSML automatically handles type conversion in many cases, but you can also explicitly convert types in your Python code:

```yaml
functions:
  convert_types:
    type: mapper
    code: |
      # String to number
      age_str = value.get("age_str")
      age = int(age_str) if age_str else 0

      # Number to string
      id_num = value.get("id")
      id_str = str(id_num)

      # String to boolean
      active_str = value.get("active")
      active = active_str.lower() == "true"

      return {
        "age": age,
        "id": id_str,
        "active": active
      }
```

## Best Practices

1. **Be specific about your types**: Avoid using `any` when possible
2. **Use complex types to represent structured data**: Use structs, lists, etc. to represent structured data
3. **Consider schema evolution**: Use AVRO with a schema registry for production systems
4. **Choose the right notation for your use case**: Consider compatibility with upstream and downstream systems
5. **Validate data**: Check that data conforms to expected types and formats
6. **Handle missing or null values**: Always handle the case where a value might be null
7. **Document your types**: Add comments to explain complex type structures

## Related Topics

- [KSML Language Reference](language-reference.md)
- [Operations Reference](operations-reference.md)
- [Functions Reference](functions-reference.md)
- [Configuration Reference](configuration-reference.md)
