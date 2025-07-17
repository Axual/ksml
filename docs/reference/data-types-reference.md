# KSML Data Types Reference

KSML supports a wide range of data types for both keys and values in your streams. Understanding these types is
essential for properly defining your streams and functions that process your data.

## Data Types in KSML

## Primitive Types

KSML supports the following primitive types:

| Type      | Description                     | Example               | Java Equivalent |
|-----------|---------------------------------|-----------------------|-----------------|
| `boolean` | True or false values            | `true`, `false`       | `Boolean`       |
| `byte`    | 8-bit integer                   | `42`                  | `Byte`          |
| `short`   | 16-bit integer                  | `1000`                | `Short`         |
| `int`     | 32-bit integer                  | `1000000`             | `Integer`       |
| `long`    | 64-bit integer                  | `9223372036854775807` | `Long`          |
| `float`   | Single-precision floating point | `3.14`                | `Float`         |
| `double`  | Double-precision floating point | `3.141592653589793`   | `Double`        |
| `string`  | Text string                     | `"Hello, World!"`     | `String`        |
| `bytes`   | Array of bytes                  | Binary data           | `byte[]`        |
| `null`    | Null value                      | `null`                | `null`          |

## Complex Types

KSML also supports several complex types that can contain multiple values:

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

In Python code, an enum value is always represented as a string:

```yaml
functions:
  update_status:
    type: valueTransformer
    code: |
      if value.get("shipped"):
        return "SHIPPED"
      elif value.get("processing"):
        return "PROCESSING"
    expression: "PENDING"
    resultType: string
```

Note that the `expression` here is mandatory, since the `valueTransformer` function type determines that the function
is required to return a value.

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

  orders_stream:
    topic: orders
    keyType: string
    valueType: "[struct]"
```

In Python code, a list is represented as a Python list:

```yaml
functions:
  extract_tags:
    type: keyValueToValueListTransformer
    expression: value.get("tags", [])
    resultType: "[string]"
```

### Struct

A struct is a key-value map where all keys are strings. This is the most common complex type and is used for JSON
objects, AVRO records, etc.

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
    type: valueTransformer
    expression: |
      return {
        "id": value.get("user_id"),
        "name": value.get("first_name") + " " + value.get("last_name"),
        "email": value.get("email"),
        "age": value.get("age")
      }
```

### Tuple

A tuple combines multiple elements of different types into a single value. Note that the elements in a tuple have no
name, only a data type.

#### Syntax

```yaml
valueType: "(<type1>, <type2>, ...)"
```

Where `<type1>`, `<type2>`, etc. can be any valid KSML type.

Note that tuples can be used as keyType or valueType on Kafka topics, since KSML comes with its own schema format for
tuples. These schemas are generated dynamically based on the tuple's element types. For example, a tuple defined as
`(string, int, struct)` will lead to the following schema:

```yaml
namespace: io.axual.ksml.data
name: TupleOfStringAndIntAndStruct
doc: "Tuple of 3 fields"
fields:
  - name: elem1
    type: string
  - name: elem2
    type: int
  - name: elem3
    type: struct
```

Since tuple schemas are dynamic, when you want to use a tuple as `keyType` or `valueType` on a topic, be aware that the
tuple is serialized as a struct. So the schema will be registered by the serializer in the corresponding schema
registry, or expected to be registered on the topic by the deserializer. If the latter is not the case, but you
still want to maintain strong typing, then save the generated schema in your desired format (AVRO, JSON Schema, ...) to
a local file from which KSML can read it. Then you will be able to (re)use it in other pipelines as
`avro:TupleOfStringAndIntAndStruct` for AVRO, `json:TupleOfStringAndIntAndStruct` for JSON, etc.

#### Example

```yaml
# Example: Tuple of string and SensorData
streams:
  sensor_stream:
    topic: tags
    keyType: string
    valueType: "(string, avro:SensorData)"
```

In Python code, a tuple is represented as a Python tuple:

```yaml
functions:
  create_user_age_pair:
    type: keyValueTransformer
    expression: |
      (value.get("name"), value.get("age"))
    resultType: "(string, int)"
```

### Union

A union type can be one of several possible types.

#### Syntax

```yaml
valueType: "union(<type1>, <type2>, ...)"
```

Where `<type1>`, `<type2>`, etc. can be any valid KSML type.

#### Example

Unions are serializable to Kafka topics. KSML comes with an internal UnionSerde. Upon serialization, it will use the
serde that belongs to the value stored in the union. When deserializing it will try all serdes associated with the
member types until one is able to decode the message without throwing exceptions.

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
    type: valueMapper
    code: |
      message = value.get("message")
    expression: message if message else None
```

### Windowed

Some operations group messages on an input stream together in user-defined windows. If the input stream's `keyType` is
`string`, then the resulting stream will have a `keyType` of `windowed(string)`. Sometimes you need to specify this
type explicitly in your KSML definition, which is why it will be recognized as its own type.

Whenever a `windowed` type is exposed to a Kafka topic, or to Python code, KSML converts the `Windowed` object from
Kafka Streams into a struct with five fields. This is dynamically done, and the output depends on the type of key.
For example, a `windowed(string)` type is converted into the following struct schema:

```yaml
namespace: io.axual.ksml.data
name: WindowedString
doc: Windowed String
fields:
  - name: start
    type: long
    doc: "Start timestamp in milliseconds"
  - name: end
    type: long
    doc: "End timestamp in milliseconds"
  - name: startTime
    type: string
    doc: "Start time in UTC"
  - name: endTime
    type: string
    doc: "End time in UTC"
  - name: key
    type: string # This is determined by the windowed type within brackets
    doc: "Window key"
```

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
    type: keyValueTransformer
    expression: |
      (None, {
        "key": key.key(),
        "start": key.start(),
        "end": key.end(),
        "count": value
      })
    resultType: "(null, struct)"
```

## The Any Type

The special type `?` or `any` can be used when the exact type is unknown or variable. Code that deals with this type
should always perform proper type checking.

#### Syntax

```yaml
# Either of these can be used:
# resultType: "?"
# resultType: "any"
```

#### Example

It is not possible to use the `any` type on a Kafka topic, as KSML would not be able to tie the type to the right
serde. Therefore, on Kafka topics you always need to specify a precise type.

In Python code, you should check the type before using values of the `any` type:

```yaml
functions:
  process_any:
    type: valueTransformer
    code: |
      if isinstance(value, dict):
        return {"type": "object", "value": value}
      elif isinstance(value, list):
        return {"type": "array", "value": value}
      elif isinstance(value, str):
        return {"type": "string", "value": value}
      elif isinstance(value, (int, float)):
        return {"type": "number", "value": value}
    expression: |
      {"type": "unknown", "value": str(value)}
    resultType: struct
```

## Type Conversion

KSML automatically performs type conversion wherever required and possible. This holds for numbers (integer to long,
etc.) but also for enums, lists, structs, tuples, and unions. Given a value and the desired data type, KSML will try
the best possible conversion to allow developers to focus on their logic instead of data type compatibility.

Below is an example of a function that returns a string
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
- [Operations Reference](operation-reference.md)
- [Functions Reference](function-reference.md)
- [Configuration Reference](configuration-reference.md)
