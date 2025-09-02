# Data Types and Notations Reference

KSML supports a wide range of data types and formats for both keys and values in your streams. This comprehensive reference covers all data types, notation formats, and type conversion capabilities available in KSML.

## Data Types in KSML

### Primitive Types

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

### Complex Types

KSML also supports several complex types that can contain multiple values:

#### Enum

An enumeration defines a set of allowed values.

**Syntax:**
```yaml
valueType: "enum(<value1>, <value2>, ...)"
```

**Example:**
```yaml
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

??? info "Producer - Enum example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/enum-producer.yaml"
    %}
    ```

??? info "Processor - Enum example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/enum-processor.yaml"
    %}
    ```

#### List

A list contains multiple elements of the same type.

**Syntax:**
```yaml
valueType: "[<element_type>]"
```

**Example:**
```yaml
streams:
  tags_stream:
    topic: tags
    keyType: string
    valueType: "[string]"
```

In Python code, a list is represented as a Python list:
```yaml
functions:
  extract_tags:
    type: keyValueToValueListTransformer
    expression: value.get("tags", [])
    resultType: "[string]"
```

**See it in action**:

- [List example](../reference/function-reference.md#keyvaluetovaluelisttransformer) - predicate functions for data filtering



#### Struct

A struct is a key-value map where all keys are strings. This is the most common complex type and is used for JSON objects, AVRO records, etc.

**Syntax:**
```yaml
valueType: struct
```

**Example:**
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

??? info "Producer - Struct example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/struct-producer.yaml"
    %}
    ```

??? info "Processor - Struct example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/struct-processor.yaml"
    %}
    ```

#### Tuple

A tuple combines multiple elements of different types into a single value.

**Syntax:**
```yaml
valueType: "(<type1>, <type2>, ...)"
```

**Example:**
```yaml
streams:
  sensor_stream:
    topic: sensor-data
    keyType: string
    valueType: "(string, avro:SensorData)"
```

In Python code, a tuple is represented as a Python tuple:
```yaml
functions:
  create_user_age_pair:
    type: keyValueTransformer
    expression: (value.get("name"), value.get("age"))
    resultType: "(string, int)"
```

**See it in action**:

- [Tuple example](../reference/function-reference.md#foreignkeyextractor)

#### Union

A union type can be one of several possible types.

**Syntax:**
```yaml
valueType: "union(<type1>, <type2>, ...)"
```

**Example:**

Union types are used in two main places in KSML:

**1. In stream definitions** - to specify that a stream can contain multiple types:
```yaml
streams:
  optional_messages:
    topic: optional-messages
    keyType: string
    valueType: "union(null, json)"  # This stream accepts either null OR a JSON object
```

**2. In function return types** - to specify that a function can return multiple types:
```yaml
functions:
  generate_optional:
    type: generator
    code: |
      # Can return either null or a message
      if random.random() > 0.5:
        return ("key1", {"data": "value"})
      else:
        return ("key1", None)
    resultType: "(string, union(null, json))"  # Returns a tuple with union type
```

**What union types mean:**

- `union(null, json)` means the value can be either `null` OR a JSON object
- When processing union types, your code must check which type was received and handle each case

**Complete example showing both usages:**

??? info "Producer - Union example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/union-producer.yaml"
    %}
    ```

??? info "Processor - Union example (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/union-processor.yaml"
    %}
    ```

#### Windowed

Windowing operations in Kafka Streams group messages together in time-based windows. KSML provides the `windowed(<base_type>)` syntax to work with these windowed keys.

**Syntax:**
```yaml
# Without notation - requires manual transformation for Kafka output
keyType: "windowed(<base_type>)"

# With notation - automatically serializes to the specified format
keyType: "<notation>:windowed(<base_type>)"  # e.g., json:windowed(string), avro:windowed(string)
```

**Understanding Windowed Keys:**

After windowing operations (like `windowByTime`), Kafka Streams internally creates windowed keys that contain:

- The original key value
- Window start timestamp (milliseconds)
- Window end timestamp (milliseconds)  
- Human-readable start/end times

**Two Approaches for Handling Windowed Keys:**

**1. Without Notation (Manual Transformation Required):**

When using plain `windowed(string)`, the windowed keys cannot be directly serialized to Kafka topics. You must manually transform them to a regular type:

```yaml
--8<-- "definitions/reference/data-types/windowed-processor.yaml:51:52"
```

**2. With Notation Prefix (Automatic Serialization):**

Using a notation prefix like `json:windowed(string)` or `avro:windowed(string)` enables automatic serialization of the windowed key structure:

```yaml
--8<-- "definitions/reference/data-types/windowed-processor-notation.yaml:59:60"
```

The notation automatically serializes the windowed key as a structured object with fields: `start`, `end`, `startTime`, `endTime`, and `key`.

**Complete Examples:**

??? info "Producer - Generates events for windowing (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/data-types/windowed-producer.yaml"
    %}
    ```

??? info "Processor - Manual transformation approach (click to expand)"

    This example shows how to manually transform windowed keys to regular strings when not using notation:

    ```yaml
    {%
      include "../definitions/reference/data-types/windowed-processor.yaml"
    %}
    ```

??? info "Processor - Automatic serialization with notation (click to expand)"

    This example shows the simpler approach using notation for automatic serialization:

    ```yaml
    {%
      include "../definitions/reference/data-types/windowed-processor-notation.yaml"
    %}
    ```

**When to Use Each Approach:**

- **Use notation prefix** (`json:windowed(string)`) when you want to:
     - Write windowed keys directly to Kafka topics
     - Preserve the complete window structure in a standard format
     - Avoid manual transformation code

- **Use plain windowed type** (`windowed(string)`) when you:
     - Only need windowed keys for internal processing
     - Want custom key formatting for output
     - Need to extract specific window information

**Key Takeaway:**

Windowed types enable time-based analytics like counting events per time window, calculating moving averages, or detecting patterns over time intervals. The notation prefix approach simplifies working with windowed data by handling serialization automatically.

### The Any Type

The special type `?` or `any` can be used when the exact type is unknown or variable. This is particularly useful for functions that need to handle different data structures generically.

**Syntax:**
```yaml
parameterType: "?"
# or
parameterType: "any"
```

**Key Use Cases:**

- Processing streams with variable JSON structures
- Generic data transformation functions
- Handling mixed data types within a single function

**Important:** The `any` type can only be used in function signatures for internal processing. Kafka streams must still use concrete types (like `json`) for serialization.

??? info "Producer - Any type data generation (click to expand)"

    This example generates mixed JSON data with different structures, demonstrating variable data types that can be processed using `any` type functions.

    ```yaml
    --8<-- "definitions/reference/data-types/any-producer.yaml"
    ```

??? info "Processor - Any type processing (click to expand)"

    This example shows how to use `parameterType: "any"` to process variable data structures. The function accepts any input type and handles it generically.

    ```yaml
    --8<-- "definitions/reference/data-types/any-processor.yaml:17:64"
    ```

**Key Takeaway:**

The `any` type enables flexible data processing for functions that need to handle variable or unknown data structures. While streams require concrete types for serialization, functions can use `any` to process diverse data generically.

## Notation Formats

KSML uses _notations_ to allow reading/writing different message formats to Kafka topics. Notations are specified as a prefix to the schema name.

### Examples
**See a working example for every data format in this tutorial**:

- [Data Format Examples](../tutorials/beginner/data-formats.md)

### Format Selection Guide

The choice of notation depends on your specific requirements:

| If you need...                              | Consider using... |
|---------------------------------------------|-------------------|
| Schema evolution and backward compatibility | AVRO or Protobuf  |
| Human-readable data for debugging           | JSON              |
| Integration with legacy systems             | XML or SOAP       |
| Simple tabular data                         | CSV               |
| Compact binary format                       | AVRO or Protobuf  |
| Raw binary data handling                    | Binary            |

### AVRO

AVRO is a binary format that supports schema evolution.

**Syntax:**
```yaml
valueType: "avro:<schema_name>"
# or for schema registry lookup
valueType: "avro"
```

**Example:**
```yaml
streams:
  sensor_readings:
    topic: sensor-data
    keyType: string
    valueType: avro:SensorData
```

### JSON

JSON is a text-based, human-readable format for data transfer.

**Syntax:**
```yaml
# For schemaless JSON:
valueType: "json"
# For JSON with a schema:
valueType: "json:<schema_name>"
```

**Example:**
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

Python functions can return JSON by returning a dictionary:
```yaml
functions:
  merge_key_value_data:
    type: valueTransformer
    expression: { 'key': key, 'value': value }
    resultType: json
```

### JSON Schema

JSON Schema adds vendor-specific schema support to JSON serialization.

**Syntax:**
```yaml
# For schema registry lookup:
valueType: "jsonschema"
# For JSON with a schema:
valueType: "jsonschema:<schema_name>"
```

**Example:**
```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "jsonschema:UserProfile"
```

### CSV

CSV (Comma-Separated Values) is a simple tabular data format.

**Syntax:**
```yaml
# For schemaless CSV:
valueType: "csv"
# For CSV with a schema:
valueType: "csv:<schema_name>"
```

**Example:**
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

### XML

XML (Extensible Markup Language) is used for complex hierarchical data.

**Syntax:**
```yaml
# For schemaless XML:
valueType: "xml"
# For XML with a schema:
valueType: "xml:<schema_name>"
```

**Example:**
```yaml
streams:
  customer_data:
    topic: customer-data
    keyType: string
    valueType: "xml:CustomerData"
```

### Protobuf

Protobuf is a popular encoding format developed by Google.

**Syntax:**
```yaml
# For schema registry lookup:
valueType: "protobuf"
# For Protobuf with a schema:
valueType: "protobuf:<schema_name>"
```

**Example:**
```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: "protobuf:UserProfile"
```

### Binary

Binary data represents raw bytes for custom protocols.

**Syntax:**
```yaml
valueType: "binary"
```

**Example:**
```yaml
streams:
  binary_data:
    topic: binary-messages
    keyType: string
    valueType: "binary"
```

### SOAP

SOAP (Simple Object Access Protocol) is an XML-based messaging protocol.

**Syntax:**
```yaml
valueType: "soap"
```

**Example:**
```yaml
streams:
  service_requests:
    topic: service-requests
    keyType: string
    valueType: "soap"
```

## Schema Management

When working with structured data, it's important to manage your schemas effectively.

### Examples
**See a working example for every type of schema in this tutorial**:

- [Schema Examples](../tutorials/beginner/data-formats.md)

### Local Files vs. Schema Registry

**Local Schema Files:**
When a schema is specified, KSML loads the schema from a local file from the `schemaDirectory`. The notation determines the filename extension:

- AVRO schemas: `.avsc` extension
- XML schemas: `.xsd` extension  
- CSV schemas: `.csv` extension
- JSON schemas: `.json` extension

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: "avro:SensorReading"  # Looks for SensorReading.avsc
```

**Schema Registry Lookup:**
When no schema is specified, KSML assumes the schema is loadable from Schema Registry:

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: "avro"  # Schema fetched from registry
```

## Type Conversion

KSML handles type conversion differently depending on the context:

| Context | Conversion Type | When to Use |
|---------|----------------|-------------|
| **Functions** | Automatic | When `resultType` differs from returned value |
| **Streams** | Explicit | When input/output stream formats differ |

### Function Type Conversion (Automatic)

Functions automatically convert return values to match their declared `resultType` when possible:

**Successful Conversions:**

- Any type → string: Always works via automatic `.toString()` conversion
- String → numeric types (int, long, float, double): Works only if string contains a valid numeric value (e.g., "123" → int)
- Numeric conversions: Work between compatible numeric types (int ↔ long, float ↔ double)
- Complex types: Dict → JSON, lists/structs/tuples with matching schemas

**Failed Conversions:**

- Invalid string → numeric: Throws exception and stops processing (e.g., "not_a_number" → int fails)
- Incompatible complex types: Mismatched schemas or structures

**Example:**
```yaml
functions:
  string_to_int:
    type: valueTransformer
    code: |
      result = "123"        # Valid numeric string
    expression: result
    resultType: int         # ← Succeeds: converts "123" → 123

  invalid_conversion:
    type: valueTransformer
    code: |
      result = "not_a_number"  # Invalid numeric string
    expression: result
    resultType: int         # ← Fails: throws conversion exception
```

??? info "Working example - Automatic type conversion in functions"

    Producer:
    ```yaml
    --8<-- "definitions/reference/data-types/auto-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "definitions/reference/data-types/auto-conversion-processor.yaml"
    ```

### Stream Format Conversion (Explicit)

Streams require explicit `convertValue` operations when formats differ:

```yaml
pipelines:
  example_pipeline:
    from: json_input      # JSON format
    via:
      - type: convertValue
        into: string      # Must explicitly convert
    to: string_output     # String format
```

Without `convertValue`, KSML will fail with a type mismatch error.

??? info "Working example - Explicit stream conversion"

    Producer:
    ```yaml
    --8<-- "definitions/reference/data-types/explicit-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "definitions/reference/data-types/explicit-conversion-processor.yaml"
    ```

### Chaining Multiple Conversions

Chain `convertValue` operations for complex transformations:

```yaml
pipelines:
  multi_conversion:
    from: json_stream
    via:
      - type: convertValue
        into: string      # JSON → String
      - type: convertValue  
        into: json        # String → JSON
    to: json_output
```

??? info "Working example - Chained conversions"

    Producer:
    ```yaml
    --8<-- "definitions/reference/data-types/multi-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "definitions/reference/data-types/multi-conversion-processor.yaml"
    ```

**Key Takeaway:** Functions convert automatically, streams need explicit conversion.

## Working with Multiple Formats in a Single Pipeline

Process different data formats within one KSML definition using separate pipelines.

This producer generates both JSON config data and Avro sensor data:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../definitions/beginner-tutorial/different-data-formats/producer-multiple-formats.yaml"
    %}
    ```

This processor shows two pipelines handling different formats (Avro and JSON) and combining results:

??? info "Processor definition for working with multiple formats in a single pipeline (click to expand)"

    ```yaml
    {%
      include "../definitions/beginner-tutorial/different-data-formats/processor-multiple-formats.yaml"
    %}
    ```

## Type Definition Quoting Rules

When defining types in KSML (for `keyType`, `valueType`, `resultType`, or any other type field), quotes around type expressions follow specific rules based on YAML parsing requirements:

### Quotes MUST be used for:

1. **Special type functions (these will fail without quotes):**
   ```yaml
   # enum types - MUST have quotes
   valueType: "enum(PENDING, PROCESSING, SHIPPED)"
   
   # windowed types without notation prefix - MUST have quotes  
   keyType: "windowed(string)"
   
   # union types - MUST have quotes
   valueType: "union(null, string)"
   
   # List of tuples - MUST have quotes
   resultType: "[(string, json)]"
   ```

2. **Complex type expressions with special characters:**
   ```yaml
   # Types with spaces or special chars - MUST have quotes
   resultType: "(string, avro:SensorData)"
   valueType: "list<string>"
   ```

### Quotes are OPTIONAL for:

1. **Simple basic types (work with or without quotes):**
   ```yaml
   valueType: string       # Works
   valueType: "string"     # Also works
   keyType: json          # Works
   keyType: "json"        # Also works
   valueType: struct      # Works
   resultType: bytes      # Works
   ```

2. **Simple tuples (both forms are valid):**
   ```yaml
   resultType: (string, json)      # Works without quotes
   resultType: "(string, json)"    # Also works with quotes
   ```

3. **Notation:schema combinations (notation prefix makes quotes unnecessary):**
   ```yaml
   valueType: avro:SensorData         # Works without quotes
   keyType: json:windowed(string)     # Notation prefix handles complexity
   valueType: jsonschema:UserProfile  # Works without quotes
   valueType: csv:Temperature         # Works without quotes
   ```

### Why These Rules Exist:

- **YAML parsing**: Without quotes, YAML interprets `enum(A,B,C)` as a function call, causing parsing errors. The quotes tell YAML to treat it as a string literal.
- **KSML parsing**: The UserTypeParser in KSML specifically looks for patterns like `enum(`, `windowed(`, `union(` at the start of strings to determine the type.
- **Notation prefix**: When using `json:windowed(string)`, the colon acts as a delimiter that YAML handles correctly without quotes.

### Examples from Real KSML Code:

```yaml
# Stream definitions
streams:
  sensor_data:
    topic: sensor-readings
    keyType: string                          # Simple type - no quotes needed
    valueType: avro:SensorData               # Notation:schema - no quotes needed
    
  windowed_data:
    topic: windowed-counts
    keyType: "windowed(string)"              # Complex type - quotes required
    valueType: long                          # Simple type - no quotes needed

# Function definitions
functions:
  my_generator:
    type: generator
    resultType: (string, json)               # Simple tuple - quotes optional
    
  my_transformer:
    type: valueTransformer  
    resultType: "enum(SUCCESS, FAILURE)"     # Enum type - quotes required
```

### Quick Reference:

| Type Pattern | Quotes Required? | Example |
|-------------|-----------------|----------|
| `enum(...)` | **Yes** | `"enum(ACTIVE, INACTIVE)"` |
| `windowed(...)` | **Yes** (without notation) | `"windowed(string)"` |
| `union(...)` | **Yes** | `"union(null, string)"` |
| `[(...)]` | **Yes** | `"[(string, json)]"` |
| `(type1, type2)` | **Optional** | `(string, json)` or `"(string, json)"` |
| `notation:type` | **Optional** | `avro:SensorData` |
| `notation:windowed(...)` | **Optional** | `json:windowed(string)` |
| Simple types | **Optional** | `string`, `json`, `struct` |
