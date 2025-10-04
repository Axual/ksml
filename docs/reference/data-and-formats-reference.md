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
valueType: enum(<value1>, <value2>, ...)   # Quotes optional
```

**Example:**
```yaml
streams:
  order_status_stream:
    topic: order-statuses
    keyType: string
    valueType: enum(PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED)  # Works without quotes
    # valueType: "enum(PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED)"  # Also works with quotes
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
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/enum-producer.yaml" %}
    ```

??? info "Processor - Enum example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/enum-processor.yaml" %}
    ```

#### List

A list contains multiple elements of the same type.

**Syntax:**
```yaml
# Function notation (recommended - avoids YAML validation warnings)
valueType: list(<element_type>) # valueType: [<element_type>] also works
```

**Example:**
```yaml
streams:
  tags_stream:
    topic: tags
    keyType: string
    valueType: list(string)     # Function notation, valueType: [string] is also valid
  
  categories_stream:
    topic: categories
    keyType: string
    valueType: list(string)      # Alternative notation (no quotes needed)
```

In Python code, a list is represented as a Python list:
```yaml
functions:
  extract_tags:
    type: keyValueToValueListTransformer
    expression: value.get("tags", [])
    resultType: list(string)    # Function notation, resultType: [string] is also valid
  
  extract_categories:
    type: keyValueToValueListTransformer
    expression: value.get("categories", [])
    resultType: list(string)    # Alternative notation (no quotes needed)
```

**See it in action**:

- [List example](../reference/function-reference.md#keyvaluetovaluelisttransformer) for predicate functions for data filtering

#### Example

```yaml
--8<-- "docs-examples/reference/data-types/list-tuple-simple-processor.yaml:13:23"
```

This example demonstrates using `list(int)` syntax in function result types to avoid YAML validation warnings:

??? info "Producer - `list()` syntax example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/list-tuple-simple-producer.yaml" %}
    ```

??? info "Processor - `list()` syntax example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/list-tuple-simple-processor.yaml" %}
    ```

**What this example does:**

- **Producer** uses `resultType: tuple(string, json)` instead of `"(string, json)"` to avoid quotes
- **Processor** uses `resultType: list(int)` instead of `"[int]"` to avoid YAML validation warnings
- **Functionality** remains identical - the new syntax is purely for YAML compatibility

#### Map

A map contains key-value pairs where keys are always strings and values are of a specified type.

**Syntax:**
```yaml
valueType: map(<value_type>)   # Quotes optional
```

**Example:**
```yaml
streams:
  user_preferences:
    topic: user-preferences
    keyType: string
    valueType: map(string)  # Map with string keys and string values (quotes optional)

  scores:
    topic: scores
    keyType: string
    valueType: map(int)     # Map with string keys and integer values, "map(int)" also valid
```

In Python code, a map is represented as a Python dictionary:
```yaml
functions:
  create_preferences:
    type: valueTransformer
    code: |
      return {
        "theme": value.get("selected_theme", "default"),
        "language": value.get("user_language", "en"),
        "notifications": value.get("notify_enabled", "true")
      }
    expression: result
    resultType: map(string) # "map(string)" also valid

  calculate_scores:
    type: valueTransformer
    code: |
      return {
        "math": 85,
        "science": 92,
        "english": 78
      }
    expression: result
    resultType: map(int) # "map(int)" also valid 
```

**Key characteristics:**

- Keys are always strings (this is enforced by the type system)
- All values must be of the same type as specified in `map(<value_type>)`
- Useful for representing configuration objects, dictionaries, and key-value stores

#### Example

```yaml
--8<-- "docs-examples/reference/data-types/map-producer.yaml:37:46"
```

This simple example demonstrates using `map(string)` and `map(int)` types in stream definitions and function result types:

??? info "Producer - `map` example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/map-producer.yaml" %}
    ```

??? info "Processor - `map` example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/map-processor.yaml" %}
    ```

**What this example does:**

- **Stream definitions** use `valueType: map(string)` and `valueType: map(int)` to define strongly-typed maps
- **Function result types** use `resultType: (string, map(string))` to return maps with type safety
- **Processing functions** use `resultType: map(string)` and `resultType: map(int)` to transform and validate map contents
- Demonstrates how the `map(valuetype)` syntax ensures all values in a map conform to the specified type

#### Struct

A struct is a key-value map where all keys are strings. This is the most common complex type and is used for JSON objects, Avro records, etc.

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
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/struct-producer.yaml" %}
    ```

??? info "Processor - Struct example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/struct-processor.yaml" %}
    ```

#### Tuple

A tuple combines multiple elements of different types into a single value.

**Syntax:**
```yaml
# Standard bracket notation
valueType: (<type1>, <type2>, ...)

# Alternative function notation (avoids YAML validation warnings)
valueType: tuple(<type1>, <type2>, ...)
```

**Example:**
```yaml
streams:
  sensor_stream:
    topic: sensor-data
    keyType: string
    valueType: (string, avro:SensorData)     # Standard notation
  
  coordinate_stream:
    topic: coordinates
    keyType: string
    valueType: tuple(double, double)           # Alternative notation (no quotes needed)
```

In Python code, a tuple is represented as a Python tuple:
```yaml
functions:
  create_user_age_pair:
    type: keyValueTransformer
    expression: (value.get("name"), value.get("age"))
    resultType: (string, int)               # Standard notation
  
  create_coordinate_pair:
    type: keyValueTransformer
    expression: (value.get("lat"), value.get("lng"))
    resultType: tuple(double, double)         # Alternative notation (no quotes needed)
```

**See it in action**:

- [Tuple example](../reference/function-reference.md#foreignkeyextractor)

#### Example

```yaml
--8<-- "docs-examples/reference/data-types/list-tuple-simple-producer.yaml:2:15"
```

This example demonstrates using `tuple(string, json)` syntax in function result types to avoid YAML validation warnings:

??? info "Producer - `tuple()` syntax example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/list-tuple-simple-producer.yaml" %}
    ```

??? info "Processor - `tuple()` syntax example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/list-tuple-simple-processor.yaml" %}
    ```

**What this example does:**

- **Producer function** uses `resultType: tuple(string, json)` instead of `"(string, json)"` to avoid quotes
- **Processor function** uses `resultType: list(int)` to demonstrate both new syntaxes working together
- **No functional difference** - the new syntax provides YAML-friendly alternatives

#### Union

A union type can be one of several possible types.

**Syntax:**
```yaml
valueType: union(<type1>, <type2>, ...)
```

**Example:**

Union types are used in two main places in KSML:

**1. In stream definitions** - to specify that a stream can contain multiple types:
```yaml
streams:
  optional_messages:
    topic: optional-messages
    keyType: string
    valueType: union(null, json)  # This stream accepts either null OR a JSON object
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
    resultType: (string, union(null, json))  # Returns a tuple with union type
```

**What union types mean:**

- `union(null, json)` means the value can be either `null` OR a JSON object
- When processing union types, your code must check which type was received and handle each case

**Complete example showing both usages:**

??? info "Producer - Union example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/union-producer.yaml" %}
    ```

??? info "Processor - Union example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/union-processor.yaml" %}
    ```

#### Windowed

Windowing operations in Kafka Streams group messages together in time-based windows. KSML provides the `windowed(<base_type>)` syntax to work with these windowed keys.

**Syntax:**
```yaml
# Without notation - requires manual transformation for Kafka output
keyType: windowed(<base_type>)

# With notation - automatically serializes to the specified format
keyType: <notation>:windowed(<base_type>)  # e.g., json:windowed(string), avro:windowed(string)
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
--8<-- "docs-examples/reference/data-types/windowed-processor.yaml:51:52"
```

**2. With Notation Prefix (Automatic Serialization):**

Using a notation prefix like `json:windowed(string)` or `avro:windowed(string)` enables automatic serialization of the windowed key structure:

```yaml
--8<-- "docs-examples/reference/data-types/windowed-processor-notation.yaml:59:60"
```

The notation automatically serializes the windowed key as a structured object with fields: `start`, `end`, `startTime`, `endTime`, and `key`.

**Complete Examples:**

??? info "Producer - Generates events for windowing (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/windowed-producer.yaml" %}
    ```

??? info "Processor - Manual transformation approach (click to expand)"

    This example shows how to manually transform windowed keys to regular strings when not using notation:

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/windowed-processor.yaml" %}
    ```

??? info "Processor - Automatic serialization with notation (click to expand)"

    This example shows the simpler approach using notation for automatic serialization:

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/reference/data-types/windowed-processor-notation.yaml" %}
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

### The Any and "?" Types

KSML supports wildcard types `any` and `?` (which are equivalent) that represent unknown or variable data types. These map internally to `DataType.UNKNOWN` and can only be used for function parameters when the exact type is not known at definition time. They cannot be used for stream types or function result types due to serialization and type system requirements.

**Syntax:**

The `any` and `?` types can be used in:

- Function parameters only (`type: any` or `type: "?"`)

```yaml
# Function parameters (SUPPORTED)
functions:
  my_function:
    type: generic
    parameters:
      - name: input
        type: any      # Accepts any type
      - name: other
        type: "?"      # Alternative syntax (quote to avoid YAML issues)
    code: |
      # Process the input parameter of unknown type
      return "processed"
    resultType: string   # Must be a concrete type

# Stream types (NOT SUPPORTED)
# valueType: any     # ❌ This will fail with "JSON serde not found"
# keyType: "?"       # ❌ This will fail with serialization error

# Function result types (NOT SUPPORTED)
# resultType: any    # ❌ This will fail with topology type checking error
```

**Why the limitations exist:**

- **Stream types**: Kafka requires concrete serialization formats. The `any` type cannot be serialized to Kafka topics because there's no serde for unknown data types.
- **Result types**: The topology type system requires concrete types for type checking and ensuring data flows correctly between operations.

**Key Use Cases:**

- Generic utility functions that accept multiple data types as parameters
- Helper functions that need to handle variable input types
- Functions that process data generically before converting to concrete output types

??? info "Producer - Any type demonstration (click to expand)"

    This example demonstrates using the `?` type for function parameters, showing how to create generic utility functions.

    ```yaml
    --8<-- "docs-examples/reference/data-types/any-producer.yaml"
    ```

??? info "Processor - Any type processing (click to expand)"

    This example shows how to process data using the `any` type for function parameters, demonstrating type-agnostic helper functions.

    ```yaml
    --8<-- "docs-examples/reference/data-types/any-processor.yaml"
    ```

## Notation Formats

KSML uses _notations_ to allow reading/writing different message formats to Kafka topics. Notations are specified as a prefix to the schema name.

### Examples
**See a working example for every data format in this tutorial**:

- [Data Format Examples](../tutorials/beginner/data-formats.md)

### Format Selection Guide

The choice of notation depends on your specific requirements:

| If you need...                              | Consider using... |
|---------------------------------------------|-------------------|
| Schema evolution and backward compatibility | Avro or Protobuf  |
| Human-readable data for debugging           | JSON              |
| Integration with legacy systems             | XML or SOAP       |
| Simple tabular data                         | CSV               |
| Compact binary format                       | Avro or Protobuf  |
| Raw binary data handling                    | Binary            |

### Avro

Avro is a binary format that supports schema evolution.

**Syntax:**
```yaml
valueType: avro:<schema_name>
# or for schema registry lookup
valueType: avro
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
valueType: json
# For JSON with a schema:
valueType: json:<schema_name>
```

**Example:**
```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: json

  orders:
    topic: orders
    keyType: string
    valueType: json:Order
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
valueType: jsonschema
# For JSON with a schema:
valueType: jsonschema:<schema_name>
```

**Example:**
```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: jsonschema:UserProfile
```

### CSV

CSV (Comma-Separated Values) is a simple tabular data format.

**Syntax:**
```yaml
# For schemaless CSV:
valueType: csv
# For CSV with a schema:
valueType: csv:<schema_name>
```

**Example:**
```yaml
streams:
  sales_data:
    topic: sales-data
    keyType: string
    valueType: csv

  inventory_data:
    topic: inventory-data
    keyType: string
    valueType: csv:InventoryRecord
```

### XML

XML (Extensible Markup Language) is used for complex hierarchical data.

**Syntax:**
```yaml
# For schemaless XML:
valueType: xml
# For XML with a schema:
valueType: xml:<schema_name>
```

**Example:**
```yaml
streams:
  customer_data:
    topic: customer-data
    keyType: string
    valueType: xml:CustomerData
```

### Protobuf

Protobuf is a popular encoding format developed by Google.

**Syntax:**
```yaml
# For schema registry lookup:
valueType: protobuf
# For Protobuf with a schema:
valueType: protobuf:<schema_name>
```

**Example:**
```yaml
streams:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: protobuf:UserProfile
```

### Binary

Binary data represents raw bytes for custom protocols.

**Syntax:**
```yaml
valueType: binary
```

**Example:**
```yaml
streams:
  binary_data:
    topic: binary-messages
    keyType: string
    valueType: binary
```

### SOAP

SOAP (Simple Object Access Protocol) is an XML-based messaging protocol.

**Syntax:**
```yaml
valueType: soap
```

**Example:**
```yaml
streams:
  service_requests:
    topic: service-requests
    keyType: string
    valueType: soap
```

## Schema Management

When working with structured data, it's important to manage your schemas effectively.

### Examples
**See a working example for every type of schema in this tutorial**:

- [Schema Examples](../tutorials/beginner/data-formats.md)

### Local Files vs. Schema Registry

**Local Schema Files:**
When a schema is specified, KSML loads the schema from a local file from the `schemaDirectory`. The notation determines the filename extension:

- Avro schemas: `.avsc` extension
- XML schemas: `.xsd` extension  
- CSV schemas: `.csv` extension
- JSON schemas: `.json` extension

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: avro:SensorReading  # Looks for SensorReading.avsc
```

**Schema Registry Lookup:**
When no schema is specified, KSML assumes the schema is loadable from Schema Registry:

```yaml
streams:
  sensor_data:
    topic: sensor-reading
    keyType: string
    valueType: avro  # Schema fetched from registry
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
    --8<-- "docs-examples/reference/data-types/auto-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "docs-examples/reference/data-types/auto-conversion-processor.yaml"
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
    --8<-- "docs-examples/reference/data-types/explicit-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "docs-examples/reference/data-types/explicit-conversion-processor.yaml"
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
    --8<-- "docs-examples/reference/data-types/multi-conversion-producer.yaml"
    ```

    Processor:
    ```yaml
    --8<-- "docs-examples/reference/data-types/multi-conversion-processor.yaml"
    ```

**Key Takeaway:** Functions convert automatically, streams need explicit conversion.

## Working with Multiple Formats in a Single Pipeline

Process different data formats within one KSML definition using separate pipelines.

This producer generates both JSON config data and Avro sensor data:

??? info "Producer definition (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/beginner-tutorial/different-data-formats/producer-multiple-formats.yaml" %}
    ```

This processor shows two pipelines handling different formats (Avro and JSON) and combining results:

??? info "Processor definition for working with multiple formats in a single pipeline (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/beginner-tutorial/different-data-formats/processor-multiple-formats.yaml" %}
    ```

## Type Definition Quoting Rules

In KSML, quotes around type definitions are **always optional**. KSML can parse all type expressions correctly whether they have quotes or not. The choice to use quotes is purely a matter of style and preference.

### All Type Expressions Work Without Quotes:

```yaml
# Basic types
keyType: string
valueType: json
resultType: int

# Function-style types
valueType: enum(PENDING, PROCESSING, SHIPPED)
valueType: map(string)
keyType: windowed(string)
resultType: list(int)
resultType: tuple(string, json)

# Complex expressions
valueType: union(null, string)
resultType: list(tuple(string, json)) # [(string, json)] also valid
resultType: (string, json)

# Notation prefixes (with colons)
valueType: avro:SensorData
keyType: protobuf:UserProfile

# With quotes (also valid)
resultType: list(tuple(string, json)) # [(string, json)] also valid
valueType: enum(PENDING, SHIPPED)
```

### YAML Syntax Highlighting Note

Some YAML syntax highlighters may incorrectly interpret bracket notation like `[(string, json)]`, expecting proper array syntax.

For better highlighting, use quotes `"[(string, json)]"` or the cleaner `list(tuple(string, json))` syntax.

### Summary:

**All type expressions work without quotes in KSML.** Use quotes only if you prefer them for style, but they are never functionally required. For bracket notation, consider using the `list()` function syntax for cleaner, more readable code.
