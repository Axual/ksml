# Working with Different Data Formats

This tutorial guides you through working with various data formats in KSML. You'll learn how to process data in different formats, convert between formats, and leverage schema-based formats for data validation and compatibility.

## Prerequisites

- Basic understanding of Kafka concepts (topics, messages)
- Completed the [Building a Simple Data Pipeline](simple-pipeline.md) tutorial
- Familiarity with basic KSML concepts (streams, functions, pipelines)

## What You'll Learn

In this tutorial, you'll learn:
1. How to specify data formats for streams
2. Working with JSON data
3. Using Avro schemas for data validation
4. Processing CSV data
5. Converting between different data formats

## Data Formats in KSML

KSML supports a variety of data formats for both keys and values in Kafka messages:

- **String**: Plain text data
- **JSON**: JavaScript Object Notation for structured data
- **Avro**: Schema-based binary format with strong typing
- **CSV**: Comma-separated values for tabular data
- **XML**: Extensible Markup Language for hierarchical data
- **Protobuf**: Google's Protocol Buffers, a compact binary format
- **Binary**: Raw binary data

Each format has its own strengths and use cases, which we'll explore in this tutorial.

## Specifying Data Formats

When defining streams in KSML, you specify the data format using the `keyType` and `valueType` properties:

```yaml
streams:
  json_stream:
    topic: example_json_topic
    keyType: string
    valueType: json

  avro_stream:
    topic: example_avro_topic
    keyType: string
    valueType: avro:SensorData
```

For schema-based formats like Avro, XML, and Protobuf, you specify the schema name after the format type (e.g., `avro:SensorData`).

## Working with JSON Data

JSON is one of the most common data formats used with Kafka. It's human-readable, flexible, and widely supported.

### Reading and Writing JSON Data

Here's an example of a pipeline that reads and writes JSON data:

```yaml
streams:
  input_stream:
    topic: input_topic
    keyType: string
    valueType: json
  output_stream:
    topic: output_topic
    keyType: string
    valueType: json

functions:
  transform_json:
    type: valueMapper
    code: |
      # Access JSON fields directly
      temperature = value.get('temperature', 0)
      humidity = value.get('humidity', 0)

      # Create a new JSON structure
      return {
        'device_id': key,
        'readings': {
          'temperature': temperature,
          'humidity': humidity
        },
        'status': 'normal' if temperature < 30 else 'warning',
        'timestamp': value.get('timestamp')
      }

pipelines:
  json_pipeline:
    from: input_stream
    via:
      - type: mapValues
        using: transform_json
    to: output_stream
```

### JSON Data Handling Tips

- Use Python's dictionary methods like `get()` to safely access fields that might not exist
- Remember that numeric values in JSON are preserved as numbers in KSML
- Nested structures can be accessed using dot notation or nested `get()` calls

## Working with Avro Data

Avro is a schema-based binary format that provides strong typing, schema evolution, and compact serialization.

### Defining an Avro Schema

Avro schemas are defined in JSON format. Here's a simple example for sensor data:

```json
{
  "type": "record",
  "name": "SensorData",
  "namespace": "com.example",
  "fields": [
    {"name": "name", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": ["null", "string"], "default": null},
    {"name": "type", "type": ["null", "string"], "default": null},
    {"name": "unit", "type": ["null", "string"], "default": null},
    {"name": "value", "type": ["null", "string"], "default": null},
    {"name": "color", "type": ["null", "string"], "default": null},
    {"name": "owner", "type": ["null", "string"], "default": null},
    {"name": "city", "type": ["null", "string"], "default": null}
  ]
}
```

Save this schema in a file with a `.avsc` extension (e.g., `SensorData.avsc`).

### Reading and Writing Avro Data

Here's how to work with Avro data in KSML:

```yaml
streams:
  avro_input:
    topic: sensor_data_avro
    keyType: string
    valueType: avro:SensorData
  json_output:
    topic: sensor_data_json
    keyType: string
    valueType: json

pipelines:
  avro_to_json:
    from: avro_input
    via:
      - type: peek
        forEach:
          code: |
            log.info("Processing Avro record: {}", value)
      - type: convertValue
        into: json
    to: json_output
```

### Benefits of Using Avro

- **Schema Validation**: Data is validated against the schema during serialization
- **Compact Format**: Binary encoding is more efficient than text-based formats
- **Schema Evolution**: Avro supports adding, removing, or changing fields while maintaining compatibility
- **Language Interoperability**: Avro schemas can be used with multiple programming languages

## Working with CSV Data

CSV (Comma-Separated Values) is a simple format for representing tabular data.

### Reading and Writing CSV Data

Here's how to work with CSV data in KSML:

```yaml
streams:
  csv_input:
    topic: sensor_data_csv
    keyType: string
    valueType: csv
  json_output:
    topic: sensor_data_json
    keyType: string
    valueType: json

functions:
  csv_to_json:
    type: valueMapper
    code: |
      # Assuming CSV columns are: name,timestamp,type,unit,value
      if len(value) >= 5:
        return {
          'name': value[0],
          'timestamp': value[1],
          'type': value[2],
          'unit': value[3],
          'value': value[4]
        }
      else:
        log.warn("Invalid CSV record: {}", value)
        return {}

pipelines:
  process_csv:
    from: csv_input
    via:
      - type: mapValues
        using: csv_to_json
    to: json_output
```

### CSV Data Handling Tips

- CSV data is represented as a list of values in KSML
- You need to know the column order to correctly interpret the data
- Consider adding header validation if your CSV data includes headers

## Converting Between Data Formats

KSML makes it easy to convert between different data formats using the `convertValue` operation.

### Example: Converting Between Multiple Formats

Here's an example that demonstrates converting between several formats:

```yaml
streams:
  avro_input:
    topic: sensor_data_avro
    keyType: string
    valueType: avro:SensorData
  xml_output:
    topic: sensor_data_xml
    keyType: string
    valueType: xml:SensorData

pipelines:
  format_conversion:
    from: avro_input
    via:
      # Convert from Avro to JSON
      - type: convertValue
        into: json
      - type: peek
        forEach:
          code: |
            log.info("As JSON: {}", value)

      # Convert from JSON to XML
      - type: convertValue
        into: xml:SensorData
      - type: peek
        forEach:
          code: |
            log.info("As XML object: {}", value)

      # Convert to string to see the XML representation
      - type: convertValue
        into: string
      - type: peek
        forEach:
          code: |
            log.info("As XML string: {}", value)

      # Convert back to XML object for output
      - type: convertValue
        into: xml:SensorData
    to: xml_output
```

### Implicit Conversion

KSML can also perform implicit conversion when writing to a topic with a different format than the current message format:

```yaml
pipelines:
  implicit_conversion:
    from: avro_input  # Stream with Avro format
    to: xml_output    # Stream with XML format
    # The conversion happens automatically
```

## Advanced: Working with Multiple Formats in a Single Pipeline

In real-world scenarios, you might need to process data from multiple sources with different formats. Here's an example that combines JSON and Avro data:

```yaml
streams:
  json_stream:
    topic: device_config
    keyType: string
    valueType: json
  avro_stream:
    topic: sensor_readings
    keyType: string
    valueType: avro:SensorData
  enriched_output:
    topic: enriched_sensor_data
    keyType: string
    valueType: json

tables:
  config_table:
    stream: json_stream
    keyType: string
    valueType: json

functions:
  enrich_sensor_data:
    type: valueMapper
    code: |
      # Get device configuration from the table
      device_config = tables.config_table.get(key)

      # Convert Avro to a dictionary if needed
      sensor_data = value

      # Combine the data
      if device_config is not None:
        return {
          'device_id': key,
          'reading': {
            'type': sensor_data.get('type'),
            'unit': sensor_data.get('unit'),
            'value': sensor_data.get('value')
          },
          'config': {
            'threshold': device_config.get('threshold'),
            'alert_level': device_config.get('alert_level')
          },
          'timestamp': sensor_data.get('timestamp')
        }
      else:
        return sensor_data

pipelines:
  enrichment_pipeline:
    from: avro_stream
    via:
      - type: mapValues
        using: enrich_sensor_data
    to: enriched_output
```

## Conclusion

In this tutorial, you've learned how to work with different data formats in KSML, including:

- Specifying data formats for streams
- Working with JSON data
- Using Avro schemas for data validation
- Processing CSV data
- Converting between different formats

These skills will help you build flexible and interoperable data pipelines that can work with a variety of data sources and destinations.

## Next Steps

- Explore the [Logging and Monitoring](logging-monitoring.md) tutorial to learn how to add effective logging to your pipelines
- Check out the [Intermediate Tutorials](../intermediate/index.md) for more advanced KSML features
- Review the [KSML Examples](../../resources/examples-library.md) for more examples of working with different data formats
