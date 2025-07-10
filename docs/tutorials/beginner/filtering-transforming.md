# Filtering and Transforming Data in KSML

This tutorial builds on the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md) and explores more advanced filtering and transformation techniques in KSML. You'll learn how to work with complex filter conditions, apply multiple transformations, handle nested data structures, and implement error handling in your transformations.

## What You'll Build

In this tutorial, you'll build a data pipeline that:

1. Reads sensor data from a Kafka topic
2. Applies complex filtering based on multiple conditions
3. Transforms the data using various techniques
4. Handles potential errors in the transformation process
5. Writes the processed data to another Kafka topic

## Prerequisites

Before you begin, make sure you have:

- Completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- A running KSML environment with Kafka
- Basic understanding of YAML and Python syntax

## Complex Filtering Techniques

### Using Multiple Conditions

Let's start by creating a filter that combines multiple conditions:

```yaml
streams:
  input_stream:
    topic: tutorial_input
    keyType: string
    valueType: json
  output_stream:
    topic: filtered_data
    keyType: string
    valueType: json

pipelines:
  filtering_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: value.get('temperature', 0) > 70 and value.get('humidity', 99) < 50 and value.get('location', '') == 'warehouse'
    to: output_stream
```

This filter only passes messages where:
- The temperature is greater than 70°F
- The humidity is less than 50%
- The location is 'warehouse'

### Using Custom Filter Functions

For more complex filtering logic, you can create a custom filter function:

```yaml
streams:
  input_stream:
    topic: tutorial_input
    keyType: string
    valueType: json
  alerts_stream:
    topic: alerts_stream
    keyType: string
    valueType: json

functions:
  is_critical_sensor:
    type: predicate
    code: |
      # Check if this is a critical sensor based on multiple criteria
      if value.get('type') != 'temperature':
        return False

      # Check location
      if value.get('location') not in ['server_room', 'data_center']:
        return False

      # Check temperature threshold based on location
      if value.get('location') == 'server_room' and value.get('temperature') > 80:
        return True
      if value.get('location') == 'data_center' and value.get('temperature') > 75:
        return True

      return False

pipelines:
  critical_alerts:
    from: input_stream
    via:
      - type: filter
        if: is_critical_sensor
    to: alerts_stream
```

This function implements complex business logic to determine if a sensor reading indicates a critical situation that requires an alert.

### Filtering with Error Handling

Sometimes your filter conditions might encounter malformed data. Here's how to handle that:

```yaml
functions:
  safe_filter:
    type: predicate
    code: |
      try:
        # Attempt to apply our filter logic
        if 'temperature' not in value or 'humidity' not in value:
          log.warn("Missing required fields in message: {}", value)
          return False

        return value['temperature'] > 70 and value['humidity'] < 50
      except Exception as e:
        # Log the error and filter out the message
        log.error("Error in filter: {} - Message: {}", str(e), value)
        return False

pipelines:
  robust_filtering:
    from: input_stream
    via:
      - type: filter
        if: safe_filter
    to: output_stream
```

This approach ensures that malformed messages are logged and filtered out rather than causing the pipeline to fail.

## Advanced Transformation Techniques

### Transforming Nested Data Structures

Let's look at how to transform data with nested structures:

```yaml
functions:
  transform_nested_data:
    type: valueMapper
    code: |
      # Create a new structure with flattened and transformed data
      result = {
        "device_id": key,
        "timestamp": value.get('metadata', {}).get('timestamp'),
        "readings": {}
      }

      # Extract and transform sensor readings
      sensors = value.get('sensors', {})
      for sensor_type, reading in sensors.items():
        # Convert temperature from F to C if needed
        if sensor_type == 'temperature' and reading.get('unit') == 'F':
          celsius = (reading.get('value') - 32) * 5/9
          result['readings'][sensor_type] = {
            'value': celsius,
            'unit': 'C',
            'original_value': reading.get('value'),
            'original_unit': 'F'
          }
        else:
          result['readings'][sensor_type] = reading

      return result

pipelines:
  transform_pipeline:
    from: input_stream
    via:
      - type: mapValues
        mapper: transform_nested_data
    to: output_stream
```

This transformation function handles a complex nested JSON structure, extracting and transforming specific fields while preserving others.

### Applying Multiple Transformations

You can chain multiple transformations to break down complex logic into manageable steps:

```yaml
pipelines:
  multi_transform_pipeline:
    from: input_stream
    via:
      # Step 1: Extract relevant fields
      - type: mapValues
        mapper:
          expression: {
            "device_id": key,
            "temperature": value.get('sensors', {}).get('temperature', {}).get('value'),
            "humidity": value.get('sensors', {}).get('humidity', {}).get('value'),
            "timestamp": value.get('metadata', {}).get('timestamp')
          }

      # Step 2: Convert temperature from F to C
      - type: mapValues
        mapper:
          expression: {
            "device_id": value.get('device_id'),
            "temperature_c": (value.get('temperature') - 32) * 5/9,
            "humidity": value.get('humidity'),
            "timestamp": value.get('timestamp')
          }

      # Step 3: Add calculated fields
      - type: mapValues
        mapper:
          expression: {
            "device_id": value.get('device_id'),
            "temperature_c": value.get('temperature_c'),
            "humidity": value.get('humidity'),
            "heat_index": value.get('temperature_c') * 1.8 + 32 - 0.55 * (1 - value.get('humidity') / 100),
            "timestamp": value.get('timestamp')
          }
    to: output_stream
```

Breaking transformations into steps makes your pipeline easier to understand and maintain.

### Error Handling in Transformations

Here's how to implement robust error handling in transformations:

```yaml
functions:
  safe_transform:
    type: valueMapper
    code: |
      try:
        # Attempt the transformation
        if 'temperature' not in value:
          log.warn("Missing temperature field in message: {}", value)
          return {"error": "Missing temperature field", "original": value}

        celsius = (value['temperature'] - 32) * 5/9
        return {
          "device_id": key,
          "temperature_f": value['temperature'],
          "temperature_c": celsius,
          "status": "processed"
        }
      except Exception as e:
        # Log the error and return a special error message
        log.error("Error in transformation: {} - Message: {}", str(e), value)
        return {
          "error": str(e),
          "original": value,
          "status": "error"
        }

pipelines:
  robust_transformation:
    from: input_stream
    via:
      - type: mapValues
        mapper: safe_transform
      # You could add a branch here to handle error messages differently
    to: output_stream
```

This approach ensures that transformation errors are caught, logged, and handled gracefully without crashing the pipeline.

## Combining Filtering and Transformation

Let's put everything together in a complete example:

```yaml
streams:
  sensor_data:
    topic: raw_sensor_data
    keyType: string
    valueType: json
  processed_data:
    topic: processed_sensor_data
    keyType: string
    valueType: json
  error_data:
    topic: error_sensor_data
    keyType: string
    valueType: json

functions:
  validate_sensor_data:
    type: predicate
    code: |
      try:
        # Check if all required fields are present
        required_fields = ['temperature', 'humidity', 'location', 'timestamp']
        for field in required_fields:
          if field not in value:
            log.warn("Missing required field '{}' in message: {}", field, value)
            return False

        # Validate data types and ranges
        if not isinstance(value['temperature'], (int, float)) or value['temperature'] < -100 or value['temperature'] > 200:
          log.warn("Invalid temperature value: {}", value['temperature'])
          return False

        if not isinstance(value['humidity'], (int, float)) or value['humidity'] < 0 or value['humidity'] > 100:
          log.warn("Invalid humidity value: {}", value['humidity'])
          return False

        return True
      except Exception as e:
        log.error("Error validating sensor data: {} - Message: {}", str(e), value)
        return False

  transform_sensor_data:
    type: valueMapper
    code: |
      try:
        # Convert temperature from F to C
        temp_c = (value['temperature'] - 32) * 5/9

        # Calculate heat index
        heat_index = temp_c * 1.8 + 32 - 0.55 * (1 - value['humidity'] / 100)

        # Format timestamp
        timestamp = value['timestamp']
        if isinstance(timestamp, (int, float)):
          # Assume it's a Unix timestamp
          from datetime import datetime
          formatted_time = datetime.fromtimestamp(timestamp / 1000).isoformat()
        else:
          # Pass through as is
          formatted_time = timestamp

        return {
          "sensor_id": key,
          "location": value['location'],
          "readings": {
            "temperature": {
              "celsius": round(temp_c, 2),
              "fahrenheit": value['temperature']
            },
            "humidity": value['humidity'],
            "heat_index": round(heat_index, 2)
          },
          "timestamp": formatted_time,
          "processed_at": int(time.time() * 1000)
        }
      except Exception as e:
        log.error("Error transforming sensor data: {} - Message: {}", str(e), value)
        return {
          "error": str(e),
          "original": value,
          "sensor_id": key,
          "timestamp": int(time.time() * 1000)
        }

pipelines:
  process_sensor_data:
    from: sensor_data
    via:
      - type: filter
        if: validate_sensor_data
      - type: mapValues
        mapper: transform_sensor_data
      - type: peek
        forEach:
          code: |
            log.info("Processed sensor data for {}: temp={}°C, humidity={}%", 
                     value.get('sensor_id'), 
                     value.get('readings', {}).get('temperature', {}).get('celsius'), 
                     value.get('readings', {}).get('humidity'))
    branch:
      - if:
          expression: 'error' in value
        to: error_data
      - to: processed_data
```

This complete example:

1. Validates incoming sensor data, filtering out invalid messages
2. Transforms valid data, converting temperatures and calculating derived values
3. Logs information about processed messages
4. Routes messages with errors to an error topic and valid messages to a processed data topic

## Conclusion

In this tutorial, you've learned how to:

- Create complex filters with multiple conditions
- Implement custom filter functions with business logic
- Handle errors in filtering and transformation
- Transform nested data structures
- Apply multiple transformations in sequence
- Combine filtering and transformation in a robust pipeline

These techniques will help you build more sophisticated and reliable KSML applications that can handle real-world data processing challenges.

## Next Steps

- Learn about [working with different data formats](data-formats.md) in KSML
- Explore [logging and monitoring](logging-monitoring.md) to better understand your pipelines
- Move on to [intermediate tutorials](../intermediate/index.md) to learn about stateful operations and joins
