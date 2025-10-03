# Filtering and Transforming Data in KSML

## What We'll Build

In this tutorial, we'll build a data pipeline that:

1. Reads sensor data from a Kafka topic
2. Applies complex filtering based on multiple conditions
3. Transforms the data using various techniques and by handling nested data structures
4. Handles potential errors in the transformation process
5. Writes the processed data to another Kafka topic

## Prerequisites

Before we begin:

- Make sure there is a running Docker Compose KSML environment as described in the [Quick Start](../../getting-started/quick-start.md). 
- We recommend to have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    # Filtering and Transforming Tutorial
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic tutorial_input && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic filtered_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic alerts_stream && \
    ```

### To try out each example

1. Make sure to update the definitions section in `ksml-runner.yaml`(the full file in is in the [Basics Tutorial](../../getting-started/basics-tutorial.md):
```yaml
ksml:
  definitions:
    producer: producer.yaml
    processor: processor.yaml
```
2. When making changes in either file (`producer.yaml` or `processor.yaml`), reload KSML producer and processing definitions:
     - `docker compose restart ksml && docker compose logs ksml -f` (slower due to Kafka Streams rebalancing)
     - `docker compose down && docker compose up -d && docker compose logs ksml -f` (faster, but topics will be empty again due to Kafka broker restart)

## Creating test data 

To let KSML produce random test data with the correct format, let's create a file `producer.yaml` and add this producer definition

??? info "Test Data Producer Configuration (click to expand)"
    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/producer-filtering-transforming.yaml"
    %}
    ```

This will generate simulated sensor data for temperature and humidity, in different locations. The JSON input test data, that we will start from with our filtering and transformations, looks like this:
```json
{
  "sensors": {
    "humidity": 53,
    "location": "server_room",
    "temperature": 143
  }
}
```

## Complex Filtering Techniques

### Using Multiple Filters

Let's start by creating a file `processor.yaml` that filters on multiple conditions:

??? info "Multiple Filter Conditions Example (click to expand)"
    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-complex-filtering-multiple-filters.yaml"
    %}
    ```

This filter only passes messages where:

- The temperature is greater than 20°F
- The humidity is less than 80%
- The location is 'warehouse'

Now let's update the definitions section in `ksml-runner.yaml`:

??? info "KSML Runner Configuration Update (click to expand)"

    ```yaml
    ksml:
      definitions:
         producer: producer.yaml
         processor: processor.yaml
    ```

- Let's test by doing:
```bash
docker compose restart ksml && docker compose logs ksml -f
```
- Here are the input messages: [http://localhost:8080/topics/tutorial_input](http://localhost:8080/topics/tutorial_input)
- Here are the filtered messages: [http://localhost:8080/topics/filtered_data](http://localhost:8080/topics/filtered_data)

### Using Custom Filter Functions

By following the same as in previous section, let's try to create a custom filter function:

??? info "Custom Filter Function Example (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-custom-filter.yaml"
    %}
    ```

This function implements complex business logic to determine if a sensor reading indicates a critical situation that requires an alert.

### Filtering with Error Handling

Sometimes your filter conditions might encounter malformed data. Here's how to handle that:

??? info "Error Handling in Filters Example (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-error-handling.yaml"
    %}
    ```

This approach ensures that malformed messages are logged and filtered out rather than causing the pipeline to fail.
This will not throw errors currently, to check that errors are correctly logged, change the key to something that doesn't exist, for example:
```bash
sensors = value.get('sensors2', {})
```

## Advanced Transformation Techniques

For these examples, let's use a different KSML producer definition:

??? info "Enhanced Producer Configuration (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/producer-filtering-transforming-advanced.yaml"
    %}
    ```

This produces messages like these:
INPUT message:

- key: sensor0
- value:
```json
{
  "metadata": {
    "timestamp": 1753935622755
  },
  "sensors": {
    "humidity": {
      "unit": "%",
      "value": 34
    },
    "location": {
      "unit": "text",
      "value": "data_center"
    },
    "temperature": {
      "unit": "F",
      "value": 80
    }
  }
}
```

### Transforming Nested Data Structures

Let's look at how to transform data with nested structures:

??? info "Nested Data Transformation Example (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-nested.yaml"
    %}
    ```

INPUT message:

- key: sensor0
- value: 
```json
{
  "metadata": {
    "timestamp": 1753935622755
  },
  "sensors": {
    "humidity": {
      "unit": "%",
      "value": 34
    },
    "location": {
      "unit": "text",
      "value": "data_center"
    },
    "temperature": {
      "unit": "F",
      "value": 80
    }
  }
}
```

OUTPUT message:

- key: sensor0
- value:
```json
  {
      "device_id": "sensor0",
      "readings": {
          "humidity": {
              "unit": "%",
              "value": 34
          },
          "location": {
              "unit": "text",
              "value": "data_center"
          },
          "temperature": {
              "original_unit": "F",
              "original_value": 80,
              "unit": "C",
              "value": 26.67
          }
      },
      "timestamp": 1753935622755
  }
```

This transformation performs several operations on the incoming sensor data:

1. The nested `metadata.timestamp` is extracted and placed at the root level of the output
2. The message key (sensor0) is added to the value as `device_id`, making the device identifier available in the message body
3. Temperature readings in Fahrenheit are automatically converted to Celsius using the formula (F - 32) × 5/9
4. When converting temperature, both the original and converted values are retained for audit purposes
5. Only temperature sensors with Fahrenheit units are converted; all other sensor types (`humidity`, `location`) pass through unchanged

The transformation maintains the original key while restructuring the value to be more suitable for downstream processing, with standardized temperature units and flattened metadata.

### Applying Multiple Transformations

You can chain multiple transformations to break down complex logic into manageable steps:

??? info "Multiple Transformations Pipeline Example (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml"
    %}
    ```

INPUT message:

- key: sensor0
- value:
```json
  {
  "metadata": {
    "timestamp": 1753937101388
  },
  "sensors": {
    "humidity": {
      "unit": "%",
      "value": 61
    },
    "location": {
      "unit": "text",
      "value": "server_room"
    },
    "temperature": {
      "unit": "F",
      "value": 87
    }
  }
}
```

OUTPUT message:

- key: sensor0
- value:
```json
{
  "device_id": "sensor0",
  "heat_index_c": 27.1,
  "humidity": 61,
  "temperature_c": 30.56,
  "timestamp": 1753937101388
}
```

This pipeline demonstrates a three-stage transformation process:

**Stage 1: Field Extraction** (`extract_fields`)

- Input: Full nested sensor data with metadata and location information
- Process: Extracts only the essential fields (temperature, humidity, timestamp) and adds the device ID from the message key
- Output: Simplified structure with just the needed values
- Note: The location field is intentionally dropped as it's not needed for calculations

**Stage 2: Temperature Conversion** (`convert_temperature`)

- Input: Extracted data with temperature in Fahrenheit (87°F)
- Process: Converts temperature from Fahrenheit to Celsius using the formula (F - 32) × 5/9
- Output: Same structure but with temperature_c field containing 30.56°C
- Note: Original temperature field is removed, replaced with the converted value

**Stage 3: Heat Index Calculation** (`add_heat_index`)

- Input: Data with temperature in Celsius and humidity percentage
- Process: Calculates the heat index (apparent temperature) considering both temperature and humidity
- Output: Final structure with added heat_index_c field showing 27.1°C
- Note: The heat index is lower than actual temperature due to 61% humidity

The transformation reduces the original nested structure from 5 fields across multiple levels to a flat structure with 5 essential fields, while also performing unit conversion and derived calculations.
Breaking transformations into steps makes your pipeline easier to understand and maintain.

### Error Handling in Transformations

This tutorial demonstrates how to implement robust error handling in KSML transformations, ensuring your data pipelines can gracefully handle unexpected data formats and processing errors.

#### Overview

When processing streaming data, it's crucial to handle errors gracefully without crashing the entire pipeline. This example shows how to:

- Safely extract nested data with validation
- Route successful and failed transformations to different topics
- Preserve error context for debugging

#### KSML Definition

??? info "Error Handling in Transformations Example (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/filtering-transforming/processor-filtering-transforming-error-handling2.yaml"
    %}
    ```

#### Example Data Flow

##### Successful Case

**INPUT message:**

- key: `sensor0`
- value:

```json
{
    "metadata": {
        "timestamp": 1753939130968
    },
    "sensors": {
        "humidity": {
            "unit": "%",
            "value": 71
        },
        "location": {
            "unit": "text",
            "value": "server_room"
        },
        "temperature": {
            "unit": "F",
            "value": 65
        }
    }
}
```

**OUTPUT message (to `filtered_data` topic):**

- key: `sensor0`
- value:
```json
{
    "device_id": "sensor0",
    "humidity": 71,
    "status": "processed",
    "temperature_c": 18.33,
    "temperature_f": 65,
    "timestamp": 1753939130968
}
```

##### Error Case - Missing Temperature

**INPUT message:**

- key: `sensor1`
- value:
```json
{
    "metadata": {
        "timestamp": 1753939130968
    },
    "sensors": {
        "humidity": {
            "unit": "%",
            "value": 71
        }
    }
}
```

**OUTPUT message (to `alerts_stream` topic):**

- key: `sensor1`
- value:
```json
{
    "error": "Missing temperature data",
    "device_id": "sensor1",
    "original": {
        "metadata": {"timestamp": 1753939130968},
        "sensors": {"humidity": {"unit": "%", "value": 71}}
    },
    "status": "error"
}
```

#### Key Features

##### Safe Data Processing
- Uses Python's try-except blocks to catch any unexpected errors during transformation
- Validates data existence before attempting to access nested fields
- Preserves the original message in error cases for debugging

##### Conditional Routing
- Successfully processed messages (with `status: "processed"`) are routed to the `filtered_data` topic
- Error messages (with `status: "error"`) are sent to the `alerts_stream` topic for monitoring
- The branching logic ensures clean separation of successful and failed transformations

##### Data Transformation
- Extracts sensor data from the nested structure
- Converts temperature from Fahrenheit (65°F) to Celsius (18.33°C)
- Flattens the structure while preserving essential fields
- Drops the location field as it's not needed in the output

##### Error Context
- Error messages include the device ID for traceability
- The original message is preserved in error cases
- Specific error messages help identify the type of failure (missing data vs. processing error)

#### Benefits

1. The pipeline continues processing even when encountering malformed data
2. Errors are routed to a dedicated topic for monitoring and alerting
3. Original messages are preserved in error cases for investigation
4. Only valid, successfully processed data reaches the output topic

This approach ensures that transformation errors are caught, logged, and handled gracefully without crashing the pipeline, while maintaining full visibility into what went wrong through the separate error stream.

## Combining Filtering and Transformation

Let's put everything together in a complete example:

??? info "Complete Filtering and Transformation Pipeline (click to expand)"

    ```yaml
    {%
      include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml"
    %}
    ```

#### Example Data Flow

##### INPUT message:
- key: `sensor0`
- value:
```json
{
    "metadata": {
        "timestamp": 1753949513729
    },
    "sensors": {
        "humidity": {
            "unit": "%",
            "value": 88
        },
        "location": {
            "unit": "text",
            "value": "data_center"
        },
        "temperature": {
            "unit": "F",
            "value": 62
        }
    }
}
```

##### OUTPUT message (to `filtered_data` topic):
- key: `sensor0`
- value:
```json
{
    "location": "data_center",
    "processed_at": 1753950116699,
    "readings": {
        "heat_index": 60.51,
        "humidity": 88,
        "temperature": {
            "celsius": 16.67,
            "fahrenheit": 62
        }
    },
    "sensor_id": "sensor0",
    "timestamp": "2025-07-31T07:31:53.729000"
}
```

##### LOG output:
```
Processed sensor data for sensor0: temp=16.67°C, humidity=88%
```

#### Pipeline Processing Steps

##### Step 1: Validation (`validate_sensor_data`)

- **Purpose**: Ensure data quality by filtering out invalid messages before processing

- **Validation Checks**:

      - **Structural Validation**: Ensures the nested JSON structure contains required fields (`temperature`, `humidity`, `timestamp`)
      - **Data Type Validation**: Confirms values are numeric where expected
      - **Range Validation**:
         - Temperature must be between -100°C and 200°C
         - Humidity must be between 0% and 100%

- **Result**: Only valid messages pass through; malformed data is filtered out

##### Step 2: Transformation (`transform_sensor_data`)

- **Purpose**: Enrich and standardize the data format

- **Transformations Applied**:
      - **Temperature Conversion**: Converts 62°F to 16.67°C using the formula `(F - 32) × 5/9`
      - **Heat Index Calculation**: Computes apparent temperature considering humidity (60.51 in this case)
      - **Timestamp Formatting**: Converts Unix timestamp (1753949513729) to ISO format (2025-07-31T07:31:53.729000)
      - **Structure Flattening**: Extracts values from nested structure into a cleaner format
      - **Metadata Addition**: Adds `processed_at` timestamp to track when the transformation occurred

##### Step 3: Logging (`log_processed_data`)

- **Purpose**: Provide operational visibility

- **Features**:

      - **Visibility**: Logs key metrics for monitoring pipeline health
      - **Non-invasive**: Uses `peek` operation to observe data without modifying it
      - **Format**: Outputs sensor ID, temperature in Celsius, and humidity percentage

##### Step 4: Routing

- **Purpose**: Direct messages to appropriate destinations based on processing results

- **Routing Logic**:

      - **Error Handling**: Messages with errors (containing an 'error' field) are routed to `alerts_stream`
      - **Success Path**: Successfully processed messages go to `filtered_data` topic
      - **Guaranteed Delivery**: Every message is routed somewhere - no data loss


#### Conclusion

In this tutorial, you've learned how to:

- Create complex filters with multiple conditions
- Implement custom filter functions with business logic
- Handle errors in filtering and transformation
- Transform nested data structures
- Apply multiple transformations in sequence
- Combine filtering and transformation in a robust pipeline

These techniques will help you build more sophisticated and reliable KSML applications that can handle real-world data processing challenges.

#### Next Steps

- Move on to [intermediate tutorials](../intermediate/index.md) to learn about stateful operations and joins
- Learn about [working with different data formats](data-formats.md) in KSML
- Explore [logging and monitoring](logging-monitoring.md) to better understand your pipelines