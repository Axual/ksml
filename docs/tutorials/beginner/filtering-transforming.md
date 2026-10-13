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

1. Make sure to update the definitions section in `ksml-runner.yaml` (the full file is in the [Basics Tutorial](../../getting-started/basics-tutorial.md)):
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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/producer-filtering-transforming.yaml" %}
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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-complex-filtering-multiple-filters.yaml" %}
    ```

This filter only passes messages where:

- The temperature is greater than 20
- The humidity is less than 80%
- The location is 'warehouse'

The diagram below follows four example messages. Only the message that meets all three conditions reaches the output
topic:

<div class="diagram-w45" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example messages
    Topic[(tutorial_input)]
    Events@{ shape: docs, label: "<b>sensor0</b>: warehouse, temperature 45, humidity 60<br/><b>sensor1</b>: warehouse, temperature 50, humidity 85<br/><b>sensor2</b>: server_room, temperature 70, humidity 40<br/><b>sensor3</b>: warehouse, temperature 15, humidity 50" }

    %% Pipeline
    Filter{"<code>filter</code><br/>temperature above 20<br/>and humidity below 80<br/>and location is warehouse"}
    Peek["<code>peek</code><br/>log the message"]
    Output[(filtered_data)]
    Drop["❌ Dropped<br/><b>sensor1</b>: humidity 85<br/><b>sensor2</b>: not in the warehouse<br/><b>sensor3</b>: temperature 15"]

    %% Data Flow
    Topic --> Events
    Events --> Filter
    Filter -->|"sensor0"| Peek
    Filter -.-> Drop
    Peek --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Filter,Peek operation
    class Events message
    class Drop dropped
```

</div>

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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-custom-filter.yaml" %}
    ```

This function implements complex business logic to determine if a sensor reading indicates a critical situation that requires an alert.

The diagram below shows the decisions the `is_critical_sensor` function makes for each message. Only critical readings
are written to `alerts_stream`:

<div class="diagram-w60" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Topic[(tutorial_input)]

    %% Decisions in is_critical_sensor
    Location{"location?"}
    ServerRoom{"temperature<br/>above 20?"}
    DataCenter{"temperature<br/>above 30?"}

    %% Results
    DropServer["❌ Dropped"]
    Alert[(alerts_stream)]
    DropData["❌ Dropped"]
    DropOther["❌ Dropped"]

    %% Data Flow
    Topic --> Location
    Location -->|"server_room"| ServerRoom
    Location -->|"data_center"| DataCenter
    Location -.->|"other location"| DropOther
    ServerRoom -.->|"no"| DropServer
    ServerRoom -->|"yes"| Alert
    DataCenter -->|"yes"| Alert
    DataCenter -.->|"no"| DropData

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Alert topic
    class Location,ServerRoom,DataCenter operation
    class DropServer,DropData,DropOther dropped
```

</div>

### Filtering with Error Handling

Sometimes your filter conditions might encounter malformed data. Here's how to handle that:

??? info "Error Handling in Filters Example (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-error-handling.yaml" %}
    ```

This approach ensures that malformed messages are logged and filtered out rather than causing the pipeline to fail.

The diagram below shows the three ways a message can go through the `safe_filter` function:

<div class="diagram-w70" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Topic[(tutorial_input)]
    Check{"temperature and<br/>humidity present?"}
    Rule{"temperature above 70<br/>and humidity below 50?"}

    %% Results
    Warn["❌ Dropped<br/>log.warn: missing fields"]
    Error["❌ Dropped<br/>log.error: exception<br/>in the filter"]
    NoMatch["❌ Dropped<br/>no match"]
    Alert[(alerts_stream)]

    %% Data Flow
    Topic --> Check
    Check -.->|"no"| Warn
    Check -->|"yes"| Rule
    Check -.->|"error"| Error
    Rule -->|"yes"| Alert
    Rule -.->|"no"| NoMatch

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Alert topic
    class Check,Rule operation
    class Warn,Error partial
    class NoMatch dropped
```

</div>
The test data always contains both fields, so nothing is logged. To see the warning for missing fields, change the key
to one that doesn't exist:
```python
sensors = value.get('sensors2', {})
```
To see the error log, make the code raise an exception, for example by reading a field that doesn't exist with `[]`
instead of `get()`:
```python
temperature = sensors['temperature2']
```

## Advanced Transformation Techniques

For these examples, let's use a different KSML producer definition:

??? info "Enhanced Producer Configuration (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/producer-filtering-transforming-advanced.yaml" %}
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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-nested.yaml" %}
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

The diagram below shows where each field of the output comes from. The key stays the same:

<div class="diagram-w80" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input fields
    subgraph SRC["Input (key: sensor0)"]
        direction TB
        K["message key<br/>sensor0"]
        X1["metadata.timestamp"]
        X2["sensors.temperature<br/>value 80, unit F"]
        X3["sensors.humidity<br/>value 34, unit %"]
        X4["sensors.location<br/>value data_center, unit text"]
    end

    %% Output fields
    subgraph DST["Output (key: sensor0)"]
        direction TB
        T1["device_id"]
        T2["timestamp"]
        T3["readings.temperature<br/>value 26.67, unit C<br/>original_value 80, original_unit F"]
        T4["readings.humidity<br/>unchanged"]
        T5["readings.location<br/>unchanged"]
    end

    %% Mappings
    K -->|"copied from the key"| T1
    X1 -->|"moved to the top"| T2
    X2 -->|"F to C, original kept"| T3
    X3 --> T4
    X4 --> T5

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class K,X1,X2,X3,X4 message
    class T1,T2,T3,T4,T5 result
    style SRC fill:none,stroke:#616161,stroke-dasharray:5 5
    style DST fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

The transformation maintains the original key while restructuring the value to be more suitable for downstream processing, with standardized temperature units and flattened metadata.

### Applying Multiple Transformations

You can chain multiple transformations to break down complex logic into manageable steps:

??? info "Multiple Transformations Pipeline Example (click to expand)"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-multiple-transform.yaml" %}
    ```

The diagram below shows where each field of the output comes from, and which of the three steps creates it:

<div class="diagram-w80" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input fields
    subgraph SRC["Input (key: sensor0)"]
        direction TB
        K["message key<br/>sensor0"]
        X1["metadata.timestamp"]
        X2["sensors.temperature.value<br/>87 F"]
        X3["sensors.humidity.value<br/>61"]
        X4["sensors.location.value<br/>server_room"]
    end

    Drop["❌ not in the output"]

    %% Output fields
    subgraph DST["Output (key: sensor0)"]
        direction TB
        T1["device_id"]
        T2["timestamp"]
        T3["temperature_c<br/>30.56"]
        T4["humidity<br/>61"]
        T5["heat_index_c<br/>27.1"]
    end

    %% Mappings
    K -->|"1. extract_fields"| T1
    X1 -->|"1. extract_fields"| T2
    X2 -->|"1. extract_fields<br/>2. F to C"| T3
    X3 -->|"1. extract_fields"| T4
    X4 -.->|"1. extract_fields"| Drop
    T3 -->|"3. add_heat_index"| T5
    T4 -->|"3. add_heat_index"| T5

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class K,X1,X2,X3,X4 message
    class T1,T2,T3,T4 result
    class T5 partial
    class Drop dropped
    style SRC fill:none,stroke:#616161,stroke-dasharray:5 5
    style DST fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

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

The result is a flat structure with five fields instead of the nested input, with the temperature converted to Celsius and a calculated heat index.
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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-error-handling2.yaml" %}
    ```

The diagram below follows the two example messages from this section. The `map` operation always returns a message,
with a `status` field that the `branch` operation uses to choose the output topic:

<div class="diagram-w60" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Topic[(tutorial_input)]
    Events@{ shape: docs, label: "<b>sensor0</b><br/>temperature 65 F, humidity 71 %<br/><b>sensor1</b><br/>humidity only, no temperature" }

    %% Pipeline
    Map["<code>map</code><br/>safe_transform<br/>try / except"]
    Branch{"<code>branch</code><br/>status?"}

    %% Results
    Ok@{ shape: doc, label: "<b>sensor0</b><br/>status: processed<br/>temperature_c 18.33" }
    Err@{ shape: doc, label: "<b>sensor1</b><br/>status: error<br/>error: Missing temperature data<br/>original message kept" }
    Output[(filtered_data)]
    Alerts[(alerts_stream)]

    %% Data Flow
    Topic --> Events
    Events --> Map
    Map --> Branch
    Branch -->|"processed"| Ok
    Branch -->|"error"| Err
    Ok --> Output
    Err --> Alerts

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output,Alerts topic
    class Map,Branch operation
    class Events message
    class Ok result
    class Err partial
```

</div>

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

The diagram below shows where each field of the processed message comes from:

<div class="diagram-w70" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input fields
    subgraph SRC["Input (key: sensor0)"]
        direction TB
        K["message key<br/>sensor0"]
        X1["metadata.timestamp"]
        X2["sensors.temperature.value<br/>65 F"]
        X3["sensors.humidity.value<br/>71"]
        X4["sensors.location.value<br/>server_room"]
    end

    Drop["❌ not in the output"]

    %% Output fields
    subgraph DST["Output to filtered_data"]
        direction TB
        T1["device_id"]
        T2["timestamp"]
        T3["temperature_f<br/>65"]
        T4["temperature_c<br/>18.33"]
        T5["humidity<br/>71"]
        T6["status<br/>processed"]
    end

    %% Mappings
    K -->|"copied from the key"| T1
    X1 --> T2
    X2 -->|"copied"| T3
    X2 -->|"F to C"| T4
    X3 --> T5
    X4 -.-> Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class K,X1,X2,X3,X4 message
    class T1,T2,T3,T4,T5 result
    class T6 partial
    class Drop dropped
    style SRC fill:none,stroke:#616161,stroke-dasharray:5 5
    style DST fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

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

For the error message, the whole input value is copied to `original`, and the function adds the other fields:

<div class="diagram-w60" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input fields
    subgraph SRC["Input (key: sensor1)"]
        direction TB
        K["message key<br/>sensor1"]
        X1["whole value<br/>no sensors.temperature"]
    end

    %% Output fields
    subgraph DST["Output to alerts_stream"]
        direction TB
        T1["device_id"]
        T2["original"]
        T3["error<br/>Missing temperature data"]
        T4["status<br/>error"]
    end

    %% Mappings
    K -->|"copied from the key"| T1
    X1 -->|"copied as is"| T2

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class K,X1 message
    class T1,T2 result
    class T3,T4 partial
    style SRC fill:none,stroke:#616161,stroke-dasharray:5 5
    style DST fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

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
    {% include "../../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml" %}
    ```

The diagram below shows the four steps of the complete pipeline:

<div class="diagram-w55" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Topic[(tutorial_input)]

    %% Steps
    Validate{"<b>1.</b> <code>filter</code><br/>validate_sensor_data<br/>fields present, values in range"}
    Transform["<b>2.</b> <code>transformKeyValue</code><br/>transform_sensor_data<br/>convert, calculate, format"]
    Log["<b>3.</b> <code>peek</code><br/>log_processed_data"]
    Branch{"<b>4.</b> <code>branch</code><br/>has an error field?"}

    %% Results
    Drop["❌ Dropped<br/>invalid message"]
    Output[(filtered_data)]
    Alerts[(alerts_stream)]

    %% Data Flow
    Topic --> Validate
    Validate -->|"valid"| Transform
    Validate -.->|"invalid"| Drop
    Transform --> Log
    Log --> Branch
    Branch -->|"no"| Output
    Branch -->|"yes: exception<br/>during step 2"| Alerts

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output,Alerts topic
    class Validate,Transform,Log,Branch operation
    class Drop dropped
```

</div>

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
        "heat_index": 61.74,
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

The diagram below shows where each field of the output comes from:

<div class="diagram-w80" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input fields
    subgraph SRC["Input (key: sensor0)"]
        direction TB
        K["message key<br/>sensor0"]
        X1["sensors.location.value<br/>data_center"]
        X2["sensors.temperature.value<br/>62 F"]
        X3["sensors.humidity.value<br/>88"]
        X4["metadata.timestamp<br/>1753949513729"]
    end

    %% Output fields
    subgraph DST["Output to filtered_data"]
        direction TB
        T1["sensor_id"]
        T2["location"]
        T3["readings.temperature.fahrenheit<br/>62"]
        T4["readings.temperature.celsius<br/>16.67"]
        T5["readings.humidity<br/>88"]
        T6["readings.heat_index<br/>61.74"]
        T7["timestamp<br/>2025-07-31T07:31:53.729000"]
        T8["processed_at<br/>current time"]
    end

    %% Mappings
    K -->|"copied from the key"| T1
    X1 --> T2
    X2 -->|"copied"| T3
    X2 -->|"F to C"| T4
    X3 --> T5
    X2 -->|"heat index"| T6
    X3 -->|"heat index"| T6
    X4 -->|"to ISO date"| T7

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class K,X1,X2,X3,X4 message
    class T1,T2,T3,T4,T5,T7 result
    class T6,T8 partial
    style SRC fill:none,stroke:#616161,stroke-dasharray:5 5
    style DST fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

##### LOG output:
```
Processed sensor data for sensor0: temp=16.67C, humidity=88%
```

#### Pipeline Processing Steps

##### Step 1: Validation (`validate_sensor_data`)

- **Purpose**: Ensure data quality by filtering out invalid messages before processing

- **Validation Checks**:

      - **Structural Validation**: Ensures the nested JSON structure contains required fields (`temperature`, `humidity`, `timestamp`)
      - **Data Type Validation**: Confirms values are numeric where expected
      - **Range Validation**:
         - Temperature must be between -100 and 200 (in °F, as sent by the producer)
         - Humidity must be between 0% and 100%

- **Result**: Only valid messages pass through; malformed data is filtered out

##### Step 2: Transformation (`transform_sensor_data`)

- **Purpose**: Enrich and standardize the data format

- **Transformations Applied**:
      - **Temperature Conversion**: Converts 62°F to 16.67°C using the formula `(F - 32) × 5/9`
      - **Heat Index Calculation**: Computes the apparent temperature in °F considering humidity (61.74 in this case), with the same simplified formula as in [Applying Multiple Transformations](#applying-multiple-transformations)
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
      - **Complete Routing**: Every message that passed validation goes to one of the two topics; invalid messages were already dropped in step 1


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