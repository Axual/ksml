# Quick Start with KSML

Get KSML running in 5 minutes! This guide shows you how to create your first stream processing application using only YAML - no Java required. You'll build a simple pipeline that filters and transforms sensor data.

## What We'll Build

A simple data pipeline that:

- **Filters** out invalid sensor readings  
- **Transforms** temperature data
- **Logs** processed messages

All with just YAML configuration and Python code snippets!

## Prerequisites

You'll need:

- **Docker Compose** installed ([installation guide](https://docs.docker.com/compose/install/))
- 5 minutes of your time

## Step 1: Set Up Your Environment

- Create a new directory for your KSML project:

```bash
mkdir my-ksml-project
cd my-ksml-project
mkdir examples
```
- Create a `docker-compose.yml` file:

??? info "Docker Compose Configuration (click to expand)"

    ```yaml
    {%
      include "../local-docker-compose-setup/docker-compose.yml"
    %}
    ```

- Create `kowl-ui-config.yaml` for Kafka UI:

??? info "Kafka UI Configuration (click to expand)"

    ```yaml
    {%
      include "../local-docker-compose-setup/kowl-ui-config.yaml"
    %}
    ```

## Step 2: Start Docker Services

```bash
docker compose up -d
```

This starts:

- **Kafka** broker (port 9092)
- **KSML** runner for your stream processing  
- **Kafka UI** for monitoring (port 8080)
- Automatic topic creation

## Step 3: Verify Everything Started

```bash
docker compose ps
```

- Kafka broker and UI should be running  
- KSML runner will have exited (missing config - we'll fix this next)

Check the Kafka UI at [http://localhost:8080](http://localhost:8080) to see your topics.

## Step 4: Create Your First KSML Application

Now let's create a simple but powerful stream processing application!

In the `examples/` directory, create `sensor-pipeline.yaml`:

??? info "KSML Pipeline Definition (click to expand)"

       > This is a definition for a demo KSML application. It is not required at this point to understand this YAML & Python syntax, we will explain what it does and how it works later.

    ```yaml
    streams:
      input_stream:
        topic: sensor_data
        keyType: string
        valueType: json
      output_stream:
        topic: processed_data
        keyType: string
        valueType: json

    functions:
      is_valid_sensor:
        type: predicate
        expression: value.get('temperature') is not None and value.get('temperature') > -50 and value.get('temperature') < 150
        resultType: boolean

      enrich_data:
        type: keyValueMapper
        code: |
          # Add sensor ID and convert temperature to Celsius
          temp_c = round((value.get('temperature', 0) - 32) * 5/9, 1)
          enriched = {
            "sensor_id": key,
            "temperature_f": value.get('temperature'),
            "temperature_c": temp_c,
            "location": value.get('location', 'unknown'),
            "status": "processed"
          }
        expression: (key, enriched)
        resultType: (string, json)

    pipelines:
      sensor_processing:
        from: input_stream
        via:
          # Step 1: Filter valid readings
          - type: filter
            if: is_valid_sensor
          
          # Step 2: Transform and enrich
          - type: map
            mapper: enrich_data
            
          # Step 3: Log what we processed
          - type: peek
            forEach:
              code: |
                temp_f = value.get('temperature_f')
                temp_c = value.get('temperature_c') 
                location = value.get('location')
                print(f"✅ Processed {key}: {temp_f}°F → {temp_c}°C at {location}")
        
        # Route processed data to output_stream
        to: output_stream
    ```

Now create the KSML runner configuration file `ksml-runner.yaml`:

??? info "KSML Runner Configuration (click to expand)"

    ```yaml
    kafka:
      bootstrap.servers: broker:9093
      application.id: io.ksml.example.producer
      security.protocol: PLAINTEXT
      acks: all

    ksml:
      definitions:
        sensor: sensor-pipeline.yaml
    ```

## Step 5: Start Your Application

Restart the KSML runner:

```bash
docker compose restart ksml
```

Check the logs:

```bash
docker compose logs ksml
```

## Step 6: See It In Action!

Send some test data to see your pipeline work:

```bash
docker compose exec broker kafka-console-producer.sh --bootstrap-server broker:9093 --topic sensor_data --property "parse.key=true" --property "key.separator=:"
```

Paste these test messages (press Enter after each):

```
sensor1:{"temperature": 72, "location": "office"}
sensor2:{"temperature": 98, "location": "server_room"}
sensor3:{"temperature": 32, "location": "warehouse"}
sensor4:{"temperature": 999, "location": "invalid"}
```

Press `Ctrl+C` to exit.

## What Just Happened?

Your KSML pipeline processed the data in real-time:

1. **sensor1** (72°F) → **Filtered IN** (valid range) → **Converted** to 22.2°C → **Logged** processing
2. **sensor2** (98°F) → **Filtered IN** (valid range) → **Converted** to 36.7°C → **Logged** processing  
3. **sensor3** (32°F) → **Filtered IN** (valid range) → **Converted** to 0.0°C → **Logged** processing
4. **sensor4** (999°F) → **Filtered OUT** (invalid - outside -50°F to 150°F range)

Check the KSML logs to see the processing:

```bash
docker compose logs ksml -f
```

You'll see logs like:
```
✅ Processed sensor1: 72°F → 22.2°C at office
✅ Processed sensor2: 98°F → 36.7°C at server_room
✅ Processed sensor3: 32°F → 0.0°C at warehouse
```

Notice sensor4 doesn't appear in the logs because it was filtered out for having an unrealistic temperature.

### Example messages

**INPUT** (to `sensor_data` topic):

- key: `sensor0`
- value:
```json
{
  "temperature": 72,
  "location": "office"
}
```

**OUTPUT** (to `processed_data` topic):

- key: `sensor0`
- value:
```json
{
  "location": "office",
  "sensor_id": "sensor1",
  "status": "processed",
  "temperature_c": 22.2,
  "temperature_f": 72
}
```

Open [http://localhost:8080](http://localhost:8080) to explore your topics and see the transformed data!

## Congratulations!

You just built a stream processing application with:

- **20 lines of YAML** (vs 100+ lines of Java)
- **Minimal Python** (just 3 lines for temperature conversion)
- **Real-time processing** with filtering and transformation
- **No compilation** or complex setup needed

## What Makes KSML Powerful?

**Traditional Kafka Streams requires:**

- Java expertise and boilerplate code
- Complex build configuration
- Compilation and deployment steps

** With KSML you get:**

- Simple YAML configuration
- Optional Python for custom logic
- Instant deployment

## Next Steps

Ready to learn more?

1. **[Understanding KSML](introduction.md)** - Learn the concepts
2. **[KSML Basics Tutorial](basics-tutorial.md)** - Build more advanced pipelines
3. **[Examples Library](../resources/examples-library.md)** - More patterns

**Need help?** Check our [Troubleshooting Guide](../resources/troubleshooting.md)