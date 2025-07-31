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

Create a new directory for your KSML project:

```bash
mkdir my-ksml-project
cd my-ksml-project
mkdir examples
```

Create a `docker-compose.yml` file:

```yaml
networks:
  ksml:
    name: ksml_example
    driver: bridge

services:
  broker:
    image: bitnami/kafka:3.8.0
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
    networks:
      - ksml
    restart: always
    environment:
      KAFKA_CFG_PROCESS_ROLES: 'controller,broker'
      KAFKA_CFG_BROKER_ID: 0
      KAFKA_CFG_NODE_ID: 0
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: '0@broker:9090'
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_CFG_ADVERTISED_LISTENERS: 'INNER://broker:9093,OUTER://localhost:9092'
      KAFKA_CFG_LISTENERS: 'INNER://broker:9093,OUTER://broker:9092,CONTROLLER://broker:9090'
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: 'INNER:PLAINTEXT,OUTER:PLAINTEXT,CONTROLLER:PLAINTEXT'
      KAFKA_CFG_LOG_CLEANUP_POLICY: delete
      KAFKA_CFG_LOG_RETENTION_MINUTES: 10070
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: INNER
      KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CFG_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: 'false'
      KAFKA_CFG_MIN_INSYNC_REPLICAS: 1
      KAFKA_CFG_NUM_PARTITIONS: 1
    healthcheck:
      test: kafka-topics.sh --bootstrap-server broker:9093 --list
      interval: 5s
      timeout: 10s
      retries: 10
      start_period: 5s

  ksml:
    image: registry.axual.io/opensource/images/axual/ksml:1.0.8
    networks:
      - ksml
    container_name: ksml
    working_dir: /ksml
    volumes:
      - ./examples:/ksml
    depends_on:
      broker:
        condition: service_healthy
      kafka-setup:
        condition: service_completed_successfully

  kafka-ui:
    image: quay.io/cloudhut/kowl:master
    container_name: kowl
    restart: always
    ports:
      - 8080:8080
    volumes:
      - ./:/config
    environment:
      CONFIG_FILEPATH: "/config/kafka-ui-config.yaml"
    depends_on:
      broker:
        condition: service_healthy
    networks:
      - ksml

  kafka-setup:
    image: bitnami/kafka:3.8.0
    hostname: kafka-setup
    networks:
      - ksml
    depends_on:
      broker:
        condition: service_healthy
    restart: on-failure
    command: "bash -c 'echo Creating topics... && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic processed_data && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic filtered_data && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic alerts_stream && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic tutorial_input'"
```

Create `kafka-ui-config.yaml` for monitoring:

```yaml
server:
  listenPort: 8080
  listenAddress: 0.0.0.0

kafka:
  brokers:
    - broker:9093
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

✅ Kafka broker and UI should be running  
⚠️ KSML runner will have exited (missing config - we'll fix this next)

Check the Kafka UI at [http://localhost:8080](http://localhost:8080) to see your topics.

## Step 4: Create Your First KSML Application

Now let's create a simple but powerful stream processing application!

In the `examples/` directory, create `sensor-pipeline.yaml`:
> This is a definition for a demo KSML application. It is not required at this point to understand this YAML & Python syntax, we will explain what it does and how it works later.

```yaml
streams:
  input_stream:
    topic: temperature_data
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
    
    # Route processed data to output, invalid data to error stream
    branch:
      - to: output_stream
```

Now create the KSML runner configuration file `ksml-runner.yaml`:

```yaml
kafka:
  bootstrap.servers: broker:9093
  application.id: io.ksml.example.producer
  security.protocol: PLAINTEXT
  acks: all

ksml:
  definitions:
    sensor-monitor: sensor-monitor.yaml
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

Check the KSML logs:

```bash
docker compose logs ksml -f
```

You'll see:

- ✅ Valid sensor readings processed with temperature conversions
- ❌ Invalid sensor4 data filtered out (999°F is unrealistic)

## Data Flow Examples

### Valid Reading Processing

**INPUT** (to `sensor_data` topic):

key: `sensor0`
value:
```json
{
  "temperature": 72,
  "location": "office"
}
```

**OUTPUT** (to `processed_data` topic):
key: `sensor0`
value:
```json
{
  "location": "office",
  "sensor_id": "sensor1",
  "status": "processed",
  "temperature_c": 22.2,
  "temperature_f": 72
}
```

**LOG OUTPUT**:
```
✅ Processed sensor1: 72°F → 22.2°C at office
```

Open [http://localhost:8080](http://localhost:8080) to explore your topics and see the transformed data!

## 🎉 Congratulations!

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

**✅ With KSML you get:**

- Simple YAML configuration
- Optional Python for custom logic
- Instant deployment

## Next Steps

Ready to learn more?

1. **[Understanding KSML](introduction.md)** - Learn the concepts
2. **[KSML Basics Tutorial](basics-tutorial.md)** - Build more advanced pipelines
3. **[Examples Library](../resources/examples-library.md)** - More patterns

## Quick Commands

```bash
# Start everything
docker compose up -d

# Check logs
docker compose logs ksml -f

# Restart KSML after changes
docker compose restart ksml

# Stop everything
docker compose down
```

**Need help?** Check our [Troubleshooting Guide](../resources/troubleshooting.md)