# Quick Start with KSML

Get KSML running in 5 minutes! This guide shows you how to create a powerful stream processing application using only YAML and Python - no Java required. You'll build a real-time data pipeline that filters sensor data, converts temperatures, and triggers alerts.

## What We'll Build

In this quickstart, you'll create a stream processing application that:

- 🔍 **Filters** out invalid sensor readings
- 🌡️ **Converts** temperatures from Fahrenheit to Celsius
- 📊 **Calculates** heat index based on temperature and humidity
- 🚨 **Triggers** alerts for extreme conditions
- 📝 **Logs** all processing steps for monitoring

All with just YAML configuration and simple Python expressions!

## Prerequisites

You'll need:

- **Docker Compose** installed ([installation guide](https://docs.docker.com/compose/install/))
- 5 minutes of your time

## Step 1: Set Up Your Environment

Create a new directory for your KSML project and add the docker-compose.yml:

```bash
mkdir my-ksml-project
cd my-ksml-project
mkdir examples
```

Create a `docker-compose.yml` file with the following content:

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
    command: "bash -c 'echo Trying to create topics... && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temperature_data && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic tutorial_input && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic filtered_data && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic alerts_stream && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temperature_data_copied && \
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temperature_data_converted'"
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

Check the Kafka UI at [http://localhost:8080/topics](http://localhost:8080/topics) to see your topics.

## Step 4: Create Your KSML Stream Processing Application

Now for the exciting part! Let's create a real stream processing application that showcases KSML's power.

In the `examples/` directory, create `sensor-monitor.yaml`:

```yaml
streams:
  input_stream:
    topic: temperature_data
    keyType: string
    valueType: json
  processed_stream:
    topic: temperature_data_converted
    keyType: string
    valueType: json
  alerts_stream:
    topic: alerts_stream
    keyType: string
    valueType: json

functions:
  # Filter out invalid readings
  is_valid_reading:
    type: predicate
    code: |
      # Check if temperature is within reasonable range
      temp = value.get('temperature', 0)
      humidity = value.get('humidity', 0)
      valid = 0 < temp < 150 and 0 <= humidity <= 100
      if not valid:
        print(f"Filtering out invalid reading: temp={temp}, humidity={humidity}")
    expression: valid
    resultType: boolean

  # Convert temperature and calculate heat index
  enrich_sensor_data:
    type: keyValueTransformer
    code: |
      import time
      
      # Convert Fahrenheit to Celsius
      temp_f = value.get('temperature', 0)
      temp_c = round((temp_f - 32) * 5/9, 2)
      humidity = value.get('humidity', 50)
      
      # Calculate heat index (simplified formula)
      heat_index = round(temp_c * 1.2 + (humidity * 0.1), 2)
      
      # Determine comfort level
      if heat_index < 20:
        comfort = "cold"
      elif heat_index < 25:
        comfort = "comfortable"
      elif heat_index < 30:
        comfort = "warm"
      else:
        comfort = "hot"
      
      # Build enriched data
      enriched = {
        "sensor_id": key,
        "temperature_f": temp_f,
        "temperature_c": temp_c,
        "humidity": humidity,
        "heat_index": heat_index,
        "comfort_level": comfort,
        "timestamp": int(time.time() * 1000),
        "location": value.get('location', 'unknown')
      }
      
      print(f"Processed {key}: {temp_f}°F → {temp_c}°C, comfort: {comfort}")
    expression: (key, enriched)
    resultType: (string, json)

  # Check if we need to send an alert
  needs_alert:
    type: predicate
    code: |
      # Alert if temperature is extreme
      result = value.get('temperature_c', 0) > 35 or value.get('temperature_c', 0) < 5
    expression: result
    resultType: boolean

  # Create alert message
  create_alert:
    type: keyValueTransformer
    code: |
      alert = {
        "alert_type": "TEMPERATURE_EXTREME",
        "sensor_id": key,
        "temperature_c": value.get('temperature_c'),
        "temperature_f": value.get('temperature_f'),
        "message": f"Extreme temperature detected at {value.get('location', 'unknown')}: {value.get('temperature_c')}°C",
        "timestamp": value.get('timestamp'),
        "severity": "HIGH" if value.get('temperature_c', 0) > 40 or value.get('temperature_c', 0) < 0 else "MEDIUM"
      }
    expression: (key, alert)
    resultType: (string, json)

pipelines:
  main_pipeline:
    from: input_stream
    via:
      # Step 1: Filter out invalid readings
      - type: filter
        if: is_valid_reading
      
      # Step 2: Enrich and transform the data
      - type: transformKeyValue
        mapper: enrich_sensor_data
    
    # Step 3: Route to different topics based on conditions
    branch:
      # Send alerts for extreme temperatures
      - if: needs_alert
        via:
          - type: transformKeyValue
            mapper: create_alert
        to: alerts_stream
      
      # All valid readings go to processed stream
      - to: processed_stream
```

Now create the KSML runner configuration file `ksml-runner.yaml` in your `examples/` directory:

```yaml
kafka:
   bootstrap.servers: broker:9093
   application.id: io.ksml.example.producer
   security.protocol: PLAINTEXT
   acks: all

ksml:
  definitions:
    # format is: <namespace>: <filename> 
    sensormonitor: sensor-monitor.yaml 
  
  # The examples directory is mounted to /ksml in the Docker container
  configDirectory: /ksml          # When not set defaults to the working directory
  schemaDirectory: /ksml          # When not set defaults to the config directory
  storageDirectory: /tmp          # When not set defaults to the default JVM temp directory

  # This section defines if a REST endpoint is opened on the KSML runner, through which
  # state stores and/or readiness / liveness probes can be accessed.
  applicationServer:
    enabled: false                # Set to true to enable, or false to disable
    host: 0.0.0.0                 # IP address to bind the REST server to
    port: 8080                    # Port number to listen on

  # This section defines whether a Prometheus endpoint is opened to allow metric scraping.
  prometheus:
    enabled: false                 # Set to true to enable, or false to disable
    host: 0.0.0.0                 # IP address to bind the Prometheus agent server to
    port: 9999                    # Port number to listen on

  # This section enables error handling or error ignoring for certain types of errors.
  errorHandling:
    consume:                      # Error handling definitions for consume errors
      log: true                   # Log errors true/false
      logPayload: true            # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ConsumeError    # Definition of the error logger name.
      handler: stopOnFail         # How to proceed after encountering the error. Either continueOnFail or stopOnFail.
    process:
      log: true                   # Log errors true/false
      logPayload: true            # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ProcessError    # Definition of the error logger name.
      handler: continueOnFail     # How to proceed after encountering the error. Either continueOnFail or stopOnFail.
    produce:
      log: true                   # Log errors true/false
      logPayload: true            # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ProduceError    # Definition of the error logger name.
      handler: continueOnFail     # How to proceed after encountering the error. Either continueOnFail or stopOnFail.

  enableProducers: true           # Set to true to allow producer definitions to be parsed in the KSML definitions and be executed.
  enablePipelines: true          # Set to true to allow pipeline definitions to be parsed in the KSML definitions and be executed.
```

## Step 5: Start Your Stream Processing Application

Restart the KSML runner to load your application:

```bash
docker compose restart ksml
```

Check the logs to confirm it started:

```bash
docker compose logs ksml -f
```

## Step 6: See It In Action! 

Let's send some sensor data and watch the magic happen. Start the Kafka console producer:
> Please note that KSML is capable of also producing

```bash
docker compose exec broker kafka-console-producer.sh --bootstrap-server broker:9093 --topic temperature_data --property "parse.key=true" --property "key.separator=:"
```

Now paste these test messages (press Enter after each line):

```
sensor1:{"temperature": 72, "humidity": 45, "location": "office"}
sensor2:{"temperature": 98, "humidity": 80, "location": "server_room"} 
sensor3:{"temperature": -10, "humidity": 30, "location": "freezer"}
sensor4:{"temperature": 105, "humidity": 95, "location": "boiler_room"}
sensor5:{"temperature": 200, "humidity": 150, "location": "invalid"}
```

Press `Ctrl+C` to exit the producer.

## What Just Happened?

Check the KSML logs to see your pipeline in action:

```bash
docker compose logs ksml -f
```

You'll see:

- ✅ Valid readings being processed with temperature conversions
- 🌡️ Comfort levels calculated (cold, comfortable, warm, hot)
- 🚨 Alerts generated for extreme temperatures (freezer and boiler room)
- ❌ Invalid sensor5 data filtered out

Open Kafka UI at [http://localhost:8080](http://localhost:8080) and explore the transformed data:

## Data Transformation Examples

### Normal Processing Flow

**INPUT** (to `temperature_data` topic):
```json
key: sensor1
value: {
  "temperature": 72,
  "humidity": 45, 
  "location": "office"
}
```

**OUTPUT** (to `temperature_data_converted` topic):
```json
key: sensor1
value: {
  "sensor_id": "sensor1",
  "location": "office",
  "temperature_f": 72,
  "temperature_c": 22.22,
  "humidity": 45,
  "heat_index": 26.89,
  "comfort_level": "comfortable",
  "timestamp": 1753956574552
}
```

### Alert Generation Flow

**INPUT** (to `temperature_data` topic):
```json
key: sensor2
value: {
  "temperature": 98,
  "humidity": 80,
  "location": "server_room"
}
```

**OUTPUT** (to `alerts_stream` topic):
```json
key: sensor2
value: {
  "alert_type": "TEMPERATURE_EXTREME",
  "sensor_id": "sensor2",
  "temperature_c": 36.67,
  "temperature_f": 98,
  "message": "Extreme temperature detected at server_room: 36.67°C",
  "timestamp": 1753956579977,
  "severity": "MEDIUM"
}
```

See how your simple YAML configuration:

- ✅ **Validates** data (filters invalid sensor5)
- 🌡️ **Converts** temperatures (72°F → 22.22°C)
- 📊 **Calculates** heat index and comfort levels
- 🚨 **Generates** alerts for extreme conditions (>35°C)
- 📝 **Enriches** data with timestamps and metadata


## 🎉 Congratulations!

You just built a production-ready stream processing application without writing a single line of Java! Your pipeline:

- **Validates** data quality in real-time
- **Transforms** temperatures and calculates metrics
- **Routes** messages intelligently based on content
- **Generates** alerts for critical conditions

All with just YAML configuration and simple Python expressions.

## What Makes This Powerful?

Traditional Kafka Streams would require:

- 100s (if not 1000s)  lines of Java 
- Complex build configuration
- Compilation and deployment steps
- Java expertise

With KSML, you got the same result with:

- ✅ 50 lines of readable YAML
- ✅ Simple Python expressions
- ✅ Instant deployment
- ✅ No compilation needed

## Troubleshooting

**Common Issues:**

- **Port conflicts**: Modify ports in docker-compose.yml if 9092/8080 are in use
- **Container issues**: Run `docker compose logs <service>` to check specific services
- **YAML errors**: Ensure proper indentation (2 spaces, not tabs)

## Next Steps

Ready to learn more? Here's where to go next:

1. **[Understanding KSML](introduction.md)** - Learn the concepts behind what you just built
2. **[KSML Basics Tutorial](basics-tutorial.md)** - Deep dive into the language features with a step-by-step tutorial
3. **[Examples Library](../resources/examples-library.md)** - More patterns and use cases

## Quick Commands Reference

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

---

**Need help?** Check our [Troubleshooting Guide](../resources/troubleshooting.md) or visit the [Community Forum](../resources/community.md).