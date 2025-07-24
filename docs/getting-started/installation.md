# Quick Start with KSML

Get KSML running in 5 minutes with this simple Docker Compose setup. You'll have a working stream processing pipeline that transforms temperature data from Fahrenheit to Celsius.

## Quick Start with Docker Compose

The fastest way to get started with KSML is using our pre-configured Docker Compose setup that includes everything you need:

- Kafka broker with pre-configured topics
- KSML runner
- Kafka UI for easy topic monitoring
- Example data setup

### Prerequisites

Before you begin, make sure you have:

- **Docker** and **Docker Compose** installed
- Basic understanding of Kafka concepts like topics and messages

### Step 1: Create the Docker Compose Configuration

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
      KAFKA_CFG_LOG_RETENTION_MINUTES: 10
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
      - ./kafka-ui:/config
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
                       kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temperature_data_converted'"
```

### Step 2: Start the Environment

Start all services with Docker Compose:

```bash
docker compose up -d
```

This command starts:

- **Kafka broker** on port 9092
- **KSML runner** that will execute your KSML definitions
- **Kafka UI** on port 8080 for monitoring topics and messages
- **Topic setup** that creates required topics automatically

### Step 3: Verify Everything is Running

Check that all services are healthy:

```bash
docker compose ps
```

You should see all services in "Up" status. You can also:

- Visit http://localhost:8080 to access Kafka UI
- Check logs: `docker compose logs -f`

### Step 4: Create Your First KSML Definition

Create a simple KSML file to test the setup:

```bash
cat > examples/hello-world.yaml << 'EOF'
streams:
  input_stream:
    topic: temperature_data
    keyType: string
    valueType: json
  output_stream:
    topic: temperature_data_converted
    keyType: string
    valueType: json

functions:
  log_message:
    type: forEach
    code: |
      log.info(f"Processing message: key={key}, value={value}")

pipelines:
  - from: input_stream
    via:
      - type: peek
        forEach: log_message
    to: output_stream
EOF
```

### Step 5: Run Your KSML Application

The KSML runner automatically picks up YAML files from the examples directory. Restart it to load your new definition:

```bash
docker compose restart ksml
```

You should see logs indicating your KSML application has started:

```bash
docker compose logs ksml
```

### Step 6: Explore Your Setup with Kafka UI

Now let's explore what's running using the Kafka UI:

1. **Open Kafka UI** in your browser: http://localhost:8080

2. **View Topics**: You'll see the pre-created topics:
   - `temperature_data` (input topic)
   - `temperature_data_converted` (output topic)

3. **Explore Messages**: Click on any topic to see its configuration and messages

4. **Monitor Activity**: The UI shows real-time information about:
   - Topic partitions and offsets
   - Consumer groups
   - Message throughput

This gives you a visual way to monitor your Kafka topics and see how KSML processes your data.

## Alternative Setup Options

For more advanced setups or existing Kafka clusters, see our [Advanced Installation Guide](advanced-installation.md).

## Troubleshooting

### Common Issues

- **Connection refused errors**: Make sure all containers are running with `docker compose ps`
- **Port conflicts**: If port 9092 or 8080 are in use, modify the docker-compose.yml ports
- **KSML syntax errors**: Check your YAML syntax for proper indentation and formatting

### Getting Help

If you encounter issues:

- Check the [Troubleshooting Guide](../resources/troubleshooting.md) for detailed solutions
- Visit the [Community and Support](../resources/community.md) page for community help

## What Just Happened?

Congratulations! You now have a complete KSML environment running with:

- ✅ Kafka broker ready to handle messages
- ✅ KSML runner ready to execute your pipelines
- ✅ Kafka UI for easy monitoring and exploration
- ✅ Pre-configured topics for immediate use

## Next Steps

Now that you have KSML running, let's understand what you just set up:

👉 **Continue to [Understanding KSML](introduction.md)** to learn what KSML is and why it makes stream processing so much easier.

After that, you can:

- Follow the [KSML Basics Tutorial](basics-tutorial.md) to build your first complete pipeline
- Browse the [Examples Library](../resources/examples-library.md) for more patterns