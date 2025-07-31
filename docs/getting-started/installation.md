# Quick Start with KSML

Get KSML running in 5 minutes with this simple Docker Compose setup. You'll have a working KSML application, written in YAML and Python, that creates a Kafka Stream topology that copies a message from one topic to another and prints a log statement.

## Quick Start with Docker Compose

The fastest way to get started with KSML is using our pre-configured Docker Compose setup that includes everything you need:

- Kafka broker with pre-configured topics
- KSML runner - the container that runs the KSML definition
- Kowl UI for easy Kafka topic monitoring

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

> Please note that we are creating more topics than we need for this tutorial, these extra topics will be used for more advanced tutorials later.

Next, create a config file named `kafka-ui-config.yaml` for the Kafka UI service we will use to monitor Kafka, with the following contents:

```yaml
server:
  listenPort: 8080
  listenAddress: 0.0.0.0

kafka:
  brokers:
    - broker:9093
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

### Step 3: Verify your setup so far

Check the services in the docker compose:

```bash
docker compose ps
```

You should see Kafka ("`broker`") and Kafka UI ("`kowl`") running; the "Topic setup" container will have done its work and exited.
The KSML runner will have logged an error about a missing config file and exited; we will fix this in the next step.
<br>
For now, you can check Kafka and Kafka UI, and verify that the test topics have been created:

- Visit [http://localhost:8080](http://localhost:8080) to access Kafka UI, and check the "topics" tab
- Check logs: `docker compose logs -f`

### Step 4: Create Your First KSML Definition

Create a simple KSML file to test the setup. This KSML file create a Kafka Streams topology that:

- Copies a message from topic `temperature_data` to `temperature_data_copied` 
- Prints a log statement in KSML docker compose service:
    - `Processing message: key=sensorX, value={'temperature': YY}`

In the `examples/` directory, create a file `hello-world.yaml` with the 
following contents:

```yaml
streams:
  input_stream:
    topic: temperature_data
    keyType: string
    valueType: json
  output_stream:
    topic: temperature_data_copied
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
```

Finally, the KSML runner needs a configuration file which tells it how to connect to Kafka, and which KSML definitions
to run, along with some other settings (explained later). The default name for this file is `ksml-runner.yaml`, create it
in your `examples/` directory and copy and paste the following content:

```yaml
kafka:
   bootstrap.servers: broker:9093
   application.id: io.ksml.example.producer
   security.protocol: PLAINTEXT
   acks: all

ksml:
  definitions:
    # format is: <namespace>: <filename> 
    helloworld: hello-world.yaml 
  
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

### Step 5: Run Your KSML Application

Restart the KSML runner to load your new definitions:

```bash
docker compose restart ksml
```

You should see logs indicating your KSML application has started and a Kafka Streams topology was created:

```bash
docker compose logs ksml
```

Try producing some messages, for now using the Kafka console producer. You can start it inside the KSML container,
running in interactive mode, using the following command:

```bash
docker compose exec broker kafka-console-producer.sh --bootstrap-server broker:9093 --topic temperature_data --property "parse.key=true" --property "key.separator=:"
```

Then you can enter some test messages, using the format `key:value` where the value is a JSON message:

```
sensor1:{"temperature": 75}
sensor2:{"temperature": 65}
sensor3:{"temperature": 80}
```

Press <Enter> after each message; to stop entering messages, use `Control-C`. Now, if you check the logs of the KSML runner, you should see that your
pipeline processed (in this case: logged) the messages:

```bash
docker compose logs ksml
```

Should show something like:

```
 INFO  helloworld.function.log_message      Processing message: key=sensor1, value={'temperature': 75}
 INFO  helloworld.function.log_message      Processing message: key=sensor2, value={'temperature': 65}
 INFO  helloworld.function.log_message      Processing message: key=sensor3, value={'temperature': 80}
```

Also, checking the output topic on the UI [http://localhost:8080](http://localhost:8080) should show that the records have
been copied to topic `temperature_data_copied`.

Congratulations, you have created your first working KSML Streams application. 
For a more interesting application please continue with the [KSML Basics tutorial](basics-tutorial.md); this will give a more in-depth 
explanation of the KSML language, and lets you build on this example to do temperature conversion, adding fields, and generating 
data from within KSML.

### Step 6: Explore Your Setup with Kafka UI

Now let's explore what's running using the Kafka UI:

1. **Open Kafka UI** in your browser: [http://localhost:8080](http://localhost:8080)

2. **View Topics**: You'll see the pre-created topics:
   - `temperature_data` (input topic)
   - `temperature_data_copied` (output topic)
   - `temperature_data_converted` (this is used in the KSML Basics tutorial)

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