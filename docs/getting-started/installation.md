# Installation and Setup

This guide will walk you through setting up a development environment for KSML and running your first KSML application.

## Prerequisites

Before you begin, make sure you have the following installed:

- **Docker**: KSML examples and development environment run in Docker containers
- **Git**: To clone the KSML repository (if you want to run the examples)
- **Basic understanding of Kafka**: Familiarity with Kafka concepts like topics and messages

## Setting Up a Development Environment

There are two main ways to set up KSML:

1. Using the provided Docker Compose setup (recommended for beginners)
2. Configuring KSML to connect to an existing Kafka cluster

### Option 1: Using Docker Compose

The easiest way to get started with KSML is to use the provided Docker Compose setup, which includes:

- Zookeeper
- Kafka broker
- Schema Registry
- Example data producer
- KSML runner

#### Step 1: Clone the Repository

If you haven't already, clone the KSML repository:

```bash
git clone https://github.com/axual/ksml.git
cd ksml
```

#### Step 2: Start the Environment

Start the Docker Compose environment:

```bash
docker compose up -d
```

This command starts all the necessary services in the background. You can check the logs to verify everything is running correctly:

```bash
docker compose logs -f
```

You should see log messages indicating that the services are running, including messages from the example producer that's generating random sensor data:

```
example-producer-1 | 2024-03-06T20:24:49,480Z INFO i.a.k.r.backend.KafkaProducerRunner Calling generate_sensordata_message
example-producer-1 | 2024-03-06T20:24:49,480Z INFO i.a.k.r.backend.ExecutableProducer Message: key=sensor2, value=SensorData: {"city":"Utrecht", "color":"white", "name":"sensor2", "owner":"Alice", "timestamp":1709756689480, "type":"HUMIDITY", "unit":"%", "value":"66"}
```

Press CTRL-C to stop following the logs when you're satisfied that the environment is running correctly.

### Option 2: Connecting to an Existing Kafka Cluster

If you already have a Kafka cluster and want to use KSML with it, you'll need to configure the KSML runner to connect to your cluster.

#### Step 1: Create a Configuration File

Create a file named `ksml-runner.yaml` with the following content, adjusting the values to match your Kafka cluster configuration:

```yaml
{% include "../../examples/installation/template-ksml-runner.yaml" %}
```

#### Step 2: Run the KSML Runner

Run the KSML runner with your configuration:

```bash
docker run -v $(pwd):/app axual/ksml-runner:latest --config /app/ksml-runner.yaml
```

## Running Your First KSML Application

Now that you have a running environment, let's run a simple KSML application that processes the example sensor data.

### Step 1: Run the Example KSML Runner

If you're using the Docker Compose setup, after building a local KSML image, you can run the example KSML runner with:

```bash
./examples/run-local.sh
```

This will start a KSML runner container that processes the example KSML definitions. You should see output similar to:

```
2024-03-06T20:24:51,921Z INFO io.axual.ksml.runner.KSMLRunner Starting KSML Runner 1.76.0.0
...
2024-03-06T20:24:57,196Z INFO ksml.functions.log_message Consumed AVRO message - key=sensor9, value={'city': 'Alkmaar', 'color': 'yellow', 'name': 'sensor9', 'owner': 'Bob', 'timestamp': 1709749917190, 'type': 'LENGTH', 'unit': 'm', 'value': '562', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
```

### Step 2: Explore the Example KSML Definitions

The example KSML definitions are located in the `examples` directory. Take a look at some of the simpler examples to understand how KSML works:

- `01-example-inspect.yaml`: Shows how to read and log messages from a topic
- `02-example-copy.yaml`: Demonstrates copying messages from one topic to another
- `03-example-filter.yaml`: Shows how to filter messages based on their content

### Step 3: Modify an Example

Try modifying one of the examples to see how changes affect the behavior. For instance, you could change the filter condition in `03-example-filter.yaml` to filter based on a different field or value.

1. Edit the file using your favorite text editor
2. Restart the KSML runner to apply your changes:
   ```bash
   docker compose restart ksml-runner
   ```
3. Check the logs to see the effect of your changes:
   ```bash
   docker compose logs -f ksml-runner
   ```

## Troubleshooting

### Common Issues

- **Connection refused errors**: Make sure all the Docker containers are running with `docker compose ps`
- **Schema not found errors**: Ensure the Schema Registry is running and accessible
- **KSML syntax errors**: Check your YAML syntax for proper indentation and formatting

### Getting Help

If you encounter issues not covered here:

- Check the [Troubleshooting Guide](../resources/troubleshooting.md) for more detailed solutions
- Visit the [Community and Support](../resources/community.md) page for information on how to get help from the KSML community

## Next Steps

Now that you have KSML up and running, you can:

- Follow the [KSML Basics Tutorial](basics-tutorial.md) to learn how to build your first KSML pipeline from scratch
- Explore the [Core Concepts](../../reference/stream-types-reference.md) to deepen your understanding of KSML
- Browse the [Examples Library](../resources/examples-library.md) for inspiration and ready-to-use patterns