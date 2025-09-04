# Advanced Installation Guide

This guide covers advanced installation scenarios for KSML. **Note: These steps are optional if you're using the Docker Compose setup from the [Quick Start](quick-start.md).**

## Building from Source (Optional)

**⚠️ You only need this if you want to:**
- Build KSML from source code
- Contribute to the KSML project
- Use a development version

**If you're just getting started with KSML, use the [Quick Start guide](quick-start.md) instead.**

## To build from source:

### Prerequisites

- **Git**: To clone the KSML repository
- **Docker**: For running the build environment
- **Java 21**: For building the KSML runner (if building locally)

### Step 1: Clone the Repository (Optional)

**⚠️ Only do this if you need to build from source or modify KSML itself.**

Clone the KSML repository:

```bash
git clone https://github.com/axual/ksml.git
cd ksml
```

### Step 2: Build the Project

Build the KSML runner:

```bash
# Using Docker (recommended)
docker build -t ksml-runner .

# Or build locally (requires Java 21)
./gradlew build
```

### Step 3: Run the Development Environment

Start the full development environment:

```bash
docker compose up -d
```

This includes:

- Zookeeper
- Kafka broker
- Schema Registry
- Example data producer
- KSML runner

### Step 4: Run Example Applications

After building, you can run the example KSML applications:

```bash
./examples/run-local.sh
```

You should see output indicating the KSML runner has started and is processing example data.

### Step 5: Explore the Examples

The example KSML definitions are located in the `examples` directory:

- `01-example-inspect.yaml`: Shows how to read and log messages from a topic
- `02-example-copy.yaml`: Demonstrates copying messages from one topic to another
- `03-example-filter.yaml`: Shows how to filter messages based on their content

### Step 6: Modify and Test Examples

Try modifying one of the examples:

1. Edit the file using your favorite text editor
2. Restart the KSML runner to apply your changes:
   ```bash
   docker compose restart ksml-runner
   ```
3. Check the logs to see the effect of your changes:
   ```bash
   docker compose logs -f ksml-runner
   ```

## Connecting to an Existing Kafka Cluster

If you already have a Kafka cluster and want to use KSML with it:

### Step 1: Create a Configuration File

Create a file named `ksml-runner.yaml` with your Kafka cluster configuration:

```yaml
ksml:
  configDir: "/config"
  definitionDir: "/definitions"

kafka:
  bootstrap.servers: "your-kafka-broker:9092"
  application.id: "ksml-runner"
  default.key.serde: "org.apache.kafka.common.serialization.Serdes$StringSerde"
  default.value.serde: "org.apache.kafka.common.serialization.Serdes$StringSerde"
  
  # Add any additional Kafka Streams configuration here
  # security.protocol: "SASL_SSL"
  # sasl.mechanism: "PLAIN"
  # sasl.jaas.config: "..."
```

### Step 2: Run the KSML Runner

Run the KSML runner with your configuration:

```bash
docker run -v $(pwd)/config:/config \
           -v $(pwd)/definitions:/definitions \
           -v $(pwd)/ksml-runner.yaml:/app/ksml-runner.yaml \
           registry.axual.io/opensource/images/axual/ksml:1.0.8 \
           --config /app/ksml-runner.yaml
```

## Running in Production

### Using Docker Compose in Production

For production deployments, create a production-ready docker-compose.yml:

```yaml
version: '3.8'
services:
  ksml:
    image: registry.axual.io/opensource/images/axual/ksml:1.0.8
    restart: unless-stopped
    volumes:
      - ./definitions:/definitions
      - ./config:/config
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=your-kafka-cluster:9092
    deploy:
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M
```

### Using Kubernetes

For Kubernetes deployments, create appropriate ConfigMaps and Deployments:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ksml-runner
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ksml-runner
  template:
    metadata:
      labels:
        app: ksml-runner
    spec:
      containers:
      - name: ksml-runner
        image: registry.axual.io/opensource/images/axual/ksml:1.0.8
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "your-kafka-cluster:9092"
        volumeMounts:
        - name: definitions
          mountPath: /definitions
        - name: config
          mountPath: /config
      volumes:
      - name: definitions
        configMap:
          name: ksml-definitions
      - name: config
        configMap:
          name: ksml-config
```

## Security Configuration

### SSL/TLS Configuration

To connect to a secured Kafka cluster:

```yaml
kafka:
  bootstrap.servers: "secure-kafka:9093"
  security.protocol: "SSL"
  ssl.truststore.location: "/config/kafka.client.truststore.jks"
  ssl.truststore.password: "truststore-password"
  ssl.keystore.location: "/config/kafka.client.keystore.jks"
  ssl.keystore.password: "keystore-password"
  ssl.key.password: "key-password"
```

### SASL Authentication

For SASL authentication:

```yaml
kafka:
  bootstrap.servers: "sasl-kafka:9093"
  security.protocol: "SASL_SSL"
  sasl.mechanism: "SCRAM-SHA-256"
  sasl.jaas.config: "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"your-username\" password=\"your-password\";"
```

## Monitoring and Observability

### JMX Metrics

Enable JMX metrics for monitoring:

```bash
docker run -p 9999:9999 \
           -e JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
           registry.axual.io/opensource/images/axual/ksml:1.0.8
```

### Logging Configuration

Configure logging levels:

```yaml
logging:
  level:
    io.axual.ksml: DEBUG
    org.apache.kafka: INFO
```

## Next Steps

Once you have KSML set up in your environment:

- Return to the [Installation Guide](quick-start.md) for the quick start
- Follow the [KSML Basics Tutorial](basics-tutorial.md) to learn KSML concepts
- Explore the [KSML Definition Reference](../reference/definition-reference.md) documentation