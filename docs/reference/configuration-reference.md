# Configuration Reference

This document provides a comprehensive reference for configuring the KSML Runner application through the `ksml-runner.yaml` configuration file.

## Configuration File Structure

The KSML Runner configuration consists of two main sections:

```yaml
ksml:
  # KSML-specific configuration
  configDirectory: .
  definitions:
    my_app: app-definition.yaml

kafka:
  # Kafka client configuration (standard Kafka properties)
  bootstrap.servers: "localhost:9092"
  application.id: "my-ksml-app"
```

## Minimal Working Configurations

### Without Schema Registry

For applications that don't use schema-based formats (Avro, Protobuf, JSON Schema):

??? info "Minimal Configuration - No Schema Registry (click to expand)"

    ```yaml
    ksml:
      # Section where you specify which KSML definitions to load, parse and execute.
      definitions:
        # Format is <namespace>: <ksml_definition_filename>
        producer: producer.yaml
        processor: processor.yaml
      storageDirectory: /ksml/state  # Directory for state stores (inside /ksml which is writable because /ksml is the mounted working directory)
      createStorageDirectory: true    # Create the directory if it doesn't exist

    # This setup connects to the Kafka broker and schema registry started with the example docker-compose file
    # These examples are intended to run from a inside a container on the same network
    kafka:
      bootstrap.servers: broker:9093
      application.id: io.ksml.example.producer
      security.protocol: PLAINTEXT
      acks: all
    ```

### With Schema Registry

For applications using schema-based formats (Avro, Protobuf, JSON Schema):

??? info "Minimal Configuration - With Schema Registry (click to expand)"

    ```yaml
    ksml:
      definitions:
        producer: producer.yaml
        processor: processor.yaml
      schemaRegistries:
        my_schema_registry:
          config:
            schema.registry.url: http://schema-registry:8081/apis/ccompat/v7
      notations:
        avro:  
          type: confluent_avro         # For Avro there are two implementations: apicurio_avro and confluent_avro
          schemaRegistry: my_schema_registry
          ## Below this line, specify properties to be passed into Confluent's KafkaAvroSerializer and KafkaAvroDeserializer
          config:
            normalize.schemas: true
            auto.register.schemas: true

    kafka:
      bootstrap.servers: broker:9093
      application.id: io.ksml.example.producer
      security.protocol: PLAINTEXT
      acks: all
    ```

## KSML Configuration Section

The `ksml` section contains all configuration specific to the KSML Runner application:

### Directory Configuration

| Property               | Type    | Default                    | Description                                                    |
|------------------------|---------|----------------------------|----------------------------------------------------------------|
| `configDirectory`      | String  | Working directory          | Directory containing KSML definition files                    |
| `schemaDirectory`      | String  | Same as configDirectory    | Directory containing schema files                              |
| `storageDirectory`     | String  | System temp directory      | Directory for Kafka Streams state stores                      |
| `createStorageDirectory` | Boolean | false                    | Create storage directory if it doesn't exist                  |

```yaml
ksml:
  configDirectory: .
  schemaDirectory: ./schemas
  storageDirectory: /tmp/kafka-streams
  createStorageDirectory: true
```

### Application Server Configuration

Enables a REST API for state store queries and health checks:

| Property  | Type    | Default   | Description                              |
|-----------|---------|-----------|------------------------------------------|
| `enabled` | Boolean | false     | Enable/disable the REST server           |
| `host`    | String  | 0.0.0.0   | IP address to bind the server to         |
| `port`    | Integer | 8080      | Port number for the REST API             |

```yaml
ksml:
  applicationServer:
    enabled: true
    host: 0.0.0.0
    port: 8080
```

### Prometheus Metrics Configuration

Enables Prometheus metrics endpoint:

| Property  | Type    | Default   | Description                              |
|-----------|---------|-----------|------------------------------------------|
| `enabled` | Boolean | false     | Enable/disable Prometheus metrics        |
| `host`    | String  | 0.0.0.0   | IP address to bind the metrics server to |
| `port`    | Integer | 9999      | Port number for metrics endpoint         |

```yaml
ksml:
  prometheus:
    enabled: true
    host: 0.0.0.0
    port: 9999
```

### Error Handling Configuration

Configure how different types of errors are handled:

Each error type (`consume`, `process`, `produce`) supports these properties:

| Property     | Type    | Default | Description                                           |
|--------------|---------|---------|-------------------------------------------------------|
| `log`        | Boolean | true    | Whether to log errors                                 |
| `logPayload` | Boolean | false   | Whether to include message payload in error logs     |
| `loggerName` | String  | Auto-generated | Custom logger name for this error type               |
| `handler`    | String  | stop    | Error handling strategy (`continueOnFail`, `stopOnFail`, `retryOnFail`) |

```yaml
ksml:
  errorHandling:
    consume:
      log: true
      logPayload: false
      loggerName: ConsumeError
      handler: stopOnFail
    process:
      log: true
      logPayload: false
      loggerName: ProcessError
      handler: continueOnFail
    produce:
      log: true
      logPayload: false
      loggerName: ProduceError
      handler: continueOnFail
```

### Feature Enablement

Control which KSML features are enabled:

| Property          | Type    | Default | Description                                    |
|-------------------|---------|---------|------------------------------------------------|
| `enableProducers` | Boolean | true    | Enable producer definitions in KSML files     |
| `enablePipelines` | Boolean | true    | Enable pipeline definitions in KSML files     |

```yaml
ksml:
  enableProducers: false  # Disable producers for pipeline-only applications
  enablePipelines: true
```

### Python Context Configuration

Control Python execution security and permissions:

| Property                      | Type    | Default | Description                                       |
|-------------------------------|---------|---------|---------------------------------------------------|
| `allowHostFileAccess`         | Boolean | false   | Allow Python code to access host file system      |
| `allowHostSocketAccess`       | Boolean | false   | Allow Python code to open network sockets         |
| `allowNativeAccess`           | Boolean | false   | Allow Graal native access / JNI                   |
| `allowCreateProcess`          | Boolean | false   | Allow Python code to execute external processes   |
| `allowCreateThread`           | Boolean | false   | Allow Python code to create new Java threads      |
| `inheritEnvironmentVariables` | Boolean | false   | Inherit JVM process environment in Python context |
| `pythonModulePath`            | String  | empty   | Path to customer defined Python modules           |

```yaml
ksml:
  pythonContext:
    allowHostFileAccess: false
    allowHostSocketAccess: false
    allowNativeAccess: false
    allowCreateProcess: false
    allowCreateThread: false
    inheritEnvironmentVariables: false
    pythonModulePath: /ksml
```

### Schema Registry Configuration

Configure connections to schema registries:

#### Basic Configuration

```yaml
ksml:
  schemaRegistries:
    # Confluent Schema Registry
    confluent:
      config:
        schema.registry.url: "http://schema-registry:8081"

    # Apicurio Schema Registry
    apicurio:
      config:
        apicurio.registry.url: "http://apicurio:8080/apis/registry/v2"
```

#### SSL-Enabled Schema Registry

```yaml
ksml:
  schemaRegistries:
    confluent_secure:
      config:
        schema.registry.url: "https://schema-registry:8081"
        schema.registry.ssl.protocol: TLSv1.3
        schema.registry.ssl.keystore.location: /path/to/keystore.jks
        schema.registry.ssl.keystore.type: JKS
        schema.registry.ssl.keystore.password: "${KEYSTORE_PASSWORD}"
        schema.registry.ssl.truststore.location: /path/to/truststore.jks
        schema.registry.ssl.truststore.type: JKS
        schema.registry.ssl.truststore.password: "${TRUSTSTORE_PASSWORD}"

    apicurio_secure:
      config:
        apicurio.registry.url: "https://apicurio:8080/apis/registry/v2"
        apicurio.registry.request.ssl.keystore.location: /path/to/keystore.jks
        apicurio.registry.request.ssl.keystore.type: JKS
        apicurio.registry.request.ssl.keystore.password: "${KEYSTORE_PASSWORD}"
        apicurio.registry.request.ssl.truststore.location: /path/to/truststore.jks
        apicurio.registry.request.ssl.truststore.type: JKS
        apicurio.registry.request.ssl.truststore.password: "${TRUSTSTORE_PASSWORD}"
```

### Notation Configuration

Configure data format serializers and deserializers. Each notation entry defines:

| Property         | Type   | Required | Description                                    |
|------------------|--------|----------|------------------------------------------------|
| `type`           | String | Yes      | Serializer implementation type                 |
| `schemaRegistry` | String | No       | Schema registry to use (if applicable)        |
| `config`         | Object | No       | Additional properties for the serializer      |

```yaml
ksml:
  notations:
    # Avro with Confluent
    avro:
      type: confluent_avro
      schemaRegistry: confluent
      config:
        normalize.schemas: true
        auto.register.schemas: false

    # Avro with Apicurio
    apicurio_avro:
      type: apicurio_avro
      schemaRegistry: apicurio
      config:
        apicurio.registry.auto-register: true

    # JSON Schema with Apicurio
    jsonschema:
      type: apicurio_jsonschema
      schemaRegistry: apicurio
      config:
        apicurio.registry.auto-register: true

    # Protobuf with Apicurio
    protobuf:
      type: apicurio_protobuf
      schemaRegistry: apicurio
      config:
        apicurio.registry.auto-register: false
```

Available serializer types:

| Serializer Type          | Notation  | Schema Registry | Description                    |
|--------------------------|-----------|-----------------|--------------------------------|
| `confluent_avro`         | avro      | Confluent       | Avro with Confluent SR         |
| `apicurio_avro`          | avro      | Apicurio        | Avro with Apicurio SR          |
| `confluent_jsonschema`   | jsonschema| Confluent       | JSON Schema with Confluent SR  |
| `apicurio_jsonschema`    | jsonschema| Apicurio        | JSON Schema with Apicurio SR   |
| `confluent_protobuf`     | protobuf  | Confluent       | Protobuf with Confluent SR     |
| `apicurio_protobuf`      | protobuf  | Apicurio        | Protobuf with Apicurio SR      |

Built-in serializers (no configuration needed):

- `json`: Schemaless JSON
- `csv`: Comma-separated values
- `xml`: XML format
- `soap`: SOAP messages

### KSML Definition Loading

Specify which KSML definition files to load and execute:

```yaml
ksml:
  definitions:
    # Format: <namespace>: <filename>
    my_producer: producer-definition.yaml
    my_processor: processor-definition.yaml
    order_pipeline: order-processing.yaml
```

### Schema File Loading

Specify schema files to load (Avro, JSON Schema, XSD, CSV schemas):

```yaml
ksml:
  schemas:
    # Format: <name>: <filename>
    SensorData.avsc: SensorData.avsc
    SensorData.json: SensorData.json
    SensorData.xsd: SensorData.xsd
    SensorData.csv: SensorData.csv
```

## Kafka Configuration Section

The `kafka` section contains standard Kafka client configuration properties. All Kafka Streams and Kafka client properties are supported.

### Essential Properties

| Property              | Type   | Required | Description                                    |
|-----------------------|--------|----------|------------------------------------------------|
| `bootstrap.servers`   | String | Yes      | Comma-separated list of Kafka brokers         |
| `application.id`      | String | Yes      | Unique identifier for the Kafka Streams app   |

!!! note "Application ID Aliases"
    You can use any of these property names for the application ID:
    - `application.id` (standard Kafka property)
    - `applicationId` (camelCase variant)  
    - `app.id` (KSML shorthand)

### Common Properties

| Property              | Type   | Default   | Description                                    |
|-----------------------|--------|-----------|------------------------------------------------|
| `group.instance.id`   | String | -         | Static member ID for faster rebalancing       |
| `security.protocol`   | String | PLAINTEXT | Security protocol (PLAINTEXT, SSL, SASL_*)    |
| `auto.offset.reset`   | String | latest    | Offset reset policy (earliest, latest, none)  |
| `acks`               | String | 1         | Producer acknowledgment mode                    |

```yaml
kafka:
  # Essential configuration
  bootstrap.servers: "kafka1:9092,kafka2:9092,kafka3:9092"
  application.id: "my-ksml-application"
  group.instance.id: "instance-1"

  # Security configuration
  security.protocol: PLAINTEXT
  auto.offset.reset: earliest
  acks: all

  # Performance tuning
  batch.size: 16384
  linger.ms: 5
  buffer.memory: 33554432
```

### SSL Configuration

For secure connections to Kafka:

```yaml
kafka:
  security.protocol: SSL
  ssl.protocol: TLSv1.3
  ssl.enabled.protocols: TLSv1.3,TLSv1.2
  ssl.endpoint.identification.algorithm: ""
  ssl.keystore.type: JKS
  ssl.keystore.location: /path/to/ksml.keystore.jks
  ssl.keystore.password: "${KEYSTORE_PASSWORD}"
  ssl.key.password: "${KEY_PASSWORD}"
  ssl.truststore.type: JKS
  ssl.truststore.location: /path/to/ksml.truststore.jks
  ssl.truststore.password: "${TRUSTSTORE_PASSWORD}"
```

### SASL Configuration

For SASL authentication:

```yaml
kafka:
  security.protocol: SASL_SSL
  sasl.mechanism: PLAIN
  sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USERNAME}\" password=\"${KAFKA_PASSWORD}\";"
```

### Axual Platform Configuration

For Axual platform with naming patterns:

```yaml
kafka:
  # Axual naming patterns
  axual.topic.pattern: "{tenant}-{instance}-{environment}-{topic}"
  axual.group.id.pattern: "{tenant}-{instance}-{environment}-{group.id}"
  axual.transactional.id.pattern: "{tenant}-{instance}-{environment}-{transactional.id}"
  
  # Pattern variables
  tenant: "mytenant"
  instance: "myinstance"
  environment: "dev"
```

## Environment Variable Substitution

KSML supports environment variable substitution in configuration values using `${VARIABLE_NAME}` syntax:

```yaml
kafka:
  bootstrap.servers: "${KAFKA_BROKERS}"
  application.id: "${APP_ID:-default-app-id}"  # With default value
  
ksml:
  schemaRegistries:
    confluent:
      config:
        schema.registry.url: "${SCHEMA_REGISTRY_URL}"
        schema.registry.ssl.keystore.password: "${KEYSTORE_PASSWORD}"
```

## Complete Configuration Example

```yaml
ksml:
  configDirectory: /app/definitions
  schemaDirectory: /app/schemas
  storageDirectory: /app/state-stores
  createStorageDirectory: true

  applicationServer:
    enabled: true
    host: 0.0.0.0
    port: 8080

  prometheus:
    enabled: true
    host: 0.0.0.0
    port: 9999

  errorHandling:
    consume:
      log: true
      logPayload: false
      loggerName: ConsumeError
      handler: stopOnFail
    process:
      log: true
      logPayload: true
      loggerName: ProcessError
      handler: continueOnFail
    produce:
      log: true
      logPayload: true
      loggerName: ProduceError
      handler: retryOnFail

  enableProducers: true
  enablePipelines: true

  pythonContext:
    allowHostFileAccess: false
    allowHostSocketAccess: false
    allowNativeAccess: false
    allowCreateProcess: false
    allowCreateThread: false
    inheritEnvironmentVariables: false

  schemaRegistries:
    confluent:
      config:
        schema.registry.url: "${SCHEMA_REGISTRY_URL}"

  notations:
    avro:
      type: confluent_avro
      schemaRegistry: confluent
      config:
        normalize.schemas: true
        auto.register.schemas: false

  definitions:
    order_processor: order-processing.yaml
    user_events: user-event-pipeline.yaml

  schemas:
    UserProfile.avsc: UserProfile.avsc
    OrderSchema.json: OrderSchema.json

kafka:
  bootstrap.servers: "${KAFKA_BROKERS}"
  application.id: "${APP_ID}"
  group.instance.id: "${INSTANCE_ID}"
  security.protocol: "${SECURITY_PROTOCOL:-PLAINTEXT}"
  auto.offset.reset: earliest
  acks: all
```

## Best Practices

### Configuration Management

- Use environment variables for environment-specific values (URLs, passwords, IDs)
- Keep sensitive information out of configuration files
- Use different configuration files for different environments (dev, staging, prod)
- Version control your configuration files (excluding secrets)

### Security

- Use environment variables for sensitive information like passwords and API keys
- Enable SSL/TLS for production environments  
- Use proper authentication mechanisms (SASL_PLAIN, SASL_SSL, SCRAM, etc.)
- Store certificates and keystores securely
- Regularly rotate passwords and certificates

### Performance

- Set appropriate batch sizes and linger times for producers
- Configure adequate buffer memory for high-throughput scenarios
- Use static member IDs (`group.instance.id`) for faster rebalancing
- Monitor storage directory disk usage for stateful applications
- Choose appropriate serialization formats (Avro for efficiency, JSON for debugging)

### Operational

- Set unique `application.id` for each KSML application
- Enable application server for health checks and state store queries
- Enable Prometheus metrics for monitoring and alerting
- Configure appropriate error handling strategies for your use case
- Use descriptive application IDs and instance IDs
- Regularly backup state store directories for stateful applications
- Plan for disaster recovery and state store restoration

### Development

- Start with minimal configuration and add complexity as needed
- Use `logPayload: true` during development for debugging (disable in production)
- Test with different error scenarios to validate error handling configuration
- Use schema registries to enforce data contracts between services
- Use `continueOnFail` for non-critical processing errors during testing
