# KSML Definition Reference

This document provides a comprehensive reference for the syntax with which developers can set up their own KSML
definitions. Each section is described with its purpose, available options, and examples.

## KSML Definition File Structure

A KSML definition typically consists of the following main sections:

```yaml
# Basic metadata
name: "my-ksml-application"
version: "1.0.0"
description: "My KSML Application"

streams:
  # Stream definitions
tables:
  # Table definitions
globalTables:
  # Global table definitions

stores:
  # State store definitions

functions:
  # Function definitions

pipelines:
  # Pipeline definitions
```

## Application Configuration

The `config` section contains application-level configuration options.

### Kafka Configuration

| Property              | Type   | Required | Description                                                |
|-----------------------|--------|----------|------------------------------------------------------------|
| `bootstrap.servers`   | String | Yes      | Comma-separated list of Kafka broker addresses             |
| `application.id`      | String | Yes      | The unique identifier for the Kafka Streams application    |
| `client.id`           | String | No       | The client identifier                                      |
| `auto.offset.reset`   | String | No       | Default offset reset policy (`earliest`, `latest`, `none`) |
| `schema.registry.url` | String | No       | The URL of the schema registry                             |

Example:

```yaml
config:
  kafka:
    bootstrap.servers: "kafka1:9092,kafka2:9092,kafka3:9092"
    application.id: "order-processing-app"
    client.id: "order-processing-client"
    auto.offset.reset: "earliest"
    schema.registry.url: "http://schema-registry:8081"
```

### State Store Configuration

| Property                    | Type    | Required | Description                            |
|-----------------------------|---------|----------|----------------------------------------|
| `state.dir`                 | String  | No       | The directory for state stores         |
| `cache.max.bytes.buffering` | Integer | No       | The maximum size of the cache in bytes |
| `commit.interval.ms`        | Integer | No       | The commit interval in milliseconds    |

Example:

```yaml
config:
  state:
    state.dir: "/tmp/kafka-streams"
    cache.max.bytes.buffering: 10485760  # 10 MB
    commit.interval.ms: 30000  # 30 seconds
```

### Logging Configuration

| Property  | Type   | Required | Description                                      |
|-----------|--------|----------|--------------------------------------------------|
| `level`   | String | No       | The log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `file`    | String | No       | The log file path                                |
| `pattern` | String | No       | The log pattern                                  |

Example:

```yaml
config:
  logging:
    level: "INFO"
    file: "/var/log/ksml/application.log"
    pattern: "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"
```

### Metrics Configuration

| Property      | Type    | Required | Description                                    |
|---------------|---------|----------|------------------------------------------------|
| `reporters`   | Array   | No       | The metrics reporters to use                   |
| `interval.ms` | Integer | No       | The metrics reporting interval in milliseconds |

Example:

```yaml
config:
  metrics:
    reporters:
      - type: "jmx"
      - type: "prometheus"
        port: 8080
    interval.ms: 60000  # 60 seconds
```

### Security Configuration

| Property   | Type   | Required | Description                                                              |
|------------|--------|----------|--------------------------------------------------------------------------|
| `protocol` | String | No       | The security protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) |
| `ssl`      | Object | No       | SSL configuration                                                        |
| `sasl`     | Object | No       | SASL configuration                                                       |

Example:

```yaml
config:
  security:
    protocol: "SASL_SSL"
    ssl:
      truststore.location: "/etc/kafka/ssl/kafka.truststore.jks"
      truststore.password: "${TRUSTSTORE_PASSWORD}"
    sasl:
      mechanism: "PLAIN"
      jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USERNAME}\" password=\"${KAFKA_PASSWORD}\";"
```

### Environment Variables

KSML supports environment variable substitution in configuration values.

Example:

```yaml
config:
  kafka:
    bootstrap.servers: "${KAFKA_BOOTSTRAP_SERVERS}"
    application.id: "${APPLICATION_ID:-order-processing-app}"  # Default value if not set
```

## Advanced Configuration

### Custom Serializers and Deserializers

| Property             | Type   | Required | Description                  |
|----------------------|--------|----------|------------------------------|
| `key.serializer`     | String | No       | The key serializer class     |
| `key.deserializer`   | String | No       | The key deserializer class   |
| `value.serializer`   | String | No       | The value serializer class   |
| `value.deserializer` | String | No       | The value deserializer class |

Example:

```yaml
config:
  serialization:
    key.serializer: "org.apache.kafka.common.serialization.StringSerializer"
    key.deserializer: "org.apache.kafka.common.serialization.StringDeserializer"
    value.serializer: "io.confluent.kafka.serializers.KafkaAvroSerializer"
    value.deserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer"
```

### Custom State Stores

| Property | Type   | Required | Description                                         |
|----------|--------|----------|-----------------------------------------------------|
| `name`   | String | Yes      | The name of the state store                         |
| `type`   | String | Yes      | The type of state store (`persistent`, `in-memory`) |
| `config` | Object | No       | Additional configuration for the state store        |

Example:

```yaml
config:
  stateStores:
    - name: "order-store"
      type: "persistent"
      config:
        retention.ms: 604800000  # 7 days
        cleanup.policy: "compact"
```

### Processing Guarantees

| Property               | Type   | Required | Description                                                                   |
|------------------------|--------|----------|-------------------------------------------------------------------------------|
| `processing.guarantee` | String | No       | The processing guarantee (`at_least_once`, `exactly_once`, `exactly_once_v2`) |

Example:

```yaml
config:
  processing:
    processing.guarantee: "exactly_once_v2"
```

## Best Practices

1. **Use environment variables for sensitive information**: Avoid hardcoding sensitive information like passwords
2. **Set appropriate retention periods for state stores**: Consider your application's requirements and available disk
   space
3. **Configure appropriate commit intervals**: Balance between throughput and recovery time
4. **Use descriptive names for streams, tables, and functions**: Make your KSML definitions self-documenting
5. **Set appropriate log levels**: Use `INFO` for production and `DEBUG` for development
6. **Monitor your application**: Configure metrics reporters to track your application's performance
7. **Use exactly-once processing guarantees for critical applications**: Ensure data integrity for important
   applications

## Related Topics

- [KSML Language Reference](language-reference.md)
- [Operations Reference](operation-reference.md)
- [Functions Reference](function-reference.md)
- [Data Types Reference](data-types-reference.md)