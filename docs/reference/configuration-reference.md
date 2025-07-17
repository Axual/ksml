# KSML Configuration Reference

This document provides a comprehensive reference for all configuration options available in KSML. Each configuration
section is described with its purpose, available options, and examples.

## KSML Configuration File Structure

The KSML configuration is usually stored in a file called `ksml-runner.yaml`. It contains the following main sections:

```yaml
ksml:
  # Configuration related to KSML itself
   
kafka:
  # Configuration required to connect to Kafka
```

The `ksml` section contains all configuration that KSML needs to understand what it needs to do. The `kafka` section
contains the relevant configuration to connect to allow Kafka Streams to connect to Kafka. All the normal Kafka Streams
connection properties can be configured here.

### Template `ksml-runner.yaml`

Below is a template `ksml-runner.yaml` that you can copy-paste for your own setup and fill out with your own relevant
configuration. The entries are explained in the next sections.

```yaml
ksml:
  # The examples directory is mounted to /ksml in the Docker container
  configDirectory: .                 # When not set defaults to the working directory
  schemaDirectory: .                 # When not set defaults to the config directory
  storageDirectory: /tmp             # When not set defaults to the default JVM temp directory

  # This section defines if a REST endpoint is opened on the KSML runner, through which
  # state stores and/or readiness / liveness probes can be accessed.
  applicationServer:
    enabled: false                   # Set to true to enable, or false to disable
    host: 0.0.0.0                    # IP address to bind the REST server to
    port: 8080                       # Port number to listen on

  # This section defines whether a Prometheus endpoint is opened to allow metric scraping.
  prometheus:
    enabled: false                   # Set to true to enable, or false to disable
    host: 0.0.0.0                    # IP address to bind the Prometheus agent server to
    port: 9999                       # Port number to listen on

  # This section enables error handling or error ignoring for certain types of errors.
  errorHandling:
    consume:                         # Error handling definitions for consume errors
      log: true                      # Log errors true/false
      logPayload: true               # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ConsumeError       # Definition of the error logger name.
      handler: stopOnFail            # How to proceed after encountering the error. Either continueOnFail or stopOnFail.
    process:
      log: true                      # Log errors true/false
      logPayload: true               # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ProcessError       # Definition of the error logger name.
      handler: continueOnFail        # How to proceed after encountering the error. Either continueOnFail or stopOnFail.
    produce:
      log: true                      # Log errors true/false
      logPayload: true               # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ProduceError       # Definition of the error logger name.
      handler: continueOnFail        # How to proceed after encountering the error. Either continueOnFail or stopOnFail.

  enableProducers: true              # Set to true to allow producer definitions to be parsed in the KSML definitions and be executed.
  enablePipelines: false             # Set to true to allow pipeline definitions to be parsed in the KSML definitions and be executed.

  # This section tells KSML which schema registries are available
  schemaRegistries:
    # Definition of an Apicurio schema registry
    apicurio:
      config:
        # Below is an example SSL configuration for Apicurio Serialization library from Apicurio documentation
        # apicurio.registry.request.ssl.keystore.location: /path/to/keystore.jks
        # apicurio.registry.request.ssl.keystore.type: JKS
        # apicurio.registry.request.ssl.keystore.password: password
        # apicurio.registry.request.ssl.key.password: password
        # apicurio.registry.request.ssl.truststore.location: /path/to/truststore.jks
        # apicurio.registry.request.ssl.truststore.type: JKS
        # apicurio.registry.request.ssl.truststore.password: password

    # Definition of a Confluent schema registry
    confluent:
      config:
        # Example SSL configuration for Confluent schema registry
        # schema.registry.ssl.protocol: TLSv1.3
        # schema.registry.ssl.enabled.protocols: TLSv1.3,TLSv1.2
        # schema.registry.ssl.endpoint.identification.algorithm: ""
        # schema.registry.ssl.keystore.location: /path/to/keystore.jks
        # schema.registry.ssl.keystore.type: JKS
        # schema.registry.ssl.key.password: password
        # schema.registry.ssl.keystore.password: password
        # schema.registry.ssl.truststore.location: /path/to/truststore.jks
        # schema.registry.ssl.truststore.type: JKS
        # schema.registry.ssl.truststore.password: password

  # This section tells KSML which serializers / deserializers handle which notation types
  notations:
    # Definition for "avro" notation
    avro:
      serde: confluent_avro          # For AVRO there are two implementations: apicurio_avro and confluent_avro
      schemaRegistry: confluent
      config:
        # Specify all properties to be passed into Confluent's KafkaAvroSerializer and KafkaAvroDeserializer
        normalize.schemas: true
        auto.register.schemas: false

    # Definition for "second_avro" notation. With this entry, you can use a type like "second_avro:SchemaName" in your
    # KSML definition, which then uses the Apicurio implementation for AVRO.
    second_avro:
      serde: apicurio_avro           # For AVRO there are two implementations: apicurio_avro and confluent_avro
      schemaRegistry: apicurio
      config:
        # Specify all properties to be passed into Apicurio's AvroKafkaSerializer and AvroKafkaDeserializer
        # apicurio.registry.avro.encoding: "BINARY"
        # apicurio.registry.headers.enabled: "true"
        # apicurio.registry.serde.IdHandler: "io.apicurio.registry.serde.Default4ByteIdHandler"
        # apicurio.registry.schema-resolver: "io.apicurio.registry.resolver.DefaultSchemaResolver"

    # Definition for "jsonschema" notation
    jsonschema:
      serde: apicurio_jsonschema     # For JSON Schema there are two implementations: apicurio_jsonschema and confluent_jsonschema
      schemaRegistry: apicurio
      config:
        # Specify all properties to be passed into Apicurio's JsonSchemaKafkaSerializer and JsonSchemaKafkaDeserializer
        apicurio.registry.auto-register: true

    # Definition for "protobuf" notation
    protobuf:                        # Definition for "protobuf" notation
      serde: apicurio_protobuf       # For Protobuf there are two implementations: apicurio_protobuf and confluent_protobuf
      schemaRegistry: apicurio
      config:
        # Specify all properties to be passed into Apicurio's ProtobufKafkaSerializer and ProtobufKafkaDeserializer
        apicurio.registry.auto-register: false

  # Section where you specify which KSML definitions to load, parse and execute.
  definitions:
    # Format is <namespace>: <ksml_definition_filename>
    generate_alert_setting: 00-example-generate-alertsettings.yaml
    generate_sensor_data_avro: 00-example-generate-sensordata-avro.yaml
    # generate_sensor_data_avro_batch: 00-example-generate-sensordata-avro-batch.yaml
    # generate_sensor_data_binary: 00-example-generate-sensordata-binary.yaml
    # generate_sensor_data_protobuf: 00-example-generate-sensordata-protobuf.yaml

    inspect: 01-example-inspect.yaml
    copy: 02-example-copy.yaml
    filter: 03-example-filter.yaml
    # branch: 04-example-branch.yaml
    # route: 05-example-route.yaml
    # duplicate: 06-example-duplicate.yaml
    # convert: 07-example-convert.yaml
    # count: 08-example-count.yaml
    # aggregate: 09-example-aggregate.yaml
    # queryable_table: 10-example-queryable-table.yaml
    # field_modification: 11-example-field-modification.yaml
    # byte_manipulation: 12-example-byte-manipulation.yaml
    # join: 13-example-join.yaml
    # manual_state_store: 14-example-manual-state-store.yaml
    # pipeline_linking: 15-example-pipeline-linking.yaml
    # transform_metadata: 16-example-transform-metadata.yaml
    # inspect_with_metrics: 17-example-inspect-with-metrics.yaml
    # timestamp_extractor: 18-example-timestamp-extractor.yaml
    # performance-measurement: 19-example-performance-measurement.yaml

# This setup connects to the Kafka broker and schema registry started with the example docker-compose file
# These examples are intended to run from inside a container on the same network
kafka:
  bootstrap.servers: broker:9093
  application.id: io.ksml.example.producer
  security.protocol: PLAINTEXT
  acks: all

  # These are Kafka SSL configuration properties. Check the documentation at1
  # Check the documentation at https://kafka.apache.org/documentation/#producerconfigs for more properties
  # security.protocol: SSL
  # ssl.protocol: TLSv1.3
  # ssl.enabled.protocols: TLSv1.3,TLSv1.2
  # ssl.endpoint.identification.algorithm: ""
  # ssl.keystore.type: JKS
  # ssl.truststore.type: JKS
  # ssl.key.password: xxx
  # ssl.keystore.password: xxx
  # ssl.keystore.location: /path/to/ksml.keystore.jks
  # ssl.truststore.password: xxx
  # ssl.truststore.location: /path/to/ksml.truststore.jks

  # Use these configuration properties when connecting to a cluster using the Axual naming patterns.
  # These patterns are resolved into the actual name used on Kafka using the values in this configuration map
  # and the topic names specified in the definition YAML files
  # The pattern below results in Kafka topic ksmldemo-dta-dev-<topic name from KSML definition YAML>
  # axual.topic.pattern: "{tenant}-{instance}-{environment}-{topic}"
  # axual.group.id.pattern: "{tenant}-{instance}-{environment}-{group.id}"
  # axual.transactional.id.pattern: "{tenant}-{instance}-{environment}-{transactional.id}"
  # tenant: "ksmldemo"
  # instance: "dta"
  # environment: "dev"
```

### KSML Configuration

The `ksml` section in the configuration contains the following items:


```yaml
ksml:
  # The examples directory is mounted to /ksml in the Docker container
  configDirectory: .              # When not set defaults to the working directory
  schemaDirectory: .              # When not set defaults to the config directory
  storageDirectory: /tmp          # When not set defaults to the default JVM temp directory
  createStorageDirectory: false   # When set to true, the storage directory will be created if it does not exist

  # This section defines if a REST endpoint is opened on the KSML runner, through which
  # state stores and/or readiness / liveness probes can be accessed.
  applicationServer:
    enabled: false                # Set to true to enable, or false to disable
    host: 0.0.0.0                 # IP address to bind the REST server to
    port: 8080                    # Port number to listen on

  # This section defines whether a Prometheus endpoint is opened to allow metric scraping.
  prometheus:
    enabled: true                 # Set to true to enable, or false to disable
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
      handler: stopOnFail     # How to proceed after encountering the error. Either continueOnFail or stopOnFail.
    produce:
      log: true                   # Log errors true/false
      logPayload: true            # Upon error, should the payload of the message be dumped to the log file.
      loggerName: ProduceError    # Definition of the error logger name.
      handler: continueOnFail     # How to proceed after encountering the error. Either continueOnFail or stopOnFail.

  enableProducers: true           # Set to true to allow producer definitions to be parsed in the KSML definitions and be executed.
  enablePipelines: true           # Set to true to allow pipeline definitions to be parsed in the KSML definitions and be executed.

   # This section tells KSML which schema registries are available
  schemaRegistries:
     # Definition of an Apicurio schema registry
     apicurio:
        config:
        # Below is an example SSL configuration for Apicurio Serialization library from Apicurio documentation
        # apicurio.registry.request.ssl.keystore.location: /path/to/keystore.jks
        # apicurio.registry.request.ssl.keystore.type: JKS
        # apicurio.registry.request.ssl.keystore.password: password
        # apicurio.registry.request.ssl.key.password: password
        # apicurio.registry.request.ssl.truststore.location: /path/to/truststore.jks
        # apicurio.registry.request.ssl.truststore.type: JKS
        # apicurio.registry.request.ssl.truststore.password: password

     # Definition of a Confluent schema registry
     confluent:
        config:
        # Example SSL configuration for Confluent schema registry
        # schema.registry.ssl.protocol: TLSv1.3
        # schema.registry.ssl.enabled.protocols: TLSv1.3,TLSv1.2
        # schema.registry.ssl.endpoint.identification.algorithm: ""
        # schema.registry.ssl.keystore.location: /path/to/keystore.jks
        # schema.registry.ssl.keystore.type: JKS
        # schema.registry.ssl.key.password: password
        # schema.registry.ssl.keystore.password: password
        # schema.registry.ssl.truststore.location: /path/to/truststore.jks
        # schema.registry.ssl.truststore.type: JKS
        # schema.registry.ssl.truststore.password: password

   # This section tells KSML which serializers / deserializers handle which notation types
  notations:
     # Definition for "avro" notation
     avro:
        serde: confluent_avro         # For AVRO there are two implementations: apicurio_avro and confluent_avro
        schemaRegistry: confluent
        config:
           # Specify all properties to be passed into Confluent's KafkaAvroSerializer and KafkaAvroDeserializer
           normalize.schemas: true
           auto.register.schemas: false

     # Definition for "second_avro" notation. With this entry, you can use a type like "second_avro:SchemaName" in your
     # KSML definition, which then uses the Apicurio implementation for AVRO.
     second_avro:
        serde: apicurio_avro
        schemaRegistry: apicurio
        config:
        # Apicurio Avro serialisation settings
        # apicurio.registry.avro.encoding: "BINARY"
        # apicurio.registry.headers.enabled: "true"
        # apicurio.registry.serde.IdHandler: "io.apicurio.registry.serde.Default4ByteIdHandler"
        # apicurio.registry.schema-resolver: "io.apicurio.registry.resolver.DefaultSchemaResolver"

     # Definition for "json" notation
     jsonschema:
        serde: apicurio_json          # For JSON there is only one implementation: apicurio_json
        schemaRegistry: apicurio
        config:
           apicurio.registry.auto-register: true

     # Definition for "protobuf" notation
     protobuf:                      # Definition for "protobuf" notation
        type: apicurio_protobuf      # For Protobuf there is only one implementation: apicurio_protobuf
        schemaRegistry: apicurio
        ## Below this line, specify properties to be passed into Apicurio's ProtobufKafkaSerializer and ProtobufKafkaDeserializer
        config:
           apicurio.registry.auto-register: false

  # Section where you specify which KSML definitions to load, parse and execute.
  definitions:
    # Format is <namespace>: <ksml_definition_filename>
    inspect: 01-example-inspect.yaml
#    copy: 02-example-copy.yaml
#    filter: 03-example-filter.yaml
#    branch: 04-example-branch.yaml
#    route: 05-example-route.yaml
#    duplicate: 06-example-duplicate.yaml
#    convert: 07-example-convert.yaml
#    count: 08-example-count.yaml
#    aggregate: 09-example-aggregate.yaml
#    queryable_table: 10-example-queryable-table.yaml
#    field_modification: 11-example-field-modification.yaml
#    byte_manipulation: 12-example-byte-manipulation.yaml
#    join: 13-example-join.yaml
#    manual_state_store: 14-example-manual-state-store.yaml
#    pipeline_linking: 15-example-pipeline-linking.yaml
#    transform_metadata: 16-example-transform-metadata.yaml
#    inspect_with_metrics: 17-example-inspect-with-metrics.yaml
#    timestamp_extractor: 18-example-timestamp-extractor.yaml
#    performance-measurement: 19-example-performance-measurement.yaml

# This setup connects to the Kafka broker and schema registry started with the example docker-compose file
# These examples are intended to run from a inside a container on the same network
kafka:
  application.id: io.ksml.example.streaming
  # The group instance id is used to identify a single instance of the application, allowing for
  # faster rebalances and less partition reassignments. The name must be unique for each member in the group.
  group.instance.id: example-instance

  bootstrap.servers: broker:9093
  security.protocol: PLAINTEXT
  auto.offset.reset: earliest
  acks: all

  # These are Kafka SSL configuration properties. Check the documentation at1
  # Check the documentation at https://kafka.apache.org/documentation/#producerconfigs for more properties

  #  security.protocol: SSL
  #  ssl.protocol: TLSv1.3
  #  ssl.enabled.protocols: TLSv1.3,TLSv1.2
  #  ssl.endpoint.identification.algorithm: ""
  #  ssl.keystore.type: JKS
  #  ssl.truststore.type: JKS
  #  ssl.key.password: xxx
  #  ssl.keystore.password: xxx
  #  ssl.keystore.location: /path/to/ksml.keystore.jks
  #  ssl.truststore.password: xxx
  #  ssl.truststore.location: /path/to/ksml.truststore.jks

  # Use these configuration properties when connecting to a cluster using the Axual naming patterns.
  # These patterns are resolved into the actual name used on Kafka using the values in this configuration map
  # and the topic names specified in the definition YAML files

  #  axual.topic.pattern: "{tenant}-{instance}-{environment}-{topic}"
  #  # Results in Kafka topic ksmldemo-dta-dev-<topic name from KSML definition YAML>
  #  axual.group.id.pattern: "{tenant}-{instance}-{environment}-{group.id}"
  #  axual.transactional.id.pattern: "{tenant}-{instance}-{environment}-{transactional.id}"
  #  tenant: "ksmldemo"
  #  instance: "dta"
  #  environment: "dev"
```


### Kafka Configuration



A KSML configuration file typically consists of the following main sections:

```yaml
# Basic metadata
name: "my-ksml-application"
version: "1.0.0"
description: "My KSML Application"

# Data definitions
streams:
# Stream definitions
tables:
# Table definitions
globalTables:
# Global table definitions

# Function definitions
functions:
# Function definitions

# Pipeline definitions
pipelines:
# Pipeline definitions

# Application configuration
config:
# Application configuration
```



## How to configure notations

To accommodate for variations in Kafka setups and/or ecosystem variations, the following table lists the available
options you can configure. The `notation name` lists the notation you use in your KSML definitions, for example
`avro:SensorData`. The configuration of all notations is done in the `ksml-runner.yaml` file, from which KSML reads its
technical configuration parameters.

## List of available supported variations

The table below provides a complete list of all notations, their possible implementation alternatives and corresponding
characteristics, such as what data types a notation maps to.

| Notation name | Available serdes     | Serde supplier | Schema Registry | Loaded in KSML | Remarks                                          |
|---------------|----------------------|:--------------:|:---------------:|:--------------:|--------------------------------------------------|
| avro          | apicurio_avro        |    Apicurio    |    Apicurio     | If configured  | Talks to Apicurio Schema Registry                |
| avro          | confluent_avro       |   Confluent    |    Confluent    |    Default     | Talks to Confluent Schema Registry               |
| csv           | csv                  |      ksml      |        -        |     Always     | CSV Schemaless                                   |
| json          | json                 |      ksml      |        -        |     Always     | JSON Schemaless                                  |
| jsonschema    | apicurio_jsonschema  |    Apicurio    |    Apicurio     | If configured  | Talks to Apicurio Schema Registry                |
| jsonschema    | confluent_jsonschema |   Confluent    |    Confluent    | If configured  | Talks to Confluent Schema Registry               |
| protobuf      | apicurio_protobuf    |    Apicurio    |        Y        | If configured  | Any Protobuf, schema loaded from SR upon consume |
| protobuf      | confluent_protobuf   |   Confluent    |        Y        | If configured  | Any Protobuf, schema loaded from SR upon consume |
| soap          | soap                 |      ksml      |        N        |     Always     | SOAP Schemaless                                  |
| xml           | xml                  |      ksml      |        N        |     Always     | XML Schemaless                                   |





## Application Metadata

### Basic Metadata

| Property      | Type   | Required | Description                          |
|---------------|--------|----------|--------------------------------------|
| `name`        | String | Yes      | The name of the KSML definition      |
| `version`     | String | No       | The version of the KSML definition   |
| `description` | String | No       | A description of the KSML definition |

Example:

```yaml
name: "order-processing-app"
version: "1.2.3"
description: "Processes orders from the order topic and enriches them with customer data"
```

## Data Definitions

### Streams

Streams represent unbounded sequences of records.

| Property             | Type   | Required | Description                                            |
|----------------------|--------|----------|--------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to               |
| `keyType`            | String | Yes      | The type of the record key                             |
| `valueType`          | String | Yes      | The type of the record value                           |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`) |
| `timestampExtractor` | String | No       | The function to extract timestamps from records        |

Example:

```yaml
streams:
  orders:
    topic: "orders"
    keyType: "string"
    valueType: "avro:Order"
    offsetResetPolicy: "earliest"
```

### Tables

Tables represent changelog streams from a primary-keyed table.

| Property             | Type   | Required | Description                                            |
|----------------------|--------|----------|--------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to               |
| `keyType`            | String | Yes      | The type of the record key                             |
| `valueType`          | String | Yes      | The type of the record value                           |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`) |
| `timestampExtractor` | String | No       | The function to extract timestamps from records        |
| `store`              | String | No       | The name of the key/value state store to use           |

Example:

```yaml
tables:
  customers:
    topic: "customers"
    keyType: "string"
    valueType: "avro:Customer"
    store: "customer-store"
```

### Global Tables

Global tables are similar to tables but are fully replicated on each instance of the application.

| Property             | Type   | Required | Description                                            |
|----------------------|--------|----------|--------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from                           |
| `keyType`            | String | Yes      | The type of the record key                             |
| `valueType`          | String | Yes      | The type of the record value                           |
| `offsetResetPolicy`  | String | No       | The offset reset policy (`earliest`, `latest`, `none`) |
| `timestampExtractor` | String | No       | The function to extract timestamps from records        |
| `store`              | String | No       | The name of the key/value state store to use           |

Example:

```yaml
globalTables:
  products:
    topic: "products"
    keyType: "string"
    valueType: "avro:Product"
```

## Function Definitions

Functions define reusable pieces of logic that can be referenced in pipelines.

| Property     | Type   | Required | Description                                                |
|--------------|--------|----------|------------------------------------------------------------|
| `type`       | String | Yes      | The type of function (predicate, mapper, aggregator, etc.) |
| `expression` | String | No       | A simple expression for the function                       |
| `code`       | String | No       | Python code implementing the function                      |
| `parameters` | Array  | No       | Parameters for the function                                |

Example:

```yaml
functions:
  is_valid_order:
    type: "predicate"
    code: |
      if value is None:
        return False

      if "orderId" not in value:
        return False

      if "items" not in value or not value["items"]:
        return False

      return True

  enrich_order:
    type: "mapper"
    code: |
      return {
        "order_id": value.get("orderId"),
        "customer_id": value.get("customerId"),
        "items": value.get("items", []),
        "total": sum(item.get("price", 0) * item.get("quantity", 0) for item in value.get("items", [])),
        "timestamp": value.get("timestamp", int(time.time() * 1000))
      }
```

## Pipeline Definitions

Pipelines define the flow of data through the application.

| Property | Type         | Required | Description                         |
|----------|--------------|----------|-------------------------------------|
| `from`   | String/Array | Yes      | The source stream(s) or table(s)    |
| `via`    | Array        | No       | The operations to apply to the data |
| `to`     | String/Array | Yes      | The destination stream(s)           |

Example:

```yaml
pipelines:
  process_orders:
    from: "orders"
    via:
      - type: "filter"
        if:
          code: "is_valid_order(key, value)"
      - type: "mapValues"
        mapper:
          code: "enrich_order(key, value)"
      - type: "peek"
        forEach:
          code: |
            log.info("Processing order: {}", value.get("order_id"))
    to: "processed_orders"
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
- [Operations Reference](operations-reference.md)
- [Functions Reference](functions-reference.md)
- [Data Types Reference](data-types-reference.md)