# Working with Different Data Formats

Learn how to process, convert, and validate data using KSML's supported formats including JSON, Avro, CSV, XML, Binary, and SOAP.

## Prerequisites

- Basic understanding of Kafka concepts (topics, messages)
- Completed the [Building a Simple Data Pipeline](simple-pipeline.md) tutorial
- Familiarity with basic KSML concepts (streams, functions, pipelines)

## Supported Data Formats

- **String**: Plain text
- **JSON**: Structured data without schema validation
- **Avro**: Binary format with schema registry integration
- **CSV**: Tabular data with optional schema
- **XML**: Hierarchical data with XSD schema support
- **Binary**: Raw bytes for custom protocols
- **SOAP**: Web service messaging format

## Specifying Data Formats

When defining streams in KSML, you specify the data format using the `keyType` and `valueType` properties:

??? info "Specifying data formats (click to expand)"

    ```yaml
    streams:
      json_stream:
        topic: example_json_topic
        keyType: string
        valueType: json

      avro_stream:
        topic: example_avro_topic
        keyType: string
        valueType: avro:SensorData
    ```

Schema-based formats (Avro, XML, CSV) require a schema name: `format:SchemaName` (e.g., `avro:SensorData`).


### Setup Requirements

1. Create `docker-compose.yml` with schema registry and pre-created topics 

??? info "Docker Compose Configuration (click to expand)"

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
          # If the kafka topics can list data, the broker is healthy
          test: kafka-topics.sh --bootstrap-server broker:9093 --list
          interval: 5s
          timeout: 10s
          retries: 10
          start_period: 5s

      ksml:
        image: registry.axual.io/opensource/images/axual/ksml:snapshot
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
          schema_registry:
            condition: service_healthy


      schema_registry:
        image: apicurio/apicurio-registry:3.0.2
        hostname: schema-registry
        depends_on:
          broker:
            condition: service_healthy
        ports:
          - "8081:8081"
        networks:
          - ksml
        restart: always
        environment:
          QUARKUS_HTTP_PORT: 8081
          QUARKUS_HTTP_CORS_ORIGINS: '*'
          QUARKUS_PROFILE: "prod"
          APICURIO_STORAGE_KIND: kafkasql
          APICURIO_KAFKASQL_BOOTSTRAP_SERVERS: 'broker:9093'
          APICURIO_KAFKASQL_TOPIC: '_apciurio-kafkasql-store'
        healthcheck:
          # If the api endpoint is available, the service is considered healthy
          test: curl http://localhost:8081/apis
          interval: 15s
          timeout: 10s
          retries: 10
          start_period: 10s

      kafka-ui:
        image: quay.io/cloudhut/kowl:master
        container_name: kowl
        restart: always
        ports:
          - 8080:8080
        volumes:
          - ./kowl-ui-config.yaml:/config/kowl-ui-config.yaml:ro
        environment:
          CONFIG_FILEPATH: "/config/kowl-ui-config.yaml"
        depends_on:
          broker:
            condition: service_healthy
        networks:
          - ksml

      # This "container" is a workaround to pre-create topics
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
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_avro && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_json_raw && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_transformed && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_csv && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_csv_processed && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_json_processed && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_converted_formats && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic device_config && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_readings && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic combined_sensor_data && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_xml && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_xml_processed && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_binary && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_sensordata_binary_processed && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_soap_requests && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_soap_responses && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_data_json'"
    ```

2. Create `kowl-ui-config.yaml` for Kafka UI:

??? info "Kafka UI Configuration (click to expand)"

    ```yaml
    server:
      listenPort: 8080
      listenAddress: 0.0.0.0

    kafka:
      brokers:
        - broker:9093
      schemaRegistry:
        enabled: true
        urls:
          - http://schema-registry:8081/apis/ccompat/v7
    ```

3. Create `examples/ksml-runner.yaml` with Avro configuration:

??? info "KSML Runner Configuration (click to expand)"

    ```yaml
    ksml:
      storageDirectory: /tmp
      definitions:
        producer: producer.yaml
        processor: processor.yaml
      schemaRegistries:
        my_schema_registry:
          config:
            schema.registry.url: http://schema-registry:8081/apis/ccompat/v7
      notations:
        avro:
          type: confluent_avro         # For AVRO there are two implementations: apicurio_avro and confluent_avro
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
4. For each example, create `producer.yaml` and `processor.yaml` files
5. Restart KSML: `docker compose down & docker compose up -d && docker compose logs ksml -f` (which is faster than `docker compose restart ksml`)


## Working with Avro Data

Avro provides schema-based binary serialization with validation, evolution support, and compact encoding.

This producer generates JSON data that KSML automatically converts to Avro format using the schema registry:

??? info "Producer definition for AVRO messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-avro.yaml"
    %}
    ```

Create `examples/SensorData.avsc` schema file (JSON format, auto-loaded from working directory):

??? info "AVRO Schema for examples below (click to expand)"

    ```json
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/SensorData.avsc"
    %}
    ```

This processor converts Avro messages to JSON using the `convertValue` operation:

??? info "Avro to JSON conversion processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-avro-convert.yaml"
    %}
    ```

This processor transforms Avro data (uppercases sensor names) while maintaining the Avro format:

??? info "Avro transformation processor (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-avro-transform.yaml"
    %} 
    ```

## Working with JSON Data

JSON provides flexible, human-readable structured data without schema validation requirements.

This producer generates JSON sensor data directly (no format conversion needed):

??? info "Producer definition for JSON messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-json.yaml"
    %}
    ```

This processor demonstrates key-value transformation using `keyValueTransformer` to modify both message keys and values:

??? info "Processor definition for JSON messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-json.yaml"
    %}
    ```

## Working with CSV Data

CSV handles tabular data with schema-based column definitions and structured object access.

Create `examples/SensorData.csv` schema file (defines column order):

??? info "CSV schema (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/SensorData.csv"
    %}
    ```

This producer generates JSON data that KSML converts to CSV format using the schema:

??? info "Producer definition for CSV messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-csv.yaml"
    %}
    ```

This processor demonstrates CSV data manipulation (uppercases city names) while maintaining CSV format:

??? info "Processor definition for CSV messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-csv.yaml"
    %}
    ```
## Working with XML Data

XML (eXtensible Markup Language) is a structured format for representing hierarchical data with custom tags and attributes.

- XML data is represented as nested elements with opening and closing tags
- Elements can contain text content, attributes, and child elements
- XSD (XML Schema Definition) defines the structure, data types, and constraints for XML documents

### Requirements for running KSML XML definitions

To run KSML XML processing definitions below, please follow these steps:

- Save this `examples/SensorData.xsd` as XML schema:

??? info "XSD schema (click to expand)"

    ```xml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/SensorData.xsd"
    %}
    ```

- Use this `examples/producer.yaml` that produces XML messages that our processing definition below can work with:

??? info "Producer definition for XML messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-xml.yaml"
    %}
    ```

This processor demonstrates XML data manipulation (uppercases city names) while maintaining XML format:

??? info "Processor definition for XML messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-xml.yaml"
    %}
    ```

- Please note that `kowl` is not capable of deserializing XML messages and will display the value of the messages as blank.
- To read the XML messages use kcat:

```shell
kcat -b localhost:9092 -t ksml_sensordata_xml -C -o -1 -c 1 -f '%s\n' | xmllint --format -
```

??? info "XML message example (click to expand)"

    ```xml
    <?xml version="1.1" encoding="UTF-8"?>
    <SensorData>
      <city>Rotterdam</city>
      <color>black</color>
      <name>sensor6</name>
      <owner>Alice</owner>
      <timestamp>1754376106863</timestamp>
      <type>TEMPERATURE</type>
      <unit>%</unit>
      <value>12</value>
    </SensorData>
    ```

## Working with Binary Data

Binary data represents raw bytes as sequences of numeric values ranging from 0 to 255, ideal for handling non-text content like images, files, or custom protocols.

- Binary data is represented as arrays of integers where each value corresponds to a single byte
- Each byte can store values from 0-255, allowing for compact encoding of various data types
- Binary processing enables direct byte manipulation, bit-level operations, and efficient handling of structured binary formats

This producer creates simple binary messages as byte arrays (7-byte messages with counter, random bytes, and ASCII "KSML"):

??? info "Producer definition for Binary messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-binary.yaml"
    %}
    ```

This processor demonstrates binary data manipulation (increments first byte) while maintaining binary format:

??? info "Processor definition for Binary messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-binary.yaml"
    %}
    ```

## Working with SOAP Data

SOAP provides structured web service messaging with envelope/body format and no WSDL requirements.

This producer creates SOAP request messages with envelope/body structure (no WSDL files required):

??? info "Producer definition for SOAP messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-soap.yaml"
    %}
    ```

This processor transforms SOAP requests into SOAP responses (extracts request data and creates response with sensor values):

??? info "Processor definition for SOAP messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-soap.yaml"
    %}
    ```

## Converting Between Data Formats

Use the `convertValue` operation to transform data between formats within a single pipeline.

This producer generates Avro messages for format conversion demonstrations:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-converting.yaml"
    %}
    ```

This processor demonstrates multiple format conversions (Avro → JSON → String → JSON) using `convertValue`:

??? info "Processing definition for converting between multiple formats (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-converting.yaml"
    %}
    ```

### Implicit Conversion

KSML automatically converts between formats when stream input/output types differ:

```yaml
pipelines:
  implicit_conversion:
    from: avro_input  # Stream with Avro format
    to: xml_output    # Stream with XML format
    # The conversion happens automatically
```

## Working with Multiple Formats in a Single Pipeline

Process different data formats within one KSML definition using separate pipelines.

This producer generates both JSON config data and Avro sensor data:

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/producer-multiple-formats.yaml"
    %}
    ```

This processor shows two pipelines handling different formats (Avro and JSON) and combining results:

??? info "Processor definition for working with multiple formats in a single pipeline (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/different-data-formats/processor-multiple-formats.yaml"
    %}
    ```


## Conclusion

You've learned to work with KSML's data formats: JSON, Avro, CSV, XML, Binary, and SOAP. Key concepts include format specification, schema usage, conversion operations, and multi-format pipelines.

## Next Steps

- [Logging and Monitoring](logging-monitoring.md) - Add effective logging to pipelines
- [Intermediate Tutorials](../intermediate/index.md) - Advanced KSML features
- [KSML Examples](../../resources/examples-library.md) - More data format examples
