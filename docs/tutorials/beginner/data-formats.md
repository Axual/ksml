# Working with Different Data Formats

This tutorial guides you through working with various data formats in KSML. You'll learn how to process data in different formats, convert between formats, and leverage schema-based formats for data validation and compatibility.

## Prerequisites

- Basic understanding of Kafka concepts (topics, messages)
- Completed the [Building a Simple Data Pipeline](simple-pipeline.md) tutorial
- Familiarity with basic KSML concepts (streams, functions, pipelines)

## Data Formats in KSML

KSML supports a variety of data formats for both keys and values in Kafka messages:

- **String**: Plain text data
- **JSON**: JavaScript Object Notation for structured data
- **Avro**: Schema-based binary format with strong typing
- **CSV**: Comma-separated values for tabular data
- **XML**: Extensible Markup Language for hierarchical data
- **Binary**: Raw binary data

Each format has its own strengths and use cases, which we'll explore in this tutorial.

## Specifying Data Formats

When defining streams in KSML, you specify the data format using the `keyType` and `valueType` properties:

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

For schema-based formats like Avro, XML, and CSV, you specify the schema name after the format type (e.g., `avro:SensorData`). See below for examples of this.


### Requirements for running the examples in this tutorial

To run the KSML example definitions below, please follow these steps:

- Create a `docker-compose.yml` file and copy contents from below
    - It has a schema registry for AVRO messages
    - It contains a separate `kafka-setup` service for creating the topics for the definitions in this tutorial, because setting Kafka's configuration option `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE` to `true` leads to initial errors in the logs due to Kafka Streams' Admin client doing GET requests before creating the topics. 

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

- Create `kowl-ui-config.yaml` that supports schema registry:

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

- Create `examples/ksml-runner.yaml` that supports Confluent AVRO:

??? info "Kafka UI Configuration (click to expand)"

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
- For each example, create the `producer.yaml` and `processor.yaml` and check that the yaml value for `ksml.definitions` in `examples/ksml-runner.yaml` is correct.
- Run the command `docker compose down & docker compose up -d && docker compose logs ksml -f` to restart the KSML service (which is faster than `docker compose restart ksml`).


## Working with Avro Data

Avro is a schema-based binary format that provides strong typing, schema evolution, and compact serialization.

Benefits of Using Avro:

- Data is validated against the schema during serialization
- Binary encoding is more efficient than text-based formats
- Avro supports adding, removing, or changing fields while maintaining compatibility
- Avro schemas can be used with multiple programming languages

### Requirements for running KSML AVRO definitions

- Create `examples/producer.yaml` that produces AVRO messages:

??? info "Producer definition for AVRO messages (click to expand)"

    ```yaml
    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate random sensor measurement data
          value = {
            "name": key,
            "timestamp": str(round(time.time()*1000)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "value": str(random.randrange(0, 100)),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"])
          }
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Indicate the type of key and value

    producers:
      # Produce an AVRO SensorData message every 3 seconds
      sensordata_avro_producer:
        generator: generate_sensordata_message
        interval: 3s
        to:
          topic: sensor_data_avro
          keyType: string
          valueType: avro:SensorData
    ```

- Create schema `examples/SensorData.avsc`
    - Avro schemas are defined in JSON format. KSML will pick the schema up automatically because in `docker-compose.yml` we have defined `working_dir: /ksml` and we have not overriden `configDirectory` and `schemaDirectory` in `ksml-runner.yaml`(they default to working directory).

??? info "AVRO Schema for examples below  (click to expand)"

    ```json
    {
      "type": "record",
      "name": "SensorData",
      "namespace": "com.example",
      "fields": [
        {"name": "name", "type": ["null", "string"], "default": null},
        {"name": "timestamp", "type": ["null", "string"], "default": null},
        {"name": "type", "type": ["null", "string"], "default": null},
        {"name": "unit", "type": ["null", "string"], "default": null},
        {"name": "value", "type": ["null", "string"], "default": null},
        {"name": "color", "type": ["null", "string"], "default": null},
        {"name": "owner", "type": ["null", "string"], "default": null},
        {"name": "city", "type": ["null", "string"], "default": null}
      ]
    }
    ```

- Create `examples/processor.yaml` with any of the processing definitions below

### Convert AVRO to JSON processor definition

??? info "KSML processing definition that converts from AVRO to JSON  (click to expand)"

    ```yaml
    streams:
      avro_input:
        topic: sensor_data_avro
        keyType: string
        valueType: avro:SensorData
        offsetResetPolicy: latest

      json_output:
        topic: sensor_data_json
        keyType: string
        valueType: json

    pipelines:
      avro_to_json_pipeline:
        from: avro_input
        via:
          # Log the incoming AVRO data
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("Original AVRO: sensor={}, type={}, value={}{}",
                          value.get("name"), value.get("type"), value.get("value"), value.get("unit"))
                else:
                  log.info("Received null message with key={}", key)

          # Explicitly convert from Avro to JSON
          - type: convertValue
            into: json

          # Log the converted JSON data
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("Converted to JSON: sensor={}, city={}", value.get("name"), value.get("city"))
                else:
                  log.info("JSON conversion result: null")

        to: json_output
    ```

### Transform AVRO messages example

??? info "KSML processing definition that transforms AVRO messages  (click to expand)"

    ```yaml
    streams:
      avro_input:
        topic: sensor_data_avro
        keyType: string
        valueType: avro:SensorData
        offsetResetPolicy: latest

      transformed_output:
        topic: sensor_data_transformed
        keyType: string
        valueType: avro:SensorData

    functions:
      uppercase_sensor_name:
        type: valueTransformer
        code: |
          # Simple transformation: uppercase the sensor name
          result = dict(value) if value else None
          if result and result.get("name"):
            result["name"] = result["name"].upper()
        expression: result
        resultType: avro:SensorData

    pipelines:
      transformation_pipeline:
        from: avro_input
        via:
          # Step 1: Apply transformation
          - type: transformValue
            mapper: uppercase_sensor_name

          # Step 2: Log the transformation
          - type: peek
            forEach:
              code: |
                log.info("Transformed sensor name to: {}", value.get("name") if value else "null")

        to: transformed_output
    ```

## Working with JSON Data

JSON is one of the most common data formats used with Kafka. It's human-readable, flexible, and widely supported.

### Requirements for running KSML AVRO definitions

To run KSML AVRO processing definitions below, please follow these steps:

- Use this `examples/producer.yaml` that produces JSON messages:

??? info "Producer definition for JSON messages (click to expand)"

    ```yaml
    functions:
      generate_json_sensordata:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate random sensor measurement data as JSON
          value = {
            "name": key,
            "timestamp": str(round(time.time()*1000)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "value": str(random.randrange(0, 100)),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"])
          }
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Indicate the type of key and value

    producers:
      # Produce JSON sensor data messages every 2 seconds
      json_sensor_producer:
        generator: generate_json_sensordata
        interval: 2s
        to:
          topic: sensor_data_json_raw
          keyType: string
          valueType: json
    ```

- Use this `examples/processing.yaml` that reads and writes JSON data:

??? info "Processor definition for JSON messages (click to expand)"

    ```yaml
    streams:
      json_input:
        topic: sensor_data_json_raw
        keyType: string
        valueType: json
        offsetResetPolicy: latest

      json_output:
        topic: sensor_data_json_processed
        keyType: string
        valueType: json

    functions:
      add_processing_info:
        type: keyValueTransformer
        code: |
          # Simple transformation: add processing info
          import time
          new_value = dict(value) if value else {}
          new_value["processed"] = True
          new_value["processed_at"] = str(int(time.time() * 1000))
          new_key = f"processed_{key}"
        expression: (new_key, new_value)
        resultType: (string, json)

    pipelines:
      json_processing_pipeline:
        from: json_input
        via:
          # Transform both key and value
          - type: transformKeyValue
            mapper: add_processing_info

          # Log the transformed data
          - type: peek
            forEach:
              code: |
                log.info("Transformed: key={}, sensor={}, processed_at={}",
                        key, value.get("name"), value.get("processed_at"))

        to: json_output
    ```

This definition:

- Uses a `keyValueTransformer` function that transforms both the key (prepends `processed_`) and the value (adds `processing` metadata)
- Uses the `transformKeyValue` operation in the pipeline
- Demonstrates how KSML can transform both keys and values in JSON messages

### JSON Data Handling Tips

- Use Python's dictionary methods like `get()` to safely access fields that might not exist
- Remember that numeric values in JSON are preserved as numbers in KSML
- Nested structures can be accessed using dot notation or nested `get()` calls

## Working with CSV Data

CSV (Comma-Separated Values) is a simple format for representing tabular data.

### CSV Data Handling Tips

- CSV data is represented as a list of values in KSML
- You need to know the column order to correctly interpret the data
- Consider adding header validation if your CSV data includes headers

### Requirements for running KSML CSV definitions

To run KSML CSV processing definitions below, please follow these steps:

- Save this `examples/SensorData.csv` as CSV schema:

??? info "CSV schema (click to expand)"

    ```yaml
    name,timestamp,value,type,unit,color,city,owner
    ```

- Use this `examples/producer.yaml` that produces CSV messages that our processing definition below can work with:

??? info "Producer definition for CSV messages (click to expand)"

    ```yaml
    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate data matching SensorData.csv schema
          # Schema: name,timestamp,value,type,unit,color,city,owner
          value = {
            "name": key,
            "timestamp": str(round(time.time()*1000)),
            "value": str(random.randrange(0, 100)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"])
          }
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Generate as JSON, KSML will convert to CSV

    producers:
      # Produce CSV sensor data messages every 3 seconds
      sensordata_csv_producer:
        generator: generate_sensordata_message
        interval: 3s
        to:
          topic: ksml_sensordata_csv
          keyType: string
          valueType: csv:SensorData                  # KSML will convert JSON to CSV format
    ```

- Create `examples/processor.yaml` with this processing definition that shows how to read and write CSV data:

??? info "Processor definition for CSV messages (click to expand)"

    ```yaml
    streams:
      csv_input:
        topic: ksml_sensordata_csv
        keyType: string
        valueType: csv:SensorData
        offsetResetPolicy: latest

      csv_output:
        topic: ksml_sensordata_csv_processed
        keyType: string
        valueType: csv:SensorData

    functions:
      uppercase_city:
        type: valueTransformer
        code: |
          # When using csv:SensorData, the data comes as a structured object (dict)
          if value and isinstance(value, dict):
            # Create a copy and uppercase the city
            enriched = dict(value)
            if "city" in enriched:
              enriched["city"] = enriched["city"].upper()
            result = enriched
          else:
            result = value
        expression: result
        resultType: csv:SensorData

    pipelines:
      process_csv:
        from: csv_input
        via:
          # Log the original CSV data
          - type: peek
            forEach:
              code: |
                if value:
                  log.info("Original: sensor={}, city={}", value.get("name"), value.get("city"))

          # Transform the CSV data - uppercase city
          - type: transformValue
            mapper: uppercase_city

          # Log the transformed CSV data
          - type: peek
            forEach:
              code: |
                if value:
                  log.info("Transformed: sensor={}, city={}", value.get("name"), value.get("city"))

        to: csv_output
    ```
## Working with XML Data

XML (eXtensible Markup Language) is a structured format for representing hierarchical data with custom tags and attributes.

- XML data is represented as nested elements with opening and closing tags
- Elements can contain text content, attributes, and child elements
- XSD (XML Schema Definition) defines the structure, data types, and constraints for XML documents

### Requirements for running KSML CSV definitions

To run KSML XML processing definitions below, please follow these steps:

- Save this `examples/SensorData.xsd` as XML schema:

??? info "XSD schema (click to expand)"

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="SensorData">
            <xs:complexType>
                <xs:sequence>
                    <xs:element name="name" type="xs:string"/>
                    <xs:element name="timestamp" type="xs:long"/>
                    <xs:element name="value" type="xs:string"/>
                    <xs:element name="type" type="xs:string"/>
                    <xs:element name="unit" type="xs:string"/>
                    <xs:element name="color" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="city" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="owner" type="xs:string" minOccurs="0" maxOccurs="1"/>
                </xs:sequence>
            </xs:complexType>
        </xs:element>
    </xs:schema>
    ```

- Use this `examples/producer.yaml` that produces XML messages that our processing definition below can work with:

??? info "Producer definition for XML messages (click to expand)"

    ```yaml
    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate data matching SensorData.xsd schema
          value = {
            "name": key,
            "timestamp": round(time.time()*1000),     # timestamp as long, not string
            "value": str(random.randrange(0, 100)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"])
          }
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Generate as JSON, KSML will convert to XML

    producers:
      # Produce XML sensor data messages every 3 seconds
      sensordata_xml_producer:
        generator: generate_sensordata_message
        interval: 3s
        to:
          topic: ksml_sensordata_xml
          keyType: string
          valueType: xml:SensorData                   # KSML will convert JSON to XML format
    ```

- Create `examples/processor.yaml` with this processing definition that shows how to read and write XML data:

??? info "Processor definition for XML messages (click to expand)"

    ```yaml
    streams:
      xml_input:
        topic: ksml_sensordata_xml
        keyType: string
        valueType: xml:SensorData
        offsetResetPolicy: latest

      xml_output:
        topic: ksml_sensordata_xml_processed
        keyType: string
        valueType: xml:SensorData

    functions:
      uppercase_city:
        type: valueTransformer
        code: |
          # When using xml:SensorData, the data comes as a structured object (dict)
          if value and isinstance(value, dict):
            # Create a copy and uppercase the city
            enriched = dict(value)
            if "city" in enriched:
              enriched["city"] = enriched["city"].upper()
            result = enriched
          else:
            result = value
        expression: result
        resultType: xml:SensorData

    pipelines:
      process_xml:
        from: xml_input
        via:
          # Log the original XML data
          - type: peek
            forEach:
              code: |
                if value:
                  log.info("Original: sensor={}, city={}", value.get("name"), value.get("city"))

          # Transform the XML data - uppercase city
          - type: transformValue
            mapper: uppercase_city

          # Log the transformed XML data
          - type: peek
            forEach:
              code: |
                if value:
                  log.info("Transformed: sensor={}, city={}", value.get("name"), value.get("city"))

        to: xml_output
    ```

- Please note that `kowl` is not capable of deserializing XML messages and will display the value of the messages as blank.
- To read the messages use kcat:

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

### Requirements for running KSML Binary definitions

To run KSML Binary processing definitions below, please follow these steps:

- Use this `examples/producer.yaml` that produces Binary messages that our processing definition below can work with:
- This producer definition:
    - Generates binary messages as byte arrays (list of integers 0-255)
      - Creates a simple 7-byte message with:
        - A counter byte
        - Random bytes
        - ASCII characters spelling "KSML"
        - Write to binary topic
        - Binary data is logged showing the byte values
        - Using `valueType: bytes` ensures proper serialization

??? info "Producer definition for Binary messages (click to expand)"

    ```yaml
    functions:
      generate_binary_message:
        type: generator
        globalCode: |
          import random
          counter = 0
        code: |
          global counter

          key = "msg" + str(counter)
          counter = (counter + 1) % 100

          # Create simple binary message as list of bytes
          value = [
            counter % 256,                    # Counter byte
            random.randrange(0, 256),         # Random byte
            ord('K'),                         # ASCII 'K'
            ord('S'),                         # ASCII 'S'
            ord('M'),                         # ASCII 'M'
            ord('L'),                         # ASCII 'L'
            random.randrange(0, 256)          # Another random byte
          ]

          log.info("Generated binary message: key={}, bytes={}", key, value)
        expression: (key, value)
        resultType: (string, bytes)

    producers:
      binary_producer:
        generator: generate_binary_message
        interval: 3s
        to:
          topic: ksml_sensordata_binary
          keyType: string
          valueType: bytes
    ```

- Create `examples/processor.yaml` with this processing definition that shows how to read and write XML data:
- This processing definition:
    - Reads binary messages from input binary topic
    - Performs simple manipulation (increments the first byte)
    - Writes modified binary data to output binary topic

??? info "Processor definition for Binary messages (click to expand)"

    ```yaml
    streams:
      binary_input:
        topic: ksml_sensordata_binary
        keyType: string
        valueType: bytes
        offsetResetPolicy: latest

      binary_output:
        topic: ksml_sensordata_binary_processed
        keyType: string
        valueType: bytes

    functions:
      increment_first_byte:
        type: valueTransformer
        code: |
          # Simple binary manipulation: increment the first data byte
          if isinstance(value, list) and len(value) > 0:
            modified = list(value)
            modified[0] = (modified[0] + 1) % 256  # Increment and wrap at 256
            result = modified
          else:
            result = value
        expression: result
        resultType: bytes

    pipelines:
      process_binary:
        from: binary_input
        via:
          # Log input binary
          - type: peek
            forEach:
              code: |
                log.info("Binary input: key={}, bytes={}", key, value)

          # Modify binary data
          - type: transformValue
            mapper: increment_first_byte

          # Log output binary
          - type: peek
            forEach:
              code: |
                log.info("Binary output: key={}, bytes={}", key, value)

        to: binary_output
    ```


## Working with SOAP Data

SOAP (Simple Object Access Protocol) is a structured messaging protocol that uses XML envelopes to wrap web service requests and responses, enabling standardized communication between distributed systems.

- SOAP messages follow a strict envelope/header/body structure with XML formatting
- Each message contains metadata in headers and actual service data in the body section
- SOAP processing enables web service integration, remote procedure calls, and enterprise system communication without requiring WSDL schema definitions

### Requirements for running KSML SOAP definitions

To run KSML SOAP processing definitions below, please follow these steps:

- Use this `examples/producer.yaml` that produces SOAP messages that our processing definition below can work with:
- This producer definition:
    - Generates SOAP messages with proper envelope structure
    - Creates SOAP requests with:
        - Header containing a MessageID
        - Body containing a GetSensorData operation
    - Uses the structured SOAP format without WSDL files
    - SOAP data is represented as nested dictionaries with specific fields

??? info "Producer definition for SOAP messages (click to expand)"

    ```yaml
    functions:
      generate_soap_message:
        type: generator
        globalCode: |
          counter = 0
        code: |
          global counter

          key = "msg" + str(counter)
          counter = (counter + 1) % 100

          # Create simple SOAP message
          value = {
            "envelope": {
              "body": {
                "elements": [
                  {
                    "name": {
                      "localPart": "SensorRequest",
                      "namespaceURI": "http://example.com/ksml"
                    },
                    "value": {
                      "id": str(counter),
                      "type": "temperature"
                    }
                  }
                ]
              }
            }
          }

          log.info("Generated SOAP message: {}", key)
        expression: (key, value)
        resultType: (string, soap)

    producers:
      soap_producer:
        generator: generate_soap_message
        interval: 3s
        to:
          topic: ksml_soap_requests
          keyType: string
          valueType: soap
    ```

- Create `examples/processor.yaml` with this processing definition that shows how to read and write SOAP data:
- This processing definition:
    - Reads SOAP request messages
    - Extracts data from the SOAP body
    - Creates SOAP response messages with sensor data
    - Demonstrates SOAP message transformation

??? info "Processor definition for SOAP messages (click to expand)"

    ```yaml
    streams:
      soap_input:
        topic: ksml_soap_requests
        keyType: string
        valueType: soap
        offsetResetPolicy: latest

      soap_output:
        topic: ksml_soap_responses
        keyType: string
        valueType: soap

    functions:
      create_response:
        type: valueTransformer
        code: |
          # Extract request data
          request_elements = value.get("envelope", {}).get("body", {}).get("elements", [])
          request_data = request_elements[0].get("value", {}) if request_elements else {}

          # Create SOAP response
          result = {
            "envelope": {
              "body": {
                "elements": [
                  {
                    "name": {
                      "localPart": "SensorResponse",
                      "namespaceURI": "http://example.com/ksml"
                    },
                    "value": {
                      "id": request_data.get("id", "unknown"),
                      "temperature": "25"
                    }
                  }
                ]
              }
            }
          }
        expression: result
        resultType: soap

    pipelines:
      process_soap:
        from: soap_input
        via:
          # Log input
          - type: peek
            forEach:
              code: |
                log.info("SOAP request: key={}", key)

          # Transform to response
          - type: transformValue
            mapper: create_response

          # Log output
          - type: peek
            forEach:
              code: |
                log.info("SOAP response: key={}", key)

        to: soap_output
    ```

## Converting Between Data Formats

KSML makes it easy to convert between different data formats using the `convertValue` operation.

- Use this `examples/producer.yaml` that produces AVRO messages:

??? info "Producer definition (click to expand)"

    ```yaml
    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate data matching SensorData schema
          value = {
            "name": key,
            "timestamp": str(round(time.time()*1000)),
            "value": str(random.randrange(0, 100)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"])
          }

          # Occasionally send null messages for testing
          if random.randrange(20) == 0:
            value = None
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Generate as JSON, KSML will convert to AVRO

    producers:
      # Produce AVRO sensor data messages every 3 seconds
      sensordata_avro_producer:
        generator: generate_sensordata_message
        interval: 3s
        to:
          topic: sensor_data_avro
          keyType: string
          valueType: avro:SensorData
    ```

- Use this `examples/processor.yaml` that demonstrates converting between several formats:

??? info "Processing definition for converting between multiple formats (click to expand)"

    ```yaml
    streams:
      avro_input:
        topic: sensor_data_avro
        keyType: string
        valueType: avro:SensorData
        offsetResetPolicy: latest

      json_output:
        topic: sensor_data_converted_formats
        keyType: string
        valueType: json

    pipelines:
      format_conversion:
        from: avro_input
        via:
          # Log original AVRO data
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("Original AVRO: sensor={}, type={}, value={}{}",
                          value.get("name"), value.get("type"), value.get("value"), value.get("unit"))
                else:
                  log.info("Received null AVRO message with key={}", key)

          # Convert from Avro to JSON
          - type: convertValue
            into: json
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("Converted to JSON: sensor={}, city={}", value.get("name"), value.get("city"))
                else:
                  log.info("JSON conversion result: null")

          # Convert from JSON to string to see serialized representation
          - type: convertValue
            into: string
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("As string (first 100 chars): {}", str(value)[:100])
                else:
                  log.info("String conversion result: null")

          # Convert back to JSON for output
          - type: convertValue
            into: json
          - type: peek
            forEach:
              code: |
                if value is not None:
                  log.info("Final JSON output: sensor={}, owner={}", value.get("name"), value.get("owner"))
                else:
                  log.info("Final conversion result: null")

        to: json_output
    ```

### Implicit Conversion

KSML can also perform implicit conversion when writing to a topic with a different format than the current message format:

```yaml
pipelines:
  implicit_conversion:
    from: avro_input  # Stream with Avro format
    to: xml_output    # Stream with XML format
    # The conversion happens automatically
```

## Advanced: Working with Multiple Formats in a Single Pipeline

In real-world scenarios, you might need to process data from multiple sources with different formats. Here's an example that combines JSON and Avro data.

- Use this `examples/producer.yaml` that produces AVRO and JSON messages:

??? info "Producer definition (click to expand)"

    ```yaml
    functions:
      generate_device_config:
        type: generator
        globalCode: |
          import time
          import random
          configCounter = 0
        code: |
          global configCounter

          device_id = "sensor" + str(configCounter)
          configCounter = (configCounter + 1) % 10

          # Generate device configuration as JSON
          config = {
            "device_id": device_id,
            "threshold": random.randrange(50, 90),
            "alert_level": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "calibration_factor": round(random.uniform(0.8, 1.2), 2),
            "last_maintenance": str(round(time.time() * 1000))
          }
        expression: (device_id, config)
        resultType: (string, json)

      generate_sensor_reading:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor" + str(sensorCounter)
          sensorCounter = (sensorCounter + 1) % 10

          # Generate sensor reading data that will be output as AVRO
          reading = {
            "name": key,
            "timestamp": str(round(time.time() * 1000)),
            "type": random.choice(["TEMPERATURE", "HUMIDITY", "PRESSURE"]),
            "unit": random.choice(["C", "F", "%", "Pa"]),
            "value": str(random.randrange(0, 100)),
            "color": random.choice(["black", "blue", "red", "yellow", "white"]),
            "owner": random.choice(["Alice", "Bob", "Charlie", "Dave", "Evan"]),
            "city": random.choice(["Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"])
          }
        expression: (key, reading)
        resultType: (string, json)  # Generate as JSON, output as AVRO

    producers:
      # Produce JSON device configuration every 10 seconds
      device_config_producer:
        generator: generate_device_config
        interval: 10s
        to:
          topic: device_config
          keyType: string
          valueType: json

      # Produce AVRO sensor readings every 3 seconds
      sensor_reading_producer:
        generator: generate_sensor_reading
        interval: 3s
        to:
          topic: sensor_readings
          keyType: string
          valueType: avro:SensorData%
        ```

- Use this `examples/processor.yaml` that works with multiple formats in a single pipeline:

??? info "Processor definition for working with multiple formats in a single pipeline (click to expand)"

    ```yaml
    streams:
      avro_sensor_stream:
        topic: sensor_readings
        keyType: string
        valueType: avro:SensorData
        offsetResetPolicy: latest

      json_config_stream:
        topic: device_config
        keyType: string
        valueType: json
        offsetResetPolicy: latest

      combined_output:
        topic: combined_sensor_data
        keyType: string
        valueType: json

    pipelines:
      # Pipeline 1: Process AVRO data and convert to JSON
      avro_processing:
        from: avro_sensor_stream
        via:
          # Log AVRO input
          - type: peek
            forEach:
              code: |
                log.info("AVRO sensor: name={}, type={}, value={}{}",
                        value.get("name"), value.get("type"),
                        value.get("value"), value.get("unit"))

          # Add a source field to identify the format
          - type: transformValue
            mapper:
              type: valueTransformer
              code: |
                result = dict(value) if value else {}
                result["source_format"] = "AVRO"
              expression: result
              resultType: json

        to: combined_output

      # Pipeline 2: Process JSON config data
      json_processing:
        from: json_config_stream
        via:
          # Log JSON input
          - type: peek
            forEach:
              code: |
                log.info("JSON config: device={}, threshold={}, alert={}",
                        key, value.get("threshold"), value.get("alert_level"))

          # Transform to sensor-like format with source field
          - type: transformValue
            mapper:
              type: valueTransformer
              code: |
                result = {
                  "name": key,
                  "type": "CONFIG",
                  "threshold": value.get("threshold"),
                  "alert_level": value.get("alert_level"),
                  "source_format": "JSON"
                }
              expression: result
              resultType: json

        to: combined_output
    ```


## Conclusion

In this tutorial, you've learned how to work with different data formats in KSML, including:

- Specifying data formats for streams
- Working with JSON data
- Using Avro schemas for data validation
- Processing CSV data
- Converting between different formats

These skills will help you build flexible and interoperable data pipelines that can work with a variety of data sources and destinations.

## Next Steps

- Explore the [Logging and Monitoring](logging-monitoring.md) tutorial to learn how to add effective logging to your pipelines
- Check out the [Intermediate Tutorials](../intermediate/index.md) for more advanced KSML features
- Review the [KSML Examples](../../resources/examples-library.md) for more examples of working with different data formats
