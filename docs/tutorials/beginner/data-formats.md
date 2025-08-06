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

- Create `docker-compose.yml` with schema registry and pre-created topics 

??? info "Docker Compose Configuration (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup-with-sr/docker-compose.yml"
    %}
    ```

- Create `kowl-ui-config.yaml` for Kafka UI:

??? info "Kafka UI Configuration (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup-with-sr/kowl-ui-config.yaml"
    %}
    ```

- Create `examples/ksml-runner.yaml` with Avro configuration:

??? info "KSML Runner Configuration (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup-with-sr/examples/ksml-runner.yaml"
    %}
    ```

- For each example, create `producer.yaml` and `processor.yaml` files and reference them from `ksml-runner.yaml`
- Restart KSML: `docker compose down & docker compose up -d && docker compose logs ksml -f` (which is faster than `docker compose restart ksml`)


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
