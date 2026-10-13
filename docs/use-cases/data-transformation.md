# Data Transformation with KSML

This tutorial demonstrates how to build a data transformation pipeline using KSML. You'll learn how to convert data
between different formats, enrich data with additional information, and handle complex transformations.

## Introduction

Data transformation is a fundamental use case for stream processing. It allows you to:

- Convert data between different formats (JSON, XML, Avro, etc.)
- Normalize and clean data from various sources
- Enrich data with additional context or reference information
- Restructure data to meet downstream system requirements
- Filter out unnecessary information

In this tutorial, we'll build a data transformation pipeline that processes customer data from a legacy system,
transforms it into a standardized format, enriches it with additional information, and makes it available for downstream
applications.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)
- Have a basic understanding of [Joins](../tutorials/intermediate/joins.md) for data enrichment
- Have [Docker Compose KSML environment setup running](../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic legacy_customer_data && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic customer_segments && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic standardized_customer_data && \
    ```

## The Use Case

Imagine you're working with a company that has acquired another business. You need to integrate customer data from the
acquired company's legacy system into your modern data platform. The legacy data:

- Is in a different format (XML) than your system (JSON)
- Uses different field names and conventions
- Contains some fields you don't need
- Is missing some information that you need to add from reference data

## Defining the Data Models

### Source Data (XML)

The legacy system provides customer data in XML format:

```xml

<customer>
    <cust_id>12345</cust_id>
    <fname>John</fname>
    <lname>Doe</lname>
    <dob>1980-01-15</dob>
    <addr>
        <street>123 Main St</street>
        <city>Anytown</city>
        <state>CA</state>
        <zip>90210</zip>
    </addr>
    <phone>555-123-4567</phone>
    <legacy_segment>A</legacy_segment>
    <account_created>2015-03-20</account_created>
</customer>
```

### Reference Data (JSON)

You have a reference table topic with segment code (key) mappings to segment details (value):

| Key | Value                                                                                        |
|-----|----------------------------------------------------------------------------------------------|
| A   | `{"segment_name": "Premium", "discount_tier": "Tier 1", "marketing_group": "High Value"}`    |
| B   | `{"segment_name": "Standard", "discount_tier": "Tier 2", "marketing_group": "Medium Value"}` |
| C   | `{"segment_name": "Basic", "discount_tier": "Tier 3", "marketing_group": "Growth Target"}`   |

### Target Data (JSON)

You want to transform the data into this format:

```json
{
  "customer_id": "12345",
  "name": {
    "first": "John",
    "last": "Doe"
  },
  "contact_info": {
    "email": "john.doe@example.com",
    "phone": "555-123-4567",
    "address": {
      "street": "123 Main St",
      "city": "Anytown",
      "state": "CA",
      "postal_code": "90210",
      "country": "USA"
    }
  },
  "birth_date": "1980-01-15",
  "customer_since": "2015-03-20",
  "segment": "Premium",
  "marketing_preferences": {
    "group": "High Value",
    "discount_tier": "Tier 1"
  },
  "metadata": {
    "source": "legacy_system",
    "last_updated": "2023-07-01T12:00:00Z"
  }
}
```

The diagram below shows where each target field comes from. Most fields are copied or renamed from the XML, the segment details come from the reference table, and a few fields are generated:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Sources
    subgraph XML["Source XML"]
        direction TB
        X1["cust_id"]
        X2["fname, lname"]
        X3["dob"]
        X4["account_created"]
        X5["phone"]
        X6["addr: street, city, state, zip"]
        X7["legacy_segment"]
    end
    subgraph REF["Reference table"]
        direction TB
        R1["segment_name"]
        R2["marketing_group, discount_tier"]
    end
    subgraph GEN["Generated"]
        direction TB
        G1["first.last@example.com"]
        G2["country: USA"]
        G3["source, current time"]
    end

    %% Target
    subgraph JSON["Target JSON"]
        direction TB
        T1["customer_id"]
        T2["name.first, name.last"]
        T3["birth_date"]
        T4["customer_since"]
        T5["contact_info.phone"]
        T6["contact_info.address<br/>street, city, state, postal_code"]
        T9["contact_info.email"]
        T10["contact_info.address.country"]
        T7["segment"]
        T8["marketing_preferences<br/>group, discount_tier"]
        T11["metadata<br/>source, last_updated"]
    end

    %% Mappings
    X1 --> T1
    X2 --> T2
    X3 --> T3
    X4 --> T4
    X5 --> T5
    X6 -->|"zip renamed"| T6
    X7 -.->|"join key"| REF
    R1 --> T7
    R2 --> T8
    G1 --> T9
    G2 --> T10
    G3 --> T11

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class X1,X2,X3,X4,X5,X6,X7 message
    class R1,R2 topic
    class G1,G2,G3 partial
    class T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11 result
    style XML fill:none,stroke:#616161,stroke-dasharray:5 5
    style REF fill:none,stroke:#616161,stroke-dasharray:5 5
    style GEN fill:none,stroke:#616161,stroke-dasharray:5 5
    style JSON fill:none,stroke:#616161,stroke-dasharray:5 5
```

## Creating the KSML Definition

Now, let's create our KSML definition file:

??? info "Data transformation processor (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/data-transformation/data-transformation.yaml" %}
    ```

The pipeline changes the message key twice. It first uses the segment code as the key, so the message can be joined with the `customer_segments` table, and then sets the key back to the customer ID. The diagram below follows one example customer through the pipeline:

<div class="diagram-w50" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Input[(legacy_customer_data)]
    M1@{ shape: doc, label: "<b>key: 12345</b><br/>XML customer, John Doe<br/>legacy_segment: A" }

    %% Re-key for the join
    Rekey1["<code>transformKey</code><br/>key = legacy_segment"]
    M2@{ shape: doc, label: "<b>key: A</b><br/>same XML customer" }

    %% Join with the reference table
    Table[(customer_segments<br/>table)]
    Ref@{ shape: docs, label: "<b>A</b>: Premium, Tier 1<br/><b>B</b>: Standard, Tier 2<br/><b>C</b>: Basic, Tier 3" }
    Join["<code>leftJoin</code><br/>segment_reference"]
    M3@{ shape: doc, label: "<b>key: A</b><br/>customer + segment_info<br/>Premium, Tier 1, High Value" }

    %% Restructure
    Transform["<code>transformValue</code><br/>transform_customer<br/>rename, nest,<br/>add email and country"]
    M4@{ shape: doc, label: "<b>key: A</b><br/>target JSON<br/>segment: Premium" }

    %% Re-key back
    Rekey2["<code>transformKey</code><br/>key = customer_id"]
    M5@{ shape: doc, label: "<b>key: 12345</b><br/>target JSON" }
    Output[(standardized_customer_data)]

    %% Data Flow
    Input --> M1
    M1 --> Rekey1
    Rekey1 --> M2
    M2 --> Join
    Table --> Ref
    Ref -.->|"lookup by key A"| Join
    Join --> M3
    M3 --> Transform
    Transform --> M4
    M4 --> Rekey2
    Rekey2 --> M5
    M5 --> Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Input,Output,Table topic
    class Rekey1,Join,Transform,Rekey2 operation
    class M1,M2,M3,Ref message
    class M4,M5 result
```

</div>

The two `peek` steps that only log the message are left out of the diagram. Because it is a left join, a customer whose segment is not in the table yet is still processed: its `segment` and marketing fields are set to `Unknown`.

## Setting up two producers for test data

To test out the topology above, we create two test data producers.

The first producer is a _single shot producer_ that generates data for the `customer_segments` topic:

??? info "Segment data producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/data-transformation/segment-data-producer.yaml" %}
    ```

The second producer produces a message every second to the `legacy_customer_data` topic, using a randomly chosen
segment:

??? info "Customer data producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/data-transformation/customer-data-producer.yaml" %}
    ```

## Running the Application

To run the application:

1. Save the processor definition to
   [`data-transformation.yaml`](#creating-the-ksml-definition).
2. Save the producers to
   [`segment-data-producer.yaml`](#setting-up-two-producers-for-test-data)
   and
   [`customer-data-producer.yaml`](#setting-up-two-producers-for-test-data).
3. Set up your `ksml-runner.yaml` configuration, pointing to your Kafka installation.

??? info "KSML runner configuration (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/data-transformation/ksml-runner.yaml" %}
    ```

4. Start the KSML runner. It runs all three definitions together:

      - the `customer_segment_producer` writes the three segments to the `customer_segments` topic, one every 10 seconds
      - the `legacy_customer_data_producer` writes a customer to the `legacy_customer_data` topic every second
      - the `customer_transformation_pipeline` transforms the customers

5. Monitor the `standardized_customer_data` output topic to see the transformed data.

## Advanced Transformation Techniques

### Handling Missing or Invalid Data

In real-world scenarios, source data often has missing or invalid fields. You can enhance the transformation function to
handle these cases:

```python
# Check if a field exists before accessing it
birth_date = value.get("dob") if value.get("dob") is not None else "Unknown"

# Provide default value in case dictionary key does not exist
state = address.get("state", "N/A")

# Validate data
if phone and not phone.startswith("555-"):
    log.warn("Invalid phone format for customer {}: {}", customer_id, phone)
    phone = "Unknown"
```

### Schema Evolution Handling

To handle changes in the source or target schema over time:

```python
# Version-aware transformation
schema_version = value.get("version", "1.0")
if schema_version == "1.0":
    # Original transformation logic
    value = transform_v1(value)
elif schema_version == "2.0":
    # Updated transformation logic for new schema
    value = transform_v2(value)
else:
    log.error("Unknown schema version: {}", schema_version)
```

## Conclusion

In this tutorial, you've learned how to:

- Transform data between different formats (XML to JSON)
- Restructure data to match a target schema
- Enrich data with information from reference sources
- Handle missing or derived fields
- Process and validate data during transformation

Data transformation is a powerful use case for KSML, allowing you to integrate data from various sources and prepare it
for downstream applications without complex coding.

## Next Steps

- Learn about [Real-Time Analytics](real-time-analytics.md) to analyze your transformed data
- Explore [Event-Driven Applications](event-driven-applications.md) to trigger actions based on data changes
- Check out [External Integration](../tutorials/advanced/external-integration.md) for connecting to external systems
