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

## Creating the KSML Definition

Now, let's create our KSML definition file:

??? info "Data transformation processor (click to expand)"

    ```yaml
    {%
      include "../definitions/use-cases/data-transformation/data-transformation.yaml"
    %}
    ```

## Setting up two producers for test data

To test out the topology above, we create two test data producers.

The first producer is a _single shot producer_ that generates data for the `customer_segments` topic:

??? info "Segment data producer (click to expand)"

    ```yaml
    {%
      include "../definitions/use-cases/data-transformation/segment-data-producer.yaml"
    %}
    ```

The second producer produces a message every second to the `legacy_customer_data` topic, using a randomly chosen
segment:

??? info "Customer data producer (click to expand)"

    ```yaml
    {%
      include "../definitions/use-cases/data-transformation/customer-data-producer.yaml"
    %}
    ```

## Running the Application

To run the application:

1. Save the processor definition to
   [`data_transformation.yaml`](../definitions/use-cases/data-transformation/data-transformation.yaml).
2. Save the producers to
   [`segment-data-producer.yaml`](../definitions/use-cases/data-transformation/segment-data-producer.yaml)
   and
   [`customer-data-producer.yaml`](../definitions/use-cases/data-transformation/customer-data-producer.yaml).
3. Set up your `ksml-runner.yaml` configuration, pointing to your Kafka installation.

??? info "KSML runner configuration (click to expand)"

    ```yaml
    {%
      include "../definitions/use-cases/data-transformation/ksml-runner.yaml"
    %}
    ```

4. Start the `customer_segment_producer` to produce the sample segment information to Kafka.
5. Start the `legacy_customer_data_producer` to produce some sample data to the input topic.
6. Start the `data_transformation` topology to initiate the continuous data transformation logic.
7. Monitor the output topic to see the transformed data.

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
