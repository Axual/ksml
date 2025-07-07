# Data Transformation with KSML

This tutorial demonstrates how to build a data transformation pipeline using KSML. You'll learn how to convert data between different formats, enrich data with additional information, and handle complex transformations.

## Introduction

Data transformation is a fundamental use case for stream processing. It allows you to:

- Convert data between different formats (JSON, XML, Avro, etc.)
- Normalize and clean data from various sources
- Enrich data with additional context or reference information
- Restructure data to meet downstream system requirements
- Filter out unnecessary information

In this tutorial, we'll build a data transformation pipeline that processes customer data from a legacy system, transforms it into a standardized format, enriches it with additional information, and makes it available for downstream applications.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Be familiar with [Filtering and Transforming](../beginner/filtering-transforming.md)
- Have a basic understanding of [Joins](../intermediate/joins.md) for data enrichment

## The Use Case

Imagine you're working with a company that has acquired another business. You need to integrate customer data from the acquired company's legacy system into your modern data platform. The legacy data:

- Is in a different format (XML) than your system (JSON)
- Uses different field names and conventions
- Contains some fields you don't need
- Is missing some information that you need to add from reference data

## Setting Up the Environment

First, let's set up our environment with Docker Compose:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  ksml-runner:
    image: axual/ksml-runner:latest
    depends_on:
      - kafka
    volumes:
      - ./config:/config
      - ./definitions:/definitions
```

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

You have a reference table with segment mappings:

```json
{
  "A": {
    "segment_name": "Premium",
    "discount_tier": "Tier 1",
    "marketing_group": "High Value"
  },
  "B": {
    "segment_name": "Standard",
    "discount_tier": "Tier 2",
    "marketing_group": "Medium Value"
  },
  "C": {
    "segment_name": "Basic",
    "discount_tier": "Tier 3",
    "marketing_group": "Growth Target"
  }
}
```

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

```yaml
streams:
  legacy_customers:
    topic: legacy_customer_data
    keyType: string  # customer_id
    valueType: xml  # XML customer data

  segment_reference:
    topic: customer_segments
    keyType: string  # segment code
    valueType: json  # segment details

  transformed_customers:
    topic: standardized_customer_data
    keyType: string  # customer_id
    valueType: json  # transformed customer data

functions:
  transform_customer:
    type: mapValues
    parameters:
      - name: xml_customer
        type: object
      - name: segment_info
        type: object
    code: |
      import datetime

      # Extract values from XML
      customer_id = xml_customer.find("cust_id").text
      first_name = xml_customer.find("fname").text
      last_name = xml_customer.find("lname").text
      birth_date = xml_customer.find("dob").text
      phone = xml_customer.find("phone").text
      legacy_segment = xml_customer.find("legacy_segment").text
      customer_since = xml_customer.find("account_created").text

      # Extract address
      addr = xml_customer.find("addr")
      street = addr.find("street").text
      city = addr.find("city").text
      state = addr.find("state").text
      zip_code = addr.find("zip").text

      # Generate email (not in source data)
      email = f"{first_name.lower()}.{last_name.lower()}@example.com"

      # Get segment info from reference data
      segment_name = segment_info.get("segment_name") if segment_info else "Unknown"
      discount_tier = segment_info.get("discount_tier") if segment_info else "Unknown"
      marketing_group = segment_info.get("marketing_group") if segment_info else "Unknown"

      # Current timestamp for metadata
      current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

      # Create transformed customer object
      return {
        "customer_id": customer_id,
        "name": {
          "first": first_name,
          "last": last_name
        },
        "contact_info": {
          "email": email,
          "phone": phone,
          "address": {
            "street": street,
            "city": city,
            "state": state,
            "postal_code": zip_code,
            "country": "USA"  # Default value not in source
          }
        },
        "birth_date": birth_date,
        "customer_since": customer_since,
        "segment": segment_name,
        "marketing_preferences": {
          "group": marketing_group,
          "discount_tier": discount_tier
        },
        "metadata": {
          "source": "legacy_system",
          "last_updated": current_time
        }
      }

pipelines:
  customer_transformation_pipeline:
    from: legacy_customers
    via:
      - type: peek
        forEach:
          code: |
            log.info("Processing customer: {}", key)
      - type: leftJoin
        foreignKeyExtractor:
          code: value.find("legacy_segment").text
        with: segment_reference
      - type: mapValues
        mapper:
          code: transform_customer(value, foreignValue)
      - type: peek
        forEach:
          code: |
            log.info("Transformed customer: {}", key)
    to: transformed_customers
```

## Running the Application

To run the application:

1. Save the KSML definition to `definitions/data_transformation.yaml`
2. Create a configuration file at `config/application.properties` with your Kafka connection details
3. Start the Docker Compose environment: `docker-compose up -d`
4. Produce some sample data to the input topic
5. Monitor the output topic to see the transformed data

## Testing the Transformation

You can test the transformation by producing sample data to the input topic:

```bash
# Produce sample segment reference data
echo '{"A": {"segment_name": "Premium", "discount_tier": "Tier 1", "marketing_group": "High Value"}}' | \
  kafka-console-producer --broker-list localhost:9092 --topic customer_segments --property "parse.key=true" --property "key.separator=:"

# Produce sample customer data
echo 'A:<customer><cust_id>12345</cust_id><fname>John</fname><lname>Doe</lname><dob>1980-01-15</dob><addr><street>123 Main St</street><city>Anytown</city><state>CA</state><zip>90210</zip></addr><phone>555-123-4567</phone><legacy_segment>A</legacy_segment><account_created>2015-03-20</account_created></customer>' | \
  kafka-console-producer --broker-list localhost:9092 --topic legacy_customer_data --property "parse.key=true" --property "key.separator=:"
```

Then, consume from the output topic to see the transformed data:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic standardized_customer_data --from-beginning
```

## Advanced Transformation Techniques

### Handling Missing or Invalid Data

In real-world scenarios, source data often has missing or invalid fields. You can enhance the transformation function to handle these cases:

```python
# Check if a field exists before accessing it
birth_date = xml_customer.find("dob").text if xml_customer.find("dob") is not None else "Unknown"

# Provide default values
state = addr.find("state").text if addr.find("state") is not None else "N/A"

# Validate data
if phone and not phone.startswith("555-"):
  log.warn("Invalid phone format for customer {}: {}", customer_id, phone)
  phone = "Unknown"
```

### Batch Processing with Windowing

For large-scale data migration, you might want to process data in batches:

```yaml
- type: windowedBy
  timeDifference: 60000  # 1 minute window
- type: foreach
  code: |
    batch_size = len(values)
    log.info("Processing batch of {} customers", batch_size)
    # Process batch
```

### Schema Evolution Handling

To handle changes in the source or target schema over time:

```python
# Version-aware transformation
schema_version = xml_customer.get("version", "1.0")
if schema_version == "1.0":
  # Original transformation logic
elif schema_version == "2.0":
  # Updated transformation logic for new schema
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

Data transformation is a powerful use case for KSML, allowing you to integrate data from various sources and prepare it for downstream applications without complex coding.

## Next Steps

- Learn about [Real-Time Analytics](real-time-analytics.md) to analyze your transformed data
- Explore [Event-Driven Applications](event-driven-applications.md) to trigger actions based on data changes
- Check out [External Integration](../advanced/external-integration.md) for connecting to external systems
