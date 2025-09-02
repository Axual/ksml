# KSML Definition Reference

This reference guide covers the structure and organization of KSML definition files. A KSML definition is a YAML file that describes your complete stream processing application.

## KSML File Structure

KSML supports two main patterns for applications. Choose the pattern that matches your use case:

### Stream Processing Applications

Process data from input topics to output topics:

```yaml
# Application metadata (optional)
name: "order-processor"         # Optional
version: "1.0.0"               # Optional  
description: "Process orders"  # Optional

# Data sources and sinks (optional - can be inlined)
streams:       # KStream definitions (optional)
tables:        # KTable definitions (optional)
globalTables:  # GlobalKTable definitions (optional)

# State storage (optional - only if needed)
stores:        # State store definitions (optional)

# Processing logic
functions:     # Python function definitions (optional)
pipelines:     # Data flow pipelines (REQUIRED)
```

**Required sections:** `pipelines`  
**Optional sections:** All others (streams/tables can be inlined in pipelines)

### Data Generation Applications  

Generate and produce data to Kafka topics:

```yaml
# Application metadata (optional)
name: "sensor-data-generator"   # Optional
version: "1.0.0"               # Optional
description: "Generate sensor data"  # Optional

# Processing logic
functions:     # Python function definitions (REQUIRED - must include generator functions)
producers:     # Data producer definitions (REQUIRED)
```

**Required sections:** `functions` (with generator functions), `producers`  
**Optional sections:** `name`, `version`, `description`

**Note:** Data generation applications don't use streams/tables/stores sections since they only produce data.

## Application Metadata

Optional metadata to describe your KSML application:

| Property      | Type   | Required | Description                          |
|---------------|--------|----------|--------------------------------------|
| `name`        | String | No       | The name of the KSML definition      |
| `version`     | String | No       | The version of the KSML definition   |
| `description` | String | No       | A description of the KSML definition |

```yaml
name: "order-processing-app"
version: "1.2.3"
description: "Processes orders and enriches them with customer data"
```

## Data Sources and Targets

KSML supports three types of data streams based on Kafka Streams concepts. Each stream type has different characteristics and use cases for processing streaming data.

### Streams (KStream)

**Use for:** Event-based processing where each record is an independent event.

```yaml
streams:
  user_clicks:
    topic: user-clicks
    keyType: string
    valueType: json
    offsetResetPolicy: earliest  # Optional
    timestampExtractor: click_timestamp_extractor  # Optional
    partitioner: click_partitioner  # Optional
```

**Key characteristics:**

- Records are immutable and processed individually
- Each record represents an independent event or fact
- Records arrive in order and are processed one at a time
- Ideal for: user actions, sensor readings, transactions, logs

| Property             | Type   | Required | Description                                                                                                               |
|----------------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to                                                                                  |
| `keyType`            | String | Yes      | The type of the record key                                                                                                |
| `valueType`          | String | Yes      | The type of the record value                                                                                              |
| `offsetResetPolicy`  | String | No       | The offset reset policy. Valid values: earliest, latest, none, by_duration:<duration> (e.g., by_duration:PT1H for 1 hour). Default: Kafka Streams default (typically latest) |
| `timestampExtractor` | String | No       | Function name to extract timestamps from records. Default: Kafka Streams default (message timestamp, fallback to current time)                                                                          |
| `partitioner`        | String | No       | Function name that determines message partitioning for this stream/table. Default: Kafka default (hash-based on key)                                                  |

#### Stream Example with `offsetResetPolicy`

- [`offsetResetPolicy` example](../tutorials/beginner/data-formats.md#working-with-avro-data)

#### Stream Example with `timestampExtractor`

- [`timestampExtractor` example](function-reference.md#timestampextractor)

### Tables (KTable)

**Use for:** State-based processing where records represent updates to entities.

```yaml
tables:
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: avro:UserProfile
    store: user_profiles_store  # Optional state store name
```

**Key characteristics:**

- Records with the same key represent updates to the same entity
- Only the latest record for each key is retained (compacted)
- Represents a changelog stream with the current state
- Ideal for: user profiles, inventory levels, configuration settings

| Property             | Type   | Required | Description                                                                                                               |
|----------------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from or write to                                                                                  |
| `keyType`            | String | Yes      | The type of the record key                                                                                                |
| `valueType`          | String | Yes      | The type of the record value                                                                                              |
| `offsetResetPolicy`  | String | No       | The offset reset policy. Valid values: earliest, latest, none, by_duration:<duration> (e.g., by_duration:PT1H for 1 hour). Default: Kafka Streams default (typically latest) |
| `timestampExtractor` | String | No       | Function name to extract timestamps from records. Default: Kafka Streams default (message timestamp, fallback to current time)                                                                          |
| `partitioner`        | String | No       | Function that determines message partitioning                                                                             |
| `store`              | String | No       | The name of the key/value state store to use. Default: Auto-created store using topic name                                                                              |

#### Table Example without `store`

- [Table example in Circuit Breaker Pattern](../tutorials/intermediate/error-handling.md#5-circuit-breaker-pattern)

#### Table Example with `store`

This example demonstrates using a custom inline state store for a table. The table uses custom persistence and caching settings, and the processor function accesses the table to enrich streaming data.

```yaml
--8<-- "definitions/reference/table-store-processor.yaml:11:22"
```

??? info "Producer - User Profile Data (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/table-store-producer.yaml"
    %}
    ```

??? info "Processor - Enrich Activity with Profiles (click to expand)"

    ```yaml
    {%
      include "../definitions/reference/table-store-processor.yaml"
    %}
    ```

The table `user_profiles` uses an inline store definition with custom settings:

- `persistent: true` - Data survives application restarts
- `caching: true` - Enables local caching for better performance  
- `logging: false` - Disables changelog topic creation

The enrichment function accesses the table via `stores: [user_profiles]` declaration and performs lookups using `user_profiles.get(user_id)`.

### Global Tables (GlobalKTable)

**Use for:** Reference data that needs to be available on all application instances.

```yaml
globalTables:
  product_catalog:
    topic: product-catalog
    keyType: string
    valueType: avro:Product
    store: product_catalog_store  # Optional state store name
```

**Key characteristics:**

- Fully replicated on each application instance (not partitioned)
- Allows joins without requiring co-partitioning
- Provides global access to reference data
- Ideal for: product catalogs, country codes, small to medium reference datasets

| Property             | Type   | Required | Description                                                                                                               |
|----------------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `topic`              | String | Yes      | The Kafka topic to read from                                                                                              |
| `keyType`            | String | Yes      | The type of the record key                                                                                                |
| `valueType`          | String | Yes      | The type of the record value                                                                                              |
| `offsetResetPolicy`  | String | No       | The offset reset policy. Valid values: earliest, latest, none, by_duration:<duration> (e.g., by_duration:PT1H for 1 hour). Default: Kafka Streams default (typically latest) |
| `timestampExtractor` | String | No       | Function name to extract timestamps from records. Default: Kafka Streams default (message timestamp, fallback to current time)                                                                          |
| `partitioner`        | String | No       | Function that determines message partitioning                                                                             |
| `store`              | String | No       | The name of the key/value state store to use. Default: Auto-created store using topic name                                                                              |


##### Global Table Example

- [GlobalTable Example](../tutorials/intermediate/joins.md#use-case-product-catalog-enrichment)

### Choosing the Right Stream Type

| If you need to...                                         | Consider using... |
|-----------------------------------------------------------|-------------------|
| Process individual events as they occur                   | KStream           |
| Maintain the latest state of entities                     | KTable            |
| Join with data that's needed across all partitions        | GlobalKTable      |
| Process time-ordered events                               | KStream           |
| Track changes to state over time                          | KTable            |
| Access reference data without worrying about partitioning | GlobalKTable      |

## Pipelines

Define how data flows through your application:

```yaml
pipelines:
  process_orders:
    from: orders
    via:
      - type: filter
        if: is_valid_order
      - type: mapValues
        mapper: enrich_order
    to: processed_orders
```

For complete pipeline documentation, see [Pipeline Reference](pipeline-reference.md).

## Functions

Define reusable Python logic for processing:

```yaml
functions:
  is_valid_order:
    type: predicate
    expression: value.get("total") > 0
```

For complete function documentation, see [Function Reference](function-reference.md).

## Operations

Operations are the building blocks that transform, filter, and process your data within pipelines:

```yaml
via:
  - type: filter        # Keep matching records
    if: is_valid_order
  - type: mapValues     # Transform record values
    mapper: enrich_order
  - type: join          # Combine with other streams
    with: customers
```

For complete operation documentation, see [Operation Reference](operation-reference.md).

## State Stores

Define persistent state stores for stateful operations:

```yaml
stores:
  session_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    caching: true
```

For details, see [State Store Reference](state-store-reference.md).

## Producers

Define data generators for testing and simulation:

```yaml
producers:
  test_producer:
    target: test_topic
    generator: generate_test_data
    interval: 1000
```

##### Producer Example

- [Producer Example](../getting-started/basics-tutorial.md#using-ksml-to-produce-messages)

