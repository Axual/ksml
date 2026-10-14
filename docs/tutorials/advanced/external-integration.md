# Integration with External Systems in KSML

This tutorial covers how to integrate KSML with external systems like databases and APIs using JSON data formats for better observability.

## Introduction

Stream processing often needs external data. This tutorial shows patterns for enriching events with database lookups, API calls, and async integrations.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic enriched_user_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic enriched_product_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic order_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic external_requests && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic external_responses && \
    ```

## Integration Patterns


The examples below show three ways to bring external data into a stream. They differ in when the external system is contacted and who waits for it:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    subgraph API["API enrichment"]
        direction TB
        A1[(Input topic)] --> A2["Python function"]
        A2 <-->|"call per event,<br/>wait for the answer"| A3["External API"]
        A2 --> A4[(Enriched topic)]
    end
    subgraph DB["Database lookup"]
        direction TB
        B0["Database"] -.->|"load once"| B3[("Local state store")]
        B1[(Input topic)] --> B2["Python function"]
        B2 <-->|"local lookup<br/>per event"| B3
        B2 --> B4[(Enriched topic)]
    end
    subgraph ASYNC["Async integration"]
        direction TB
        C1[(Input topic)] --> C2["Python function"]
        C2 --> C3[(Request topic)]
        C3 --> C4["External system"]
        C4 --> C5[(Response topic)]
    end
    API ~~~ DB ~~~ ASYNC

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class A1,A4,B1,B4,C1,C3,C5 topic
    class A2,B2,C2 operation
    class A3,B0,C4 external
    class B3 store
    style API fill:none,stroke:#616161,stroke-dasharray:5 5
    style DB fill:none,stroke:#616161,stroke-dasharray:5 5
    style ASYNC fill:none,stroke:#616161,stroke-dasharray:5 5
```

### API Enrichment Pattern

The API enrichment pattern calls external REST APIs to add additional data to streaming events. This is useful when you need real-time data that can't be cached locally.

**What it does:**

- **Produces user events**: Creates events (login, purchase, page_view) with user IDs, session info, device details, page URLs, referrer sources
- **Calls mock API**: For each user_id, fetches profile from hardcoded lookup table simulating external REST API call with user details
- **Handles API failures**: Uses try-catch to gracefully handle missing users, returning fallback "Unknown User" profile data
- **Enriches with profiles**: Combines original event data with fetched profile (name, tier, location, preferences, lifetime_value) 
- **Outputs enriched events**: Returns JSON combining event details with user profile, API call timing, and computed recommendations

The diagram below follows two example events. Each event waits for its own API call before the next one is processed:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(user_events)]
    Events@{ shape: docs, label: "<b>event_000001</b><br/>login, user_001<br/><b>event_000002</b><br/>purchase, user_004" }

    %% Processing function and the API it calls
    Function["<code>mapValues</code><br/>enrich_with_api_data"]
    API["Mock user profile API<br/><code>get_user_profile(user_id)</code><br/>about 100 ms per call"]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>event_000001</b><br/>login, user_001<br/>profile: Alice Johnson<br/>premium, New York<br/>tier level 3, high value<br/>status SUCCESS"]
        Step2["<b>event_000002</b><br/>purchase, user_004<br/>profile: Diana Wilson<br/>basic, Sydney<br/>tier level 1, not high value<br/>status SUCCESS"]
        Step3["<b>if the API call fails</b><br/>no profile<br/>fallback: basic tier<br/>status FAILED"]
        Step1 --> Step2 --> Step3
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(enriched_user_events)]

    %% Data Flow
    Topic --> Events
    Events --> Function
    Function <-.->|"call & wait"| API
    Function --> Steps
    Steps --> Filter
    Filter -->|"event + profile,<br/>metrics, recommendations"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Function,Filter operation
    class Events message
    class API external
    class Step1,Step2 result
    class Step3 partial
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

The API latency adds up: with a 100 ms call per event, one stream thread can enrich at most about 10 events per second.


**Key concepts demonstrated:**

- Making external API calls from KSML functions with JSON data structures
- Handling API failures gracefully with structured fallback strategies
- Managing API latency and timeouts in stream processing
- Enriching events with external data while maintaining stream processing semantics

??? info "User Events Producer (API enrichment demo) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/producer-api-enrichment.yaml" %}
    ```

??? info "API Enrichment Processor (external API calls) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/processor-api-enrichment.yaml" %}
    ```

**API enrichment benefits:**

- **Real-time enrichment**: Access to the most current external data
- **Flexible data sources**: Can integrate with any REST API
- **Graceful degradation**: Continues processing even when external systems fail
- **Simple implementation**: Straightforward request-response pattern

### Database Lookup Pattern

This pattern shows how to enrich streaming events with data that would normally come from a database.

**What happens in this example:**

Imagine you have an e-commerce website. Users view products, add them to cart, and make purchases. Your stream only has basic event data like `{"product_id": "PROD001", "event_type": "purchased", "quantity": 2}`. But you want to know the product name, price, and category too.

**The Producer** creates these basic product events every 2 seconds:
```json
{"product_id": "PROD001", "event_type": "purchased", "quantity": 2, "user_id": "user_1234"}
```

**The Processor** enriches each event by:

1. **First time only**: Loads a product catalog into memory (like a mini database):
       - PROD001 → "Wireless Headphones", $99.99, "Electronics"  
       - PROD002 → "Coffee Mug", $12.50, "Kitchen"
2. **For each event**: Looks up the product_id and adds the details:
   ```json
   {
     "product_id": "PROD001", 
     "event_type": "purchased", 
     "quantity": 2,
     "enriched_data": {
       "name": "Wireless Headphones",
       "category": "Electronics", 
       "unit_price": 99.99,
       "total_price": 199.98
     }
   }
   ```

This way you get rich product information without hitting a database for every single event.

The diagram below follows three example events. The first event also triggers loading the catalog into the state store; after that, every event is enriched with a local lookup:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(product_events)]
    Events@{ shape: docs, label: "<b>key: PROD001</b><br/>purchased, quantity 2<br/><b>key: PROD002</b><br/>viewed, quantity 1<br/><b>key: PROD005</b><br/>added_to_cart, quantity 3" }

    %% Loading and lookup
    Load["<code>peek</code><br/>load_product_reference_data<br/>first event only"]
    Catalog["Product catalog<br/>simulated database<br/>PROD001 ... PROD005"]
    Store[("product_reference_store<br/>key: product_id<br/>value: name, price, category")]
    Function["<code>mapValues</code><br/>enrich_with_product_data"]
    subgraph Steps["Event by event"]
        direction LR
        Step1["<b>PROD001</b><br/>purchased, quantity 2<br/>Wireless Headphones<br/>99.99 × 2<br/>total_price 199.98"]
        Step2["<b>PROD002</b><br/>viewed, quantity 1<br/>Coffee Mug<br/>no purchase or cart event<br/>total_price 0.0"]
        Step3["<b>PROD005</b><br/>added_to_cart, quantity 3<br/>Smartphone<br/>699.99 × 3<br/>total_price 2099.9700000000003"]
        Step1 --> Step2 --> Step3
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(enriched_product_events)]

    %% Data Flow
    Topic --> Events
    Events --> Load
    Catalog -.->|"read once"| Load
    Load -.->|"put 5 products"| Store
    Load --> Function
    Function <-.->|"get product_id"| Store
    Function --> Steps
    Steps --> Filter
    Filter -->|"3 enriched events"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Load,Function,Filter operation
    class Events message
    class Catalog external
    class Store store
    class Step1,Step2,Step3 result
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

The `total_price` is not rounded, so a multiplication like 699.99 × 3 can give a long floating point number such as `2099.9700000000003`.


??? info "Product Events Producer (database lookup demo) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/producer-product-events.yaml" %}
    ```

??? info "Database Lookup Processor (cached reference data) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/processor-database-lookup.yaml" %}
    ```

**Key concepts demonstrated:**

- Loading reference data into state stores as cache
- Fast local lookups without external database calls
- JSON enrichment with cached data

### Async Integration Pattern

The async integration pattern uses separate Kafka topics for communication with external systems, providing loose coupling and better resilience.

**What it does:**

- **Produces order events**: Creates orders with status (created, paid, shipped) containing customer info, amounts, payment methods, business context
- **Filters paid orders**: Only processes orders with status="paid", creates external payment processing requests with correlation IDs
- **Sends to request topic**: Outputs JSON payment requests to external_requests topic with order details, customer tier, processing priority
- **Simulates responses**: A second pipeline reads the requests, simulates a payment result, and writes it to `external_responses`
- **Outputs results**: Returns JSON combining original order with external processing results, transaction IDs, and follow-up actions

The example runs two pipelines. The first one (`external_request_pipeline`) turns paid orders into payment requests. The second one (`external_response_pipeline`) stands in for the external payment system: it reads the requests, simulates a payment result and writes the responses. The diagram below follows four example orders:

<div class="diagram-w55" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example events
    Topic[(order_events)]
    Events@{ shape: docs, label: "<b>ORDER_0001</b><br/>created, 80.00<br/><b>ORDER_0002</b><br/>paid, 250.00<br/><b>ORDER_0003</b><br/>shipped, 120.00<br/><b>ORDER_0004</b><br/>paid, 420.00" }

    %% Request pipeline
    subgraph Requests["external_request_pipeline"]
        Filter{"<code>filter</code><br/>filter_paid_orders<br/>status == paid"}
        Create["<code>transformKeyValue</code><br/>create_external_request<br/>new key REQ_order_timestamp"]
    end
    Drop["❌ Dropped<br/>ORDER_0001, ORDER_0003"]
    RequestCards@{ shape: docs, label: "<b>REQ_ORDER_0002_...</b><br/>250.00<br/>priority normal, high value<br/><b>REQ_ORDER_0004_...</b><br/>420.00<br/>priority high, high value" }
    RequestTopic[(external_requests)]

    %% Simulated external system
    subgraph Payment["simulated payment system"]
        Process["<code>mapValues</code><br/>process_external_response<br/>random result, approved:<br/>90% if high value<br/>95% otherwise"]
    end
    ResponseCards@{ shape: docs, label: "for example:<br/><b>REQ_ORDER_0002_...</b><br/>success, TXN_..., fee 7.25<br/><b>REQ_ORDER_0004_...</b><br/>failed, Insufficient funds" }
    ResponseTopic[(external_responses)]

    %% Data Flow
    Topic --> Events
    Events --> Filter
    Filter -->|"paid"| Create
    Filter -.->|"other status"| Drop
    Create --> RequestCards
    RequestCards --> RequestTopic
    RequestTopic --> Process
    Process --> ResponseCards
    ResponseCards --> ResponseTopic

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,RequestTopic,ResponseTopic topic
    class Filter,Create,Process operation
    class Events,RequestCards message
    class ResponseCards result
    class Drop dropped
    style Requests fill:none,stroke:#616161,stroke-dasharray:5 5
    style Payment fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

Both pipelines keep the request key (`REQ_...`), and each response carries the `correlation_id` of its order. In a real setup, the second pipeline is replaced by the external system, and another KSML pipeline reads `external_responses`.


**Key concepts demonstrated:**

- Creating request topics for external system communication with JSON payloads
- Generating correlation IDs for request-response tracking
- Processing responses asynchronously through separate topics with structured data
- Building event-driven integration patterns using JSON messaging

??? info "Order Events Producer (async integration demo) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/producer-orders.yaml" %}
    ```

??? info "Async Integration Processor (request-response pattern) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/external-integration/processor-async-integration.yaml" %}
    ```

**Async integration features:**

- **Loose coupling**: External systems communicate via topics, not direct calls
- **Scalability**: Multiple consumers can process requests and responses
- **Reliability**: Messages are persisted in Kafka topics
- **Monitoring**: Easy to monitor request/response flows through topic metrics

### Key Metrics to Track

Monitor these metrics to ensure integration health:

- Track **latency** of external API calls and database queries
- Monitor the **percentage of successful external system interactions**
- Track different types of **errors** (timeouts, authentication, etc.)
- For lookup patterns, monitor **state store cache hit rates**
- For async patterns, monitor request and response topic lag

### Alert Configuration

Set up alerts for integration issues:

```yaml
alerts:
  high_api_latency:
    condition: avg_api_response_time > 5000ms
    duration: 2_minutes
    severity: warning
    
  external_system_down:
    condition: api_error_rate > 50%
    duration: 1_minute
    severity: critical
    
  cache_miss_rate_high:
    condition: cache_miss_rate > 20%
    duration: 5_minutes
    severity: warning
```

## Advanced Integration Patterns

### Hybrid Event-Database Pattern

Combine streaming events with database lookups for complex business logic:

```yaml
functions:
  hybrid_processor:
    type: valueTransformer
    code: |
      # Process streaming event
      event_data = parse_event(value)
      
      # Look up reference data from cached database
      reference_data = get_reference_data(event_data.entity_id)
      
      # Apply business rules using both streaming and reference data
      result = apply_business_rules(event_data, reference_data)
      
      # Optionally trigger external API call based on result
      if result.requires_notification:
        send_notification(result)
      
      return result
```

### Multi-System Coordination

Coordinate operations across multiple external systems:

```yaml
functions:
  multi_system_coordinator:
    type: keyValueTransformer
    code: |
      # Create requests for multiple external systems
      payment_request = create_payment_request(value)
      inventory_request = create_inventory_request(value)
      
      # Use correlation ID to track related requests
      correlation_id = generate_correlation_id()
      
      # Return multiple outputs for different external systems
      return [
        (f"payment_{correlation_id}", payment_request),
        (f"inventory_{correlation_id}", inventory_request)
      ]
```

## Conclusion

External integration is vital for stream processing. Using API enrichment, database lookups, and async integration, you can build scalable KSML applications that enhance streaming data without sacrificing performance.

Choose the right pattern: API enrichment for real-time data, database lookups for reference data, and async integration for multi-system workflows.