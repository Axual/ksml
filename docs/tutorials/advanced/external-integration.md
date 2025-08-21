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

### API Enrichment Pattern

The API enrichment pattern calls external REST APIs to add additional data to streaming events. This is useful when you need real-time data that can't be cached locally.

**What it does:**

- **Produces user events**: Creates events (login, purchase, page_view) with user IDs, session info, device details, page URLs, referrer sources
- **Calls mock API**: For each user_id, fetches profile from hardcoded lookup table simulating external REST API call with user details
- **Handles API failures**: Uses try-catch to gracefully handle missing users, returning fallback "Unknown User" profile data
- **Enriches with profiles**: Combines original event data with fetched profile (name, tier, location, preferences, lifetime_value) 
- **Outputs enriched events**: Returns JSON combining event details with user profile, API call timing, and computed recommendations

**Key concepts demonstrated:**

- Making external API calls from KSML functions with JSON data structures
- Handling API failures gracefully with structured fallback strategies
- Managing API latency and timeouts in stream processing
- Enriching events with external data while maintaining stream processing semantics

??? info "User Events Producer (API enrichment demo) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/producer-api-enrichment.yaml"
    %}
    ```

??? info "API Enrichment Processor (external API calls) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/processor-api-enrichment.yaml"
    %}
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

??? info "Product Events Producer (database lookup demo) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/producer-product-events.yaml"
    %}
    ```

??? info "Database Lookup Processor (cached reference data) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/processor-database-lookup.yaml"
    %}
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
- **Processes responses**: Reads responses from external_responses topic, matches by correlation_id, handles success/failure scenarios
- **Outputs results**: Returns JSON combining original order with external processing results, transaction IDs, and follow-up actions

**Key concepts demonstrated:**

- Creating request topics for external system communication with JSON payloads
- Generating correlation IDs for request-response tracking
- Processing responses asynchronously through separate topics with structured data
- Building event-driven integration patterns using JSON messaging

??? info "Order Events Producer (async integration demo) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/producer-orders.yaml"
    %}
    ```

??? info "Async Integration Processor (request-response pattern) - click to expand"

    ```yaml
    {%
      include "../../definitions/advanced-tutorial/external-integration/processor-async-integration.yaml"
    %}
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