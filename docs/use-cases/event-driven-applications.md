# Event-Driven Applications with KSML

This tutorial demonstrates how to build event-driven applications using KSML. You'll learn how to detect specific events
in your data streams and trigger appropriate actions in response.

## Introduction

Event-driven architecture is a powerful paradigm for building responsive, real-time applications. In this approach:

- Systems react to events as they occur
- Components communicate through events rather than direct calls
- Business logic is triggered by changes in state
- Applications can scale and evolve independently

KSML is particularly well-suited for event-driven applications because it allows you to:

- Process streams of events in real-time
- Detect complex patterns and conditions
- Transform events into actionable insights
- Trigger downstream processes automatically

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Filtering and Transforming](../tutorials/beginner/filtering-transforming.md)
- Have a basic understanding of [Complex Event Processing](../tutorials/advanced/complex-event-processing.md)
- Have [Docker Compose KSML environment setup running](../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic product_catalog && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic order_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic inventory_updates && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic reorder_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic inventory_alerts && \
    ```

## The Use Case

In this tutorial, we'll build an event-driven inventory management system for an e-commerce platform. The system will:

1. Monitor product inventory levels in real-time
2. Detect when items are running low
3. Generate reorder events for the procurement system
4. Alert warehouse staff about critical inventory situations
5. Log every inventory update, as a starting point for a real-time inventory dashboard

## Defining the Data Models

### Product Catalog (Reference Data)

Each product in the `product_catalog` table defines when to reorder, how much to reorder, and when to alert:

```json
{
  "id": "prod-123",
  "name": "Wireless Headphones",
  "category": "electronics",
  "price": 79.99,
  "reorder_threshold": 10,
  "reorder_quantity": 50,
  "critical_threshold": 5
}
```

### Order Events

```json
{
  "order_id": "order-456",
  "customer_id": "cust-789",
  "items": [
    {
      "product_id": "prod-123",
      "quantity": 2,
      "unit_price": 79.99
    }
  ],
  "order_total": 159.98,
  "timestamp": 1625097600000
}
```

### Inventory Update Events

Every stock change is written to the `inventory_updates` topic, with the `product_id` as the key:

```json
{
  "product_id": "prod-123",
  "current_stock": 28,
  "last_order_id": "order-456",
  "last_updated": 1625097600000
}
```

After a restock, the update also contains `last_reorder_id`, the `event_id` of the reorder event that caused it.

### Reorder Events (Output)

```json
{
  "event_id": "reorder-3f9a1c2b",
  "product_id": "prod-123",
  "product_name": "Wireless Headphones",
  "current_stock": 8,
  "reorder_quantity": 50,
  "priority": "normal",
  "timestamp": 1625097600000
}
```

### Alert Events (Output)

```json
{
  "alert_id": "alert-7d2e4b6a",
  "alert_type": "critical_inventory",
  "product_id": "prod-123",
  "product_name": "Wireless Headphones",
  "current_stock": 2,
  "threshold": 5,
  "timestamp": 1625097600000,
  "message": "Critical inventory level: Wireless Headphones (2 units remaining)"
}
```

## Creating the KSML Definition

Now, let's create our KSML definition file:

??? info "Inventory event processor (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/event-driven-applications/inventory-event-processors.yaml" %}
    ```

The definition contains five pipelines that work together through topics and one shared state store. Every change in
stock is written to `inventory_updates`, and three pipelines react to it. A reorder also leads to a restock, which is
written back to `inventory_updates`:

<div class="diagram-w75" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Topics and reference data
    Orders[(order_events)]
    Updates[(inventory_updates)]
    Reorders[(reorder_events)]
    Alerts[(inventory_alerts)]
    Catalog[(product_catalog<br/>table)]
    Store[("inventory_store<br/>stock per product")]

    %% Pipelines
    OrderP["<b>order_pipeline</b><br/>one message per ordered item,<br/>subtract quantity from stock"]
    ReorderP["<b>reorder_pipeline</b><br/>join with catalog,<br/>stock ≤ reorder_threshold?"]
    AlertP["<b>alert_pipeline</b><br/>join with catalog,<br/>stock ≤ critical_threshold?"]
    RestockP["<b>restock_pipeline</b><br/>add reorder_quantity<br/>to stock"]
    Monitor["<b>inventory_monitor</b><br/>log every update"]

    %% Data Flow
    Orders --> OrderP
    OrderP --> Updates
    Updates --> ReorderP
    Updates --> AlertP
    Updates --> Monitor
    ReorderP --> Reorders
    AlertP --> Alerts
    Reorders --> RestockP
    RestockP -->|"restocked"| Updates
    Catalog -.-> ReorderP
    Catalog -.-> AlertP
    OrderP <-.->|"get & put"| Store
    Store <-.->|"get & put"| RestockP

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Orders,Updates,Reorders,Alerts,Catalog topic
    class OrderP,ReorderP,AlertP,RestockP,Monitor operation
    class Store external
```

</div>

The `order_pipeline` splits each order into one message per ordered item and uses the `product_id` as the new key, so
the stock of each product is updated separately. The diagram below follows one example order with two items:

<div class="diagram-w50" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Orders[(order_events)]
    Order@{ shape: doc, label: "<b>key: order-456</b><br/>prod-123, quantity 2<br/>prod-456, quantity 4" }

    %% Split
    Split["<code>transformKeyValueToKeyValueList</code><br/>one message per item"]
    Item1@{ shape: doc, label: "<b>key: prod-123</b><br/>quantity 2, order-456" }
    Item2@{ shape: doc, label: "<b>key: prod-456</b><br/>quantity 4, order-456" }

    %% Stock update
    Update["<code>transformValue</code> + <code>mapValues</code><br/>get stock, subtract quantity,<br/>put new stock"]
    Store[("inventory_store")]
    Result1@{ shape: doc, label: "<b>key: prod-123</b><br/>stock 30 → 28" }
    Result2@{ shape: doc, label: "<b>key: prod-456</b><br/>stock 12 → 8" }
    Updates[(inventory_updates)]

    %% Data Flow
    Orders --> Order
    Order --> Split
    Split --> Item1 & Item2
    Item1 --> Update
    Item2 --> Update
    Update <-.-> Store
    Update --> Result1 & Result2
    Result1 --> Updates
    Result2 --> Updates

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Orders,Updates topic
    class Split,Update operation
    class Order,Item1,Item2 message
    class Result1,Result2 result
    class Store external
```

</div>

The diagram below follows the stock of one product, `prod-123` (reorder threshold 10, reorder quantity 50, critical
threshold 5), through a series of updates. The store starts empty, so the first order always triggers a reorder and an
alert:

<div class="diagram-w40" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Stock changes of prod-123
    S1["<b>Order: 3 × prod-123</b><br/>store empty: stock 0<br/>new stock 0, never negative<br/><br/>0 ≤ 10: urgent reorder of 50<br/>0 ≤ 5: critical alert"]
    S2["<b>Restock: + 50</b><br/>new stock 50<br/><br/>above both thresholds:<br/>no reorder, no alert"]
    S3["<b>More orders: 42 units in total</b><br/>new stock 8<br/><br/>8 ≤ 10: normal reorder of 50<br/>8 > 5: no alert"]
    S4["<b>Restock: + 50</b><br/>new stock 58<br/><br/>no reorder, no alert"]

    %% Data Flow
    S1 -->|"reorder_events → restock_pipeline"| S2
    S2 -->|"order_events → order_pipeline"| S3
    S3 -->|"reorder_events → restock_pipeline"| S4

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class S1 dropped
    class S2,S4 message
    class S3 partial
```

</div>

A reorder is `urgent` when the stock is 5 or less, and `normal` otherwise.

## Setting up the producers for test data

To test out the topology above, we create a test data producer definition.

The definition consists of two producers. The first producer is a _single shot producer_ that generates three records
for the `product_catalog` topic. The second producer produces a message every second to the `order_events` topic, using
a randomly generated product order:

??? info "Product and order event producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/event-driven-applications/product-and-order-event-producer.yaml" %}
    ```

## Running the Application

To run the application:

1. Save the processor definition to
   [`inventory-event-processors.yaml`](#creating-the-ksml-definition).
2. Save the producers to
   [`product-and-order-event-producer.yaml`](#setting-up-the-producers-for-test-data).
3. Set up your `ksml-runner.yaml` configuration, pointing to your Kafka installation.

??? info "KSML runner configuration (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/event-driven-applications/ksml-runner.yaml" %}
    ```

4. Start the KSML runner. It runs both definitions together:
      - the `product_catalog_producer` writes the three products to the `product_catalog` topic once
      - the `order_event_producer` writes a random order to the `order_events` topic every second
      - the five pipelines process the orders
5. Monitor the `inventory_updates`, `reorder_events` and `inventory_alerts` topics to see the stock changes, reorders
   and alerts.

## Extending the Event-Driven System

### Integration with External Systems

To make this event-driven system truly useful, you can integrate it with external systems:

1. **Procurement System**: Connect the reorder events to your procurement system to automatically create purchase orders
2. **Notification Service**: Send the alerts to a notification service that can email or text warehouse staff
3. **Analytics Platform**: Stream all events to an analytics platform for business intelligence
4. **Dashboard**: Connect to a real-time dashboard for inventory visualization

### Adding More Event Types

You can extend the system with additional event types:

- **Price Change Events**: Automatically adjust prices based on inventory levels or competitor data
- **Promotion Events**: Trigger promotions for overstocked items
- **Fraud Detection Events**: Flag suspicious order patterns
- **Shipping Delay Events**: Notify customers about potential delays due to inventory issues

## Best Practices for Event-Driven Applications

When building event-driven applications with KSML, consider these best practices:

1. **Event Schema Design**: Design your events to be self-contained and include all necessary context
2. **Idempotent Processing**: Ensure your event handlers can process the same event multiple times without side effects
3. **Event Versioning**: Include version information in your events to handle schema evolution
4. **Monitoring and Observability**: Add logging and metrics to track event flow and processing
5. **Error Handling**: Implement proper error handling and dead-letter queues for failed events

## Conclusion

In this tutorial, you've learned how to:

- Build an event-driven application using KSML
- Detect specific conditions in your data streams
- Generate events in response to those conditions
- Process events to update state and trigger further actions
- Design an end-to-end event-driven architecture

Event-driven applications are a powerful use case for KSML, allowing you to build responsive, real-time systems that
react automatically to changing conditions.

## Next Steps

- Learn about [Real-Time Analytics](real-time-analytics.md) to analyze your event data
- Explore [Data Transformation](data-transformation.md) for more complex event processing
- Check out [External Integration](../tutorials/advanced/external-integration.md) for connecting your events to external
  systems