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

## The Use Case

In this tutorial, we'll build an event-driven inventory management system for an e-commerce platform. The system will:

1. Monitor product inventory levels in real-time
2. Detect when items are running low
3. Generate reorder events for the procurement system
4. Alert warehouse staff about critical inventory situations
5. Update inventory dashboards in real-time

## Defining the Data Models

### Inventory Update Events

```json
{
  "product_id": "prod-123",
  "product_name": "Wireless Headphones",
  "category": "electronics",
  "current_stock": 15,
  "warehouse_id": "wh-east-1",
  "timestamp": 1625097600000,
  "unit_price": 79.99
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

### Reorder Events (Output)

```json
{
  "event_id": "reorder-789",
  "product_id": "prod-123",
  "product_name": "Wireless Headphones",
  "current_stock": 5,
  "reorder_quantity": 50,
  "priority": "normal",
  "warehouse_id": "wh-east-1",
  "timestamp": 1625097600000
}
```

### Alert Events (Output)

```json
{
  "alert_id": "alert-123",
  "alert_type": "critical_inventory",
  "product_id": "prod-123",
  "product_name": "Wireless Headphones",
  "current_stock": 2,
  "threshold": 5,
  "warehouse_id": "wh-east-1",
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
   [`product-and-order-event-producer.yaml`](#setting-up-the-producers-for-test-data)
3. Set up your `ksml-runner.yaml` configuration, pointing to your Kafka installation.

??? info "KSML runner configuration (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/data-transformation/ksml-runner.yaml" %}
    ```

4. Start the `customer_segment_producer` to produce the sample segment information to Kafka.
5. Start the `legacy_customer_data_producer` to produce some sample data to the input topic.
6. Start the `data_transformation` topology to initiate the continuous data transformation logic.
7. Monitor the output topic to see the transformed data.

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