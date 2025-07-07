# Event-Driven Applications with KSML

This tutorial demonstrates how to build event-driven applications using KSML. You'll learn how to detect specific events in your data streams and trigger appropriate actions in response.

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
- Have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Be familiar with [Filtering and Transforming](../beginner/filtering-transforming.md)
- Have a basic understanding of [Complex Event Processing](../advanced/complex-event-processing.md)

## The Use Case

In this tutorial, we'll build an event-driven inventory management system for an e-commerce platform. The system will:

1. Monitor product inventory levels in real-time
2. Detect when items are running low
3. Generate reorder events for the procurement system
4. Alert warehouse staff about critical inventory situations
5. Update inventory dashboards in real-time

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

```yaml
streams:
  inventory_updates:
    topic: inventory_updates
    keyType: string  # product_id
    valueType: json  # inventory data
    
  order_events:
    topic: order_events
    keyType: string  # order_id
    valueType: json  # order data
    
  product_reference:
    topic: product_reference
    keyType: string  # product_id
    valueType: json  # product details including thresholds
    
  reorder_events:
    topic: reorder_events
    keyType: string  # event_id
    valueType: json  # reorder data
    
  inventory_alerts:
    topic: inventory_alerts
    keyType: string  # alert_id
    valueType: json  # alert data

functions:
  generate_reorder_event:
    type: mapValues
    parameters:
      - name: inventory
        type: object
      - name: product_info
        type: object
    code: |
      import uuid
      import time
      
      # Get reorder threshold and quantity from product reference data
      reorder_threshold = product_info.get("reorder_threshold", 10)
      reorder_quantity = product_info.get("reorder_quantity", 50)
      
      # Determine priority based on current stock
      current_stock = inventory.get("current_stock", 0)
      priority = "urgent" if current_stock <= 5 else "normal"
      
      # Generate event ID
      event_id = f"reorder-{uuid.uuid4().hex[:8]}"
      
      # Create reorder event
      return {
        "event_id": event_id,
        "product_id": inventory.get("product_id"),
        "product_name": inventory.get("product_name"),
        "current_stock": current_stock,
        "reorder_quantity": reorder_quantity,
        "priority": priority,
        "warehouse_id": inventory.get("warehouse_id"),
        "timestamp": int(time.time() * 1000)
      }
      
  generate_alert:
    type: mapValues
    parameters:
      - name: inventory
        type: object
      - name: product_info
        type: object
    code: |
      import uuid
      import time
      
      # Get critical threshold from product reference data
      critical_threshold = product_info.get("critical_threshold", 5)
      
      # Get current stock
      current_stock = inventory.get("current_stock", 0)
      product_name = inventory.get("product_name", "Unknown Product")
      
      # Generate alert ID
      alert_id = f"alert-{uuid.uuid4().hex[:8]}"
      
      # Create alert message
      message = f"Critical inventory level: {product_name} ({current_stock} units remaining)"
      
      # Create alert event
      return {
        "alert_id": alert_id,
        "alert_type": "critical_inventory",
        "product_id": inventory.get("product_id"),
        "product_name": product_name,
        "current_stock": current_stock,
        "threshold": critical_threshold,
        "warehouse_id": inventory.get("warehouse_id"),
        "timestamp": int(time.time() * 1000),
        "message": message
      }
      
  update_inventory_from_order:
    type: mapValues
    parameters:
      - name: order
        type: object
      - name: inventory
        type: object
    code: |
      if inventory is None or order is None:
        return None
        
      # Find the item in the order that matches this product
      product_id = inventory.get("product_id")
      order_items = order.get("items", [])
      
      ordered_quantity = 0
      for item in order_items:
        if item.get("product_id") == product_id:
          ordered_quantity += item.get("quantity", 0)
      
      # If this product wasn't in the order, return unchanged inventory
      if ordered_quantity == 0:
        return None
        
      # Update inventory with new stock level
      current_stock = inventory.get("current_stock", 0)
      new_stock = max(0, current_stock - ordered_quantity)
      
      return {
        **inventory,
        "current_stock": new_stock,
        "last_order_id": order.get("order_id"),
        "last_updated": int(time.time() * 1000)
      }

pipelines:
  # Pipeline for detecting low inventory and generating reorder events
  reorder_pipeline:
    from: inventory_updates
    via:
      - type: join
        with: product_reference
      - type: filter
        if:
          code: |
            reorder_threshold = foreignValue.get("reorder_threshold", 10)
            return value.get("current_stock", 0) <= reorder_threshold
      - type: mapValues
        mapper:
          code: generate_reorder_event(value, foreignValue)
    to: reorder_events
    
  # Pipeline for detecting critical inventory levels and generating alerts
  alert_pipeline:
    from: inventory_updates
    via:
      - type: join
        with: product_reference
      - type: filter
        if:
          code: |
            critical_threshold = foreignValue.get("critical_threshold", 5)
            return value.get("current_stock", 0) <= critical_threshold
      - type: mapValues
        mapper:
          code: generate_alert(value, foreignValue)
    to: inventory_alerts
    
  # Pipeline for updating inventory based on orders
  order_processing_pipeline:
    from: order_events
    via:
      - type: flatMap
        mapper:
          code: |
            # For each item in the order, emit a key-value pair with product_id as key
            result = []
            for item in value.get("items", []):
              product_id = item.get("product_id")
              if product_id:
                result.append((product_id, value))
            return result
      - type: join
        with: inventory_updates
      - type: mapValues
        mapper:
          code: update_inventory_from_order(value, foreignValue)
      - type: filter
        if:
          expression: value is not None
    to: inventory_updates
```

## Running the Application

To run the application:

1. Save the KSML definition to `definitions/event_driven.yaml`
2. Create a configuration file at `config/application.properties` with your Kafka connection details
3. Start the Docker Compose environment: `docker-compose up -d`
4. Produce sample data to the input topics
5. Monitor the output topics to see the events being generated

## Testing the Event-Driven System

You can test the system by producing sample data to the input topics:

```bash
# Produce product reference data
echo 'prod-123:{"product_id":"prod-123","reorder_threshold":10,"reorder_quantity":50,"critical_threshold":5}' | \
  kafka-console-producer --broker-list localhost:9092 --topic product_reference --property "parse.key=true" --property "key.separator=:"

# Produce inventory update
echo 'prod-123:{"product_id":"prod-123","product_name":"Wireless Headphones","category":"electronics","current_stock":8,"warehouse_id":"wh-east-1","timestamp":1625097600000,"unit_price":79.99}' | \
  kafka-console-producer --broker-list localhost:9092 --topic inventory_updates --property "parse.key=true" --property "key.separator=:"

# Produce order event
echo 'order-456:{"order_id":"order-456","customer_id":"cust-789","items":[{"product_id":"prod-123","quantity":3,"unit_price":79.99}],"order_total":239.97,"timestamp":1625097600000}' | \
  kafka-console-producer --broker-list localhost:9092 --topic order_events --property "parse.key=true" --property "key.separator=:"
```

Then, consume from the output topics to see the events being generated:

```bash
# Monitor reorder events
kafka-console-consumer --bootstrap-server localhost:9092 --topic reorder_events --from-beginning

# Monitor alerts
kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory_alerts --from-beginning

# Monitor updated inventory
kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory_updates --from-beginning
```

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

Event-driven applications are a powerful use case for KSML, allowing you to build responsive, real-time systems that react automatically to changing conditions.

## Next Steps

- Learn about [Real-Time Analytics](real-time-analytics.md) to analyze your event data
- Explore [Data Transformation](data-transformation.md) for more complex event processing
- Check out [External Integration](../advanced/external-integration.md) for connecting your events to external systems