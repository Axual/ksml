# KSML Examples Library

This document provides a collection of ready-to-use KSML examples for common stream processing patterns and use cases. Each example includes a description, the complete KSML code, and explanations of key concepts.

## Basic Examples

### Hello World

A simple pipeline that reads messages from one topic and writes them to another.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: string
    
  output:
    topic: output-topic
    keyType: string
    valueType: string

pipelines:
  hello_world:
    from: input
    via:
      - type: peek
        forEach:
          code: |
            log.info("Processing message: {}", value)
    to: output
```

### Simple Transformation

Transforms JSON messages by extracting and restructuring fields.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: json
    
  output:
    topic: output-topic
    keyType: string
    valueType: json

pipelines:
  transform:
    from: input
    via:
      - type: mapValues
        mapper:
          code: |
            return {
              "id": value.get("user_id"),
              "name": value.get("first_name") + " " + value.get("last_name"),
              "email": value.get("email"),
              "created_at": value.get("signup_date")
            }
    to: output
```

### Filtering

Filters messages based on a condition.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: json
    
  output:
    topic: output-topic
    keyType: string
    valueType: json

pipelines:
  filter:
    from: input
    via:
      - type: filter
        if:
          expression: value.get("age") >= 18
    to: output
```

## Intermediate Examples

### Aggregation

Counts the number of events by category.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: json
    
  output:
    topic: output-topic
    keyType: string
    valueType: json

pipelines:
  count_by_category:
    from: input
    via:
      - type: selectKey
        keySelector:
          expression: value.get("category")
      - type: groupByKey
      - type: count
    to: output
```

### Windowed Aggregation

Calculates the average value over a time window.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: json
    
  output:
    topic: output-topic
    keyType: string
    valueType: json

functions:
  calculate_average:
    type: aggregate
    parameters:
      - name: value
        type: object
      - name: aggregate
        type: object
    code: |
      if aggregate is None:
        return {"sum": value.get("amount", 0), "count": 1}
      else:
        return {
          "sum": aggregate.get("sum", 0) + value.get("amount", 0),
          "count": aggregate.get("count", 0) + 1
        }

pipelines:
  average_by_window:
    from: input
    via:
      - type: selectKey
        keySelector:
          expression: value.get("category")
      - type: groupByKey
      - type: windowedBy
        windowType: tumbling
        timeDifference: 60000  # 1 minute window
      - type: aggregate
        initializer:
          expression: {"sum": 0, "count": 0}
        aggregator:
          code: calculate_average(value, aggregate)
      - type: mapValues
        mapper:
          code: |
            if value.get("count", 0) > 0:
              return {
                "category": key,
                "average": value.get("sum", 0) / value.get("count", 0),
                "count": value.get("count", 0)
              }
            else:
              return {
                "category": key,
                "average": 0,
                "count": 0
              }
    to: output
```

### Join Example

Joins a stream of orders with a table of products.

```yaml
streams:
  orders:
    topic: orders-topic
    keyType: string
    valueType: json
    
  products:
    topic: products-topic
    keyType: string
    valueType: json
    
  enriched_orders:
    topic: enriched-orders-topic
    keyType: string
    valueType: json

functions:
  enrich_order:
    type: mapValues
    parameters:
      - name: order
        type: object
      - name: product
        type: object
    code: |
      if product is None:
        return order
      
      return {
        "order_id": order.get("order_id"),
        "product_id": order.get("product_id"),
        "product_name": product.get("name", "Unknown"),
        "product_category": product.get("category", "Unknown"),
        "quantity": order.get("quantity", 0),
        "unit_price": product.get("price", 0),
        "total_price": order.get("quantity", 0) * product.get("price", 0),
        "customer_id": order.get("customer_id"),
        "order_date": order.get("order_date")
      }

pipelines:
  enrich_orders:
    from: orders
    via:
      - type: selectKey
        keySelector:
          expression: value.get("product_id")
      - type: join
        with: products
      - type: mapValues
        mapper:
          code: enrich_order(value, foreignValue)
    to: enriched_orders
```

## Advanced Examples

### Complex Event Processing

Detects patterns in a stream of events.

```yaml
streams:
  events:
    topic: events-topic
    keyType: string
    valueType: json
    
  alerts:
    topic: alerts-topic
    keyType: string
    valueType: json

functions:
  detect_pattern:
    type: process
    parameters:
      - name: events
        type: array
    code: |
      # Check for a specific pattern: 3 failed login attempts within 1 minute
      if len(events) < 3:
        return None
        
      # Sort events by timestamp
      sorted_events = sorted(events, key=lambda e: e.get("timestamp", 0))
      
      # Check if all events are login failures
      all_failures = all(e.get("event_type") == "LOGIN_FAILURE" for e in sorted_events)
      
      # Check if events occurred within 1 minute
      first_timestamp = sorted_events[0].get("timestamp", 0)
      last_timestamp = sorted_events[-1].get("timestamp", 0)
      within_timeframe = (last_timestamp - first_timestamp) <= 60000  # 1 minute in ms
      
      if all_failures and within_timeframe:
        return {
          "alert_type": "POTENTIAL_BREACH",
          "user_id": sorted_events[0].get("user_id"),
          "attempt_count": len(sorted_events),
          "first_attempt": first_timestamp,
          "last_attempt": last_timestamp,
          "ip_addresses": list(set(e.get("ip_address") for e in sorted_events))
        }
      
      return None

pipelines:
  security_monitoring:
    from: events
    via:
      - type: filter
        if:
          expression: value.get("event_type") == "LOGIN_FAILURE"
      - type: selectKey
        keySelector:
          expression: value.get("user_id")
      - type: groupByKey
      - type: windowedBy
        windowType: session
        timeDifference: 300000  # 5 minute session
      - type: aggregate
        initializer:
          expression: []
        aggregator:
          code: |
            if aggregate is None:
              return [value]
            else:
              return aggregate + [value]
      - type: mapValues
        mapper:
          code: detect_pattern(value)
      - type: filter
        if:
          expression: value is not None
    to: alerts
```

### Multi-Stream Processing

Processes data from multiple input streams and produces multiple output streams.

```yaml
streams:
  clicks:
    topic: user-clicks
    keyType: string
    valueType: json
    
  purchases:
    topic: user-purchases
    keyType: string
    valueType: json
    
  user_profiles:
    topic: user-profiles
    keyType: string
    valueType: json
    
  click_stats:
    topic: click-statistics
    keyType: string
    valueType: json
    
  purchase_stats:
    topic: purchase-statistics
    keyType: string
    valueType: json
    
  user_activity:
    topic: user-activity
    keyType: string
    valueType: json

pipelines:
  # Process clicks
  process_clicks:
    from: clicks
    via:
      - type: selectKey
        keySelector:
          expression: value.get("user_id")
      - type: groupByKey
      - type: windowedBy
        windowType: tumbling
        timeDifference: 3600000  # 1 hour
      - type: count
      - type: mapValues
        mapper:
          code: |
            return {
              "user_id": key,
              "click_count": value,
              "hour": int(window.start() / 3600000),
              "date": window.startTime().toLocalDate().toString()
            }
    to: click_stats
    
  # Process purchases
  process_purchases:
    from: purchases
    via:
      - type: selectKey
        keySelector:
          expression: value.get("user_id")
      - type: groupByKey
      - type: aggregate
        initializer:
          expression: {"count": 0, "total": 0}
        aggregator:
          code: |
            if aggregate is None:
              return {"count": 1, "total": value.get("amount", 0)}
            else:
              return {
                "count": aggregate.get("count", 0) + 1,
                "total": aggregate.get("total", 0) + value.get("amount", 0)
              }
      - type: mapValues
        mapper:
          code: |
            return {
              "user_id": key,
              "purchase_count": value.get("count", 0),
              "total_spent": value.get("total", 0),
              "average_order": value.get("total", 0) / value.get("count", 1)
            }
    to: purchase_stats
    
  # Combine user activity
  combine_activity:
    from: clicks
    via:
      - type: selectKey
        keySelector:
          expression: value.get("user_id")
      - type: leftJoin
        with: purchases
      - type: leftJoin
        with: user_profiles
      - type: mapValues
        mapper:
          code: |
            clicks = value if value else {}
            purchases = foreignValue if foreignValue else {}
            profile = foreignValue2 if foreignValue2 else {}
            
            return {
              "user_id": key,
              "name": profile.get("name", "Unknown"),
              "email": profile.get("email", "Unknown"),
              "last_click": clicks.get("timestamp"),
              "last_purchase": purchases.get("timestamp"),
              "lifetime_value": purchases.get("amount", 0),
              "user_segment": profile.get("segment", "Unknown")
            }
    to: user_activity
```

### Error Handling

Demonstrates error handling patterns in KSML.

```yaml
streams:
  input:
    topic: input-topic
    keyType: string
    valueType: json
    
  output:
    topic: output-topic
    keyType: string
    valueType: json
    
  error_stream:
    topic: error-topic
    keyType: string
    valueType: json

functions:
  process_data:
    type: mapValues
    parameters:
      - name: data
        type: object
    code: |
      # This function might throw exceptions
      if "required_field" not in data:
        raise ValueError("Missing required field")
        
      result = {
        "id": data.get("id"),
        "processed_value": data.get("value") * 2,
        "timestamp": int(time.time() * 1000)
      }
      
      return result

pipelines:
  robust_processing:
    from: input
    via:
      - type: try
        operations:
          - type: mapValues
            mapper:
              code: process_data(value)
        catch:
          - type: peek
            forEach:
              code: |
                log.error("Error processing record: {}", exception)
          - type: mapValues
            mapper:
              code: |
                return {
                  "original_data": value,
                  "error": str(exception),
                  "timestamp": int(time.time() * 1000)
                }
          - type: to
            stream: error_stream
    to: output
```

## Domain-Specific Examples

### E-commerce

Processes order data for an e-commerce application.

```yaml
streams:
  orders:
    topic: orders
    keyType: string
    valueType: json
    
  inventory:
    topic: inventory
    keyType: string
    valueType: json
    
  shipments:
    topic: shipments
    keyType: string
    valueType: json
    
  order_updates:
    topic: order-updates
    keyType: string
    valueType: json

pipelines:
  process_orders:
    from: orders
    via:
      - type: peek
        forEach:
          code: |
            log.info("Processing order: {}", value.get("order_id"))
      - type: flatMap
        mapper:
          code: |
            # Create a record for each item in the order
            result = []
            for item in value.get("items", []):
              result.append((
                item.get("product_id"),
                {
                  "order_id": value.get("order_id"),
                  "product_id": item.get("product_id"),
                  "quantity": item.get("quantity"),
                  "customer_id": value.get("customer_id"),
                  "order_date": value.get("order_date")
                }
              ))
            return result
      - type: join
        with: inventory
      - type: mapValues
        mapper:
          code: |
            order_item = value
            inventory_item = foreignValue
            
            # Check if item is in stock
            in_stock = inventory_item.get("quantity", 0) >= order_item.get("quantity", 0)
            
            return {
              "order_id": order_item.get("order_id"),
              "product_id": order_item.get("product_id"),
              "product_name": inventory_item.get("name", "Unknown"),
              "quantity": order_item.get("quantity"),
              "in_stock": in_stock,
              "estimated_ship_date": inventory_item.get("next_restock_date") if not in_stock else order_item.get("order_date")
            }
    to: order_updates
```

### IoT Sensor Processing

Processes sensor data from IoT devices.

```yaml
streams:
  sensor_data:
    topic: sensor-readings
    keyType: string
    valueType: json
    
  device_metadata:
    topic: device-metadata
    keyType: string
    valueType: json
    
  alerts:
    topic: sensor-alerts
    keyType: string
    valueType: json
    
  aggregated_readings:
    topic: aggregated-readings
    keyType: string
    valueType: json

pipelines:
  # Process raw sensor data
  process_readings:
    from: sensor_data
    via:
      - type: join
        with: device_metadata
      - type: mapValues
        mapper:
          code: |
            reading = value
            metadata = foreignValue
            
            # Enrich with device metadata
            return {
              "device_id": reading.get("device_id"),
              "sensor_type": metadata.get("sensor_type"),
              "location": metadata.get("location"),
              "reading": reading.get("value"),
              "unit": metadata.get("unit"),
              "timestamp": reading.get("timestamp"),
              "battery_level": reading.get("battery_level")
            }
      - type: branch
        predicates:
          - code: |
              # Check for anomalous readings
              min_threshold = foreignValue.get("min_threshold")
              max_threshold = foreignValue.get("max_threshold")
              reading_value = value.get("reading")
              
              return (reading_value < min_threshold or reading_value > max_threshold)
          - expression: true  # All readings
    to:
      - alerts
      - aggregated_readings
```

## Best Practices

When using these examples, consider the following best practices:

1. **Adapt to your specific use case**: These examples provide a starting point. Modify them to fit your specific requirements.
2. **Test thoroughly**: Always test your KSML applications with representative data before deploying to production.
3. **Monitor performance**: Keep an eye on throughput, latency, and resource usage, especially for stateful operations.
4. **Handle errors gracefully**: Implement proper error handling to prevent pipeline failures.
5. **Document your code**: Add comments to explain complex logic and business rules.

## Contributing Examples

We welcome contributions to this examples library! If you have a useful KSML pattern or solution to share:

1. Document your example with clear explanations
2. Include the complete KSML code
3. Explain the key concepts and patterns used
4. Submit a pull request to the KSML repository

Your examples will help the community learn and apply KSML more effectively.