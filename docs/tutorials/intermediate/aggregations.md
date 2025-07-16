# Working with Aggregations in KSML

This tutorial explores how to implement aggregation operations in KSML, allowing you to combine multiple messages into meaningful summaries and statistics.

## Introduction to Aggregations

Aggregations are stateful operations that combine multiple messages to produce a single result. They're essential for:

- Computing statistics (sums, averages, counts)
- Building summaries of streaming data
- Reducing data volume by consolidating related messages
- Creating time-based analytics

In KSML, aggregations are implemented using the Kafka Streams stateful operations, but with the added flexibility of Python functions.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Stateful Operations](../../core-concepts/operations.md#stateful-operations)

## Types of Aggregations in KSML

KSML supports several types of aggregation operations:

### Count

The simplest aggregation is `count`, which counts the number of messages with the same key:

```yaml
pipelines:
  count_by_user:
    from: user_actions
    via:
      - type: groupByKey
      - type: count
      - type: toStream
    to: user_action_counts
```

### Reduce

The `reduce` operation combines messages with the same key using a reducer function:

```yaml
functions:
  sum_amounts:
    type: reducer
    expression: value1 + value2

pipelines:
  sum_transactions:
    from: financial_transactions
    via:
      - type: groupByKey
      - type: reduce
        reducer: sum_amounts
    to: transaction_sums
```

### Aggregate

The most flexible aggregation is `aggregate`, which uses an initializer function to create the initial aggregation value and an aggregator function to update it:

```yaml
functions:
  init_stats:
    type: initializer
    expression: {"count": 0, "sum": 0, "min": None, "max": None}
    
  update_stats:
    type: aggregator
    code: |
      if aggregatedValue is None:
        aggregatedValue = {"count": 0, "sum": 0, "min": None, "max": None}
      
      amount = value.get("amount", 0)
      
      # Update count and sum
      aggregatedValue["count"] += 1
      aggregatedValue["sum"] += amount
      
      # Update min and max
      if aggregatedValue["min"] is None or amount < aggregatedValue["min"]:
        aggregatedValue["min"] = amount
      if aggregatedValue["max"] is None or amount > aggregatedValue["max"]:
        aggregatedValue["max"] = amount
        
      return aggregatedValue

pipelines:
  calculate_statistics:
    from: payment_stream
    via:
      - type: groupByKey
      - type: aggregate
        initializer: init_stats
        aggregator: update_stats
    to: payment_statistics
```

## Working with Windows

Aggregations become even more powerful when combined with windowing operations, which group messages into time-based windows:

```yaml
pipelines:
  hourly_statistics:
    from: sensor_readings
    via:
      - type: groupByKey
      - type: windowByTime
        windowType: tumbling
        duration: 1h
        advanceBy: 1h
        grace: 10m
      - type: aggregate
        initializer: init_stats
        aggregator: update_stats
    to: hourly_sensor_statistics
```

This creates hourly statistics for each sensor (assuming the sensor ID is the key).

## Practical Example: Sales Analytics

Let's build a complete example that calculates sales analytics by region and product:

```yaml
streams:
  sales_events:
    topic: retail_sales
    keyType: string  # Product ID
    valueType: json  # Sale details including region, amount, quantity

  sales_by_region:
    topic: sales_by_region
    keyType: string  # Region
    valueType: json  # Aggregated sales statistics

functions:
  extract_region:
    type: keyTransformer
    code: |
      # Extract region from the sale event and use it as the new key
      return value.get("region", "unknown")
  
  initialize_sales_stats:
    type: initializer
    expression: {"total_sales": 0, "total_quantity": 0, "transaction_count": 0, "products": {}}
  
  aggregate_sales:
    type: aggregator
    code: |
      # Initialize if needed
      if aggregatedValue is None:
        aggregatedValue = {"total_sales": 0, "total_quantity": 0, "transaction_count": 0, "products": {}}
      
      # Extract data from the sale
      product_id = key
      amount = value.get("amount", 0)
      quantity = value.get("quantity", 0)
      
      # Update aggregated values
      aggregatedValue["total_sales"] += amount
      aggregatedValue["total_quantity"] += quantity
      aggregatedValue["transaction_count"] += 1
      
      # Track per-product statistics
      if product_id not in aggregatedValue["products"]:
        aggregatedValue["products"][product_id] = {"sales": 0, "quantity": 0}
      
      aggregatedValue["products"][product_id]["sales"] += amount
      aggregatedValue["products"][product_id]["quantity"] += quantity
      
      return aggregatedValue

pipelines:
  regional_sales_analytics:
    from: sales_events
    via:
      # Group by region instead of product ID
      - type: groupBy
      # Use tumbling window of 1 day
      - type: windowByTime
        windowType: tumbling
        duration: 1d
        advanceBy: 1d
      # Aggregate sales data
      - type: aggregate
        initializer: initialize_sales_stats
        aggregator: aggregate_sales
    # Output to region-specific topic
    to: sales_by_region
```

This pipeline:
1. Takes sales events with product IDs as keys
2. Regroups them by region
3. Creates daily windows
4. Aggregates sales statistics including per-product breakdowns
5. Outputs the results to a new topic

## Best Practices for Aggregations

### Performance Considerations

- **Memory Usage**: Aggregations store state, which consumes memory. Be mindful of the volume of unique keys.
- **Window Size**: Larger windows require more state storage. Choose appropriate window sizes.
- **Serialization**: Complex aggregated objects can be expensive to serialize/deserialize.

### Design Patterns

- **Two-Phase Aggregation**: For high-cardinality data, consider aggregating in two phases (local then global).
- **Pre-Filtering**: Filter unnecessary data before aggregation to reduce state size.
- **Downsampling**: For time series data, consider downsampling before aggregation.

### Error Handling

Always handle potential errors in your aggregator functions:

```yaml
functions:
  safe_aggregator:
    type: aggregator
    code: |
      try:
        # Your aggregation logic here
        return result
      except Exception as e:
        log.error("Error in aggregation: {}", str(e))
        # Return previous state to avoid losing data
        return aggregatedValue
```

## Conclusion

Aggregations are powerful tools for deriving insights from streaming data. By combining KSML's aggregation operations with Python functions, you can implement sophisticated analytics that would be complex to build with traditional Kafka Streams applications.

In the next tutorial, we'll explore how to [implement joins](joins.md) to combine data from multiple streams.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Reference: Aggregation Operations](../../reference/operations-reference.md)