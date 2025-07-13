# Implementing Joins in KSML

This tutorial explores how to implement join operations in KSML, allowing you to combine data from multiple streams or tables to create enriched datasets.

## Introduction to Joins

Joins are powerful operations that allow you to combine data from different sources based on a common key. In stream processing, joins enable you to:

- Enrich streaming data with reference information
- Correlate events from different systems
- Build comprehensive views of entities from fragmented data
- Implement complex business logic that depends on multiple data sources

KSML supports various types of joins, each with different semantics and use cases.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Stateful Operations](../../core-concepts/operations.md#stateful-operations)
- Understand the difference between [streams and tables](../../reference/stream-types-reference.md)

## Types of Joins in KSML

KSML supports several types of joins, each with different semantics:

### Stream-Stream Joins

Join two streams based on a common key within a specified time window:

- **Inner Join (`join`)**: Outputs a result only when both streams have matching keys within the time window
- **Left Join (`leftJoin`)**: Always outputs a result for messages from the left stream, joining with the right stream if available
- **Outer Join (`outerJoin`)**: Outputs a result whenever either stream has a message, joining them when both are available

### Stream-Table Joins

Join a stream with a table (materialized view) based on the key:

- **Inner Join (`join`)**: Outputs a result only when the stream key exists in the table
- **Left Join (`leftJoin`)**: Always outputs a result for stream messages, joining with the table value if available

### Stream-GlobalTable Joins

Join a stream with a global table, with the ability to use a foreign key:

- **Inner Join (`join`)**: Outputs a result only when the stream's foreign key exists in the global table
- **Left Join (`leftJoin`)**: Always outputs a result for stream messages, joining with the global table value if available

## Basic Join Example

Let's start with a simple example that joins a stream of orders with a table of customer information:

```yaml
streams:
  orders:
    topic: new_orders
    keyType: string  # Customer ID
    valueType: json  # Order details

  enriched_orders:
    topic: orders_with_customer_data
    keyType: string  # Customer ID
    valueType: json  # Combined order and customer data

tables:
  customers:
    topic: customer_data
    keyType: string  # Customer ID
    valueType: json  # Customer details

functions:
  join_order_with_customer:
    type: valueJoiner
    code: |
      # Combine order and customer information
      result = {}

      # Add order details
      if value1 is not None:
        result.update(value1)

      # Add customer details
      if value2 is not None:
        result["customer"] = value2

      return result

pipelines:
  enrich_orders:
    from: orders
    join:
      stream: customers
      valueJoiner: join_order_with_customer
    to: enriched_orders
```

This pipeline:
1. Takes a stream of orders with customer IDs as keys
2. Joins it with a table of customer data using the customer ID
3. Combines the order and customer information using a valueJoiner function
4. Outputs the enriched orders to a new topic

## Working with Time Windows in Joins

Stream-stream joins require a time window to define how long to wait for matching records:

```yaml
pipelines:
  match_clicks_with_purchases:
    from: product_clicks
    join:
      stream: product_purchases
      valueJoiner: correlate_click_and_purchase
      window:
        type: time
        size: 30m  # Look for purchases within 30 minutes of a click
    to: correlated_user_actions
```

## Foreign Key Joins

When joining with a GlobalKTable, you can use a foreign key extractor to join on fields other than the primary key:

```yaml
streams:
  orders:
    topic: new_orders
    keyType: string  # Order ID
    valueType: json  # Order details including product_id

globalTables:
  products:
    topic: product_catalog
    keyType: string  # Product ID
    valueType: json  # Product details

functions:
  extract_product_id:
    type: foreignKeyExtractor
    expression: value.get("product_id")

  join_order_with_product:
    type: valueJoiner
    code: |
      # Combine order and product information
      result = {}

      # Add order details
      if value1 is not None:
        result.update(value1)

      # Add product details
      if value2 is not None:
        result["product"] = value2

      return result

pipelines:
  enrich_orders_with_products:
    from: orders
    join:
      globalTable: products
      foreignKeyExtractor: extract_product_id
      valueJoiner: join_order_with_product
    to: orders_with_product_details
```

## Practical Example: Order Processing System

Let's build a more complex example that implements an order processing system with multiple joins:

```yaml
streams:
  orders:
    topic: incoming_orders
    keyType: string  # Order ID
    valueType: json  # Order details including customer_id and items array

  processed_orders:
    topic: orders_ready_for_fulfillment
    keyType: string  # Order ID
    valueType: json  # Fully processed order

globalTables:
  customers:
    topic: customer_data
    keyType: string  # Customer ID
    valueType: json  # Customer details

  inventory:
    topic: inventory_levels
    keyType: string  # Product ID
    valueType: json  # Inventory information

  shipping_rates:
    topic: shipping_rate_data
    keyType: string  # Region code
    valueType: json  # Shipping rates

functions:
  extract_customer_id:
    type: foreignKeyExtractor
    expression: value.get("customer_id")

  join_order_with_customer:
    type: valueJoiner
    code: |
      result = value1.copy() if value1 else {}
      if value2:
        result["customer_details"] = value2
      return result

  extract_region_code:
    type: foreignKeyExtractor
    code: |
      if value and "customer_details" in value and "region" in value["customer_details"]:
        return value["customer_details"]["region"]
      return "UNKNOWN"

  join_with_shipping_rates:
    type: valueJoiner
    code: |
      result = value1.copy() if value1 else {}
      if value2:
        result["shipping_rates"] = value2
      return result

  check_inventory_and_calculate_total:
    type: valueTransformer
    code: |
      if value is None:
        return None

      # Initialize fields
      value["items_with_inventory"] = []
      value["out_of_stock_items"] = []
      value["subtotal"] = 0
      value["shipping_cost"] = 0
      value["total"] = 0

      # Process each item
      for item in value.get("items", []):
        product_id = item.get("product_id")
        quantity = item.get("quantity", 0)
        price = item.get("price", 0)

        # Check inventory (would be another join in a real system)
        # For simplicity, we're just checking if the product_id is even or odd
        in_stock = int(product_id) % 2 == 0

        if in_stock:
          value["items_with_inventory"].append(item)
          value["subtotal"] += price * quantity
        else:
          value["out_of_stock_items"].append(item)

      # Calculate shipping
      if "shipping_rates" in value:
        base_rate = value["shipping_rates"].get("base_rate", 5.0)
        value["shipping_cost"] = base_rate

      # Calculate total
      value["total"] = value["subtotal"] + value["shipping_cost"]

      return value

pipelines:
  # First pipeline: Join orders with customer data
  join_orders_with_customers:
    from: orders
    join:
      globalTable: customers
      foreignKeyExtractor: extract_customer_id
      valueJoiner: join_order_with_customer
    to: orders_with_customers

  # Second pipeline: Join with shipping rates
  join_with_shipping:
    from: orders_with_customers
    join:
      globalTable: shipping_rates
      foreignKeyExtractor: extract_region_code
      valueJoiner: join_with_shipping_rates
    to: orders_with_shipping

  # Final pipeline: Process inventory and calculate totals
  finalize_orders:
    from: orders_with_shipping
    mapValues: check_inventory_and_calculate_total
    to: processed_orders
```

This pipeline:
1. Takes incoming orders
2. Enriches them with customer data using a foreign key join
3. Adds shipping rates based on the customer's region
4. Checks inventory, calculates totals, and marks items as in-stock or out-of-stock
5. Outputs the fully processed order

## Best Practices for Joins

### Performance Considerations

- **State Size**: Joins maintain state, which consumes memory. Monitor state store sizes.
- **Window Size**: For windowed joins, smaller windows use less state but may miss matches.
- **Join Order**: Join with smaller datasets first when possible to reduce intermediate result sizes.

### Design Patterns

- **Pre-filtering**: Filter unnecessary data before joins to reduce state size.
- **Denormalization**: Consider denormalizing data at write time for frequently joined data.
- **Caching**: Use GlobalKTables for reference data that needs to be joined with many streams.

### Error Handling

Handle missing or invalid data in your joiner functions:

```yaml
functions:
  robust_joiner:
    type: valueJoiner
    code: |
      try:
        # Validate inputs
        if value1 is None:
          log.warn("Left side of join is null for key: {}", key)
          return None

        if value2 is None:
          log.warn("Right side of join is null for key: {}", key)
          # Still proceed with just the left data
          return value1

        # Perform the join
        result = value1.copy()
        result["joined_data"] = value2
        return result

      except Exception as e:
        log.error("Error in join operation: {}", str(e))
        # Return left side data to avoid losing the record
        return value1
```

## Conclusion

Joins are essential operations for building sophisticated stream processing applications. KSML makes it easy to implement various types of joins while leveraging Python for the joining logic.

By understanding the different join types and their appropriate use cases, you can build powerful data pipelines that combine and enrich data from multiple sources.

In the next tutorial, we'll explore [Using Windowed Operations](windowed-operations.md) to process data within time boundaries.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Streams and Data Types](../../reference/stream-types-reference.md)
- [Reference: Join Operations](../../reference/operations-reference.md)
