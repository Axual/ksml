# Integration with External Systems in KSML

This tutorial explores how to integrate KSML applications with external systems such as databases, REST APIs, message
queues, and other services, allowing you to build comprehensive data processing solutions that connect to your entire
technology ecosystem.

## Introduction to External Integration

While Kafka Streams and KSML excel at processing data within Kafka, real-world applications often need to interact with
external systems to:

- Enrich streaming data with reference information from databases
- Persist processed results to data warehouses or databases
- Call external APIs to trigger actions or fetch additional data
- Integrate with legacy systems or third-party services
- Implement the Kappa architecture pattern (using Kafka as the system of record)

This tutorial covers various approaches to integrate KSML applications with external systems while maintaining the
reliability, scalability, and fault-tolerance of your stream processing applications.

## Prerequisites

Before starting this tutorial, you should:

- Understand intermediate KSML concepts (streams, functions, pipelines)
- Have completed the [Custom State Stores](custom-state-stores.md) tutorial
- Be familiar with [Error Handling](../intermediate/error-handling.md) techniques
- Have a basic understanding of external systems you want to integrate with

## Integration Patterns

### 1. Request-Response Pattern

The request-response pattern involves making synchronous calls to external systems from within your KSML functions:

```yaml
functions:
  enrich_with_api_data:
    type: valueTransformer
    globalCode: |
      import requests
      import json

      # Configure API client
      API_BASE_URL = "https://api.example.com/v1"
      API_KEY = "your-api-key"

      def get_api_data(entity_id):
          try:
              response = requests.get(
                  f"{API_BASE_URL}/entities/{entity_id}",
                  headers={"Authorization": f"Bearer {API_KEY}"},
                  timeout=2.0  # Set a reasonable timeout
              )
              response.raise_for_status()  # Raise exception for HTTP errors
              return response.json()
          except Exception as e:
              log.warn("API request failed: {}", str(e))
              return None

    code: |
      # Extract entity ID from the message
      entity_id = value.get("entity_id")
      if not entity_id:
          return value  # No enrichment possible

      # Call the API to get additional data
      api_data = get_api_data(entity_id)

      # Enrich the message with API data
      if api_data:
          value["enriched_data"] = api_data
          value["enrichment_status"] = "success"
      else:
          value["enrichment_status"] = "failed"

      return value
```

#### Best Practices for Request-Response

- **Implement timeouts**: Prevent blocking the processing pipeline
- **Use connection pooling**: Reuse connections for better performance
- **Implement circuit breakers**: Prevent cascading failures when external systems are down
- **Cache responses**: Reduce the load on external systems
- **Handle errors gracefully**: Implement fallback strategies when external systems fail

### 2. Lookup Pattern

The lookup pattern uses state stores to cache reference data from external systems:

```yaml
functions:
  load_reference_data:
    type: valueTransformer
    globalCode: |
      import psycopg2
      import json
      from datetime import datetime

      # Database connection parameters
      DB_PARAMS = {
          "host": "db.example.com",
          "database": "reference_data",
          "user": "app_user",
          "password": "app_password"
      }

      def load_product_data():
          """Load product data from database into a dictionary"""
          products = {}
          try:
              conn = psycopg2.connect(**DB_PARAMS)
              cursor = conn.cursor()
              cursor.execute("SELECT product_id, name, category, price FROM products")
              for row in cursor.fetchall():
                  product_id, name, category, price = row
                  products[product_id] = {
                      "name": name,
                      "category": category,
                      "price": float(price),
                      "loaded_at": datetime.now().isoformat()
                  }
              cursor.close()
              conn.close()
              return products
          except Exception as e:
              log.error("Failed to load product data: {}", str(e))
              return {}

    code: |
      # This function runs once when the topology is built
      # Load reference data into the state store
      products = load_product_data()
      log.info("Loaded {} products into reference data store", len(products))

      for product_id, product_data in products.items():
          reference_data_store.put(product_id, product_data)

      # Return the input unchanged - this function is just for initialization
      return value
    stores:
      - reference_data_store

  enrich_with_product_data:
    type: valueTransformer
    code: |
      # Extract product ID from the message
      product_id = value.get("product_id")
      if not product_id:
          return value  # No enrichment possible

      # Look up product data from the state store
      product_data = reference_data_store.get(product_id)

      # Enrich the message with product data
      if product_data:
          value["product_name"] = product_data.get("name")
          value["product_category"] = product_data.get("category")
          value["product_price"] = product_data.get("price")
          value["enrichment_status"] = "success"
      else:
          value["enrichment_status"] = "not_found"

      return value
    stores:
      - reference_data_store
```

#### Best Practices for Lookup Pattern

- **Periodic refreshes**: Implement a mechanism to periodically refresh the cached data
- **Incremental updates**: Consider using CDC (Change Data Capture) to keep the cache up-to-date
- **Memory management**: Be mindful of the size of the cached data
- **Fallback strategies**: Implement fallback strategies when data is not found in the cache

### 3. Async Integration Pattern

The async integration pattern uses separate Kafka topics for communication with external systems:

```yaml
streams:
  input_events:
    topic: app_events
    keyType: string
    valueType: json

  db_write_requests:
    topic: db_write_requests
    keyType: string
    valueType: json

  db_write_responses:
    topic: db_write_responses
    keyType: string
    valueType: json

functions:
  prepare_db_write:
    type: keyValueTransformer
    code: |
      # Create a write request for the database service
      request_id = str(uuid.uuid4())

      write_request = {
          "request_id": request_id,
          "table": "user_activities",
          "operation": "insert",
          "timestamp": int(time.time() * 1000),
          "data": {
              "user_id": key,
              "activity_type": value.get("type"),
              "activity_data": value
          }
      }

      # Return the request with the request ID as the key
      return (request_id, write_request)

  process_db_response:
    type: valueTransformer
    code: |
      # Process the database write response
      request_id = value.get("request_id")
      status = value.get("status")

      if status == "success":
          log.info("Database write successful for request {}", request_id)
      else:
          log.error("Database write failed for request {}: {}", 
                   request_id, value.get("error"))

      # This could update a state store with the status if needed
      return value

pipelines:
  # Send write requests to the database service
  send_to_database:
    from: input_events
    transformKeyValue: prepare_db_write
    to: db_write_requests

  # Process responses from the database service
  process_responses:
    from: db_write_responses
    mapValues: process_db_response
    to: db_write_status
```

This pattern requires a separate service that:

1. Consumes from the `db_write_requests` topic
2. Performs the database operations
3. Produces results to the `db_write_responses` topic

#### Best Practices for Async Integration

- **Correlation IDs**: Use unique IDs to correlate requests and responses
- **Idempotent operations**: Ensure that operations can be safely retried
- **Dead letter queues**: Implement DLQs for failed operations
- **Monitoring**: Monitor the request and response topics for backpressure

### 4. Database Integration with JDBC

For direct database integration, you can use JDBC within your KSML functions:

```yaml
functions:
  persist_to_database:
    type: forEach
    globalCode: |
      import java.sql.Connection
      import java.sql.DriverManager
      import java.sql.PreparedStatement
      import java.sql.SQLException

      # JDBC connection parameters
      JDBC_URL = "jdbc:postgresql://db.example.com:5432/app_db"
      DB_USER = "app_user"
      DB_PASSWORD = "app_password"

      # Connection pool
      connection_pool = None

      def get_connection():
          try:
              return DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD)
          except SQLException as e:
              log.error("Database connection error: {}", str(e))
              raise e

      def insert_event(conn, user_id, event_type, event_data):
          try:
              sql = """
                  INSERT INTO user_events (user_id, event_type, event_data, created_at)
                  VALUES (?, ?, ?, NOW())
              """
              stmt = conn.prepareStatement(sql)
              stmt.setString(1, user_id)
              stmt.setString(2, event_type)
              stmt.setString(3, json.dumps(event_data))
              return stmt.executeUpdate()
          except SQLException as e:
              log.error("Database insert error: {}", str(e))
              raise e

    code: |
      try:
          # Get database connection
          conn = get_connection()

          # Insert the event into the database
          user_id = key
          event_type = value.get("type", "unknown")

          rows_affected = insert_event(conn, user_id, event_type, value)
          log.info("Inserted event into database: {} rows affected", rows_affected)

          # Close the connection
          conn.close()

      except Exception as e:
          log.error("Failed to persist event to database: {}", str(e))
          # Consider adding to a retry queue or dead letter queue
```

#### Best Practices for JDBC Integration

- **Use connection pooling**: Manage database connections efficiently
- **Batch operations**: Group multiple operations for better performance
- **Prepare statements**: Use prepared statements to prevent SQL injection
- **Transaction management**: Use transactions for atomicity
- **Error handling**: Implement proper error handling and retries

## Practical Example: Multi-System Integration

Let's build a complete example that integrates with multiple external systems:

```yaml
streams:
  order_events:
    topic: ecommerce_orders
    keyType: string  # Order ID
    valueType: json  # Order details

  payment_requests:
    topic: payment_requests
    keyType: string  # Payment request ID
    valueType: json  # Payment details

  payment_responses:
    topic: payment_responses
    keyType: string  # Payment request ID
    valueType: json  # Payment response

  inventory_requests:
    topic: inventory_requests
    keyType: string  # Inventory request ID
    valueType: json  # Inventory details

  inventory_responses:
    topic: inventory_responses
    keyType: string  # Inventory request ID
    valueType: json  # Inventory response

  order_status_updates:
    topic: order_status_updates
    keyType: string  # Order ID
    valueType: json  # Status update

stores:
  # Store for product data
  product_reference_store:
    type: keyValue
    keyType: string  # Product ID
    valueType: json  # Product details
    persistent: true
    caching: true

  # Store for order status
  order_status_store:
    type: keyValue
    keyType: string  # Order ID
    valueType: json  # Order status
    persistent: true
    caching: true

  # Store for payment requests
  payment_request_store:
    type: keyValue
    keyType: string  # Order ID
    valueType: json  # Payment request details
    persistent: true
    caching: true

functions:
  # Load product reference data from database
  load_product_data:
    type: valueTransformer
    globalCode: |
      import psycopg2
      import json

      # Database connection parameters
      DB_PARAMS = {
          "host": "db.example.com",
          "database": "product_catalog",
          "user": "app_user",
          "password": "app_password"
      }

      def load_products():
          products = {}
          try:
              conn = psycopg2.connect(**DB_PARAMS)
              cursor = conn.cursor()
              cursor.execute("""
                  SELECT product_id, name, price, stock_level, category
                  FROM products
                  WHERE active = TRUE
              """)
              for row in cursor.fetchall():
                  product_id, name, price, stock_level, category = row
                  products[product_id] = {
                      "name": name,
                      "price": float(price),
                      "stock_level": stock_level,
                      "category": category,
                      "loaded_at": int(time.time() * 1000)
                  }
              cursor.close()
              conn.close()
              return products
          except Exception as e:
              log.error("Failed to load product data: {}", str(e))
              return {}

    code: |
      # Load product data into the reference store
      products = load_products()
      log.info("Loaded {} products into reference store", len(products))

      for product_id, product_data in products.items():
          product_reference_store.put(product_id, product_data)

      # Return the input unchanged
      return value
    stores:
      - product_reference_store

  # Process new order
  process_order:
    type: keyValueTransformer
    code: |
      order_id = key
      order = value

      # Validate order items against product reference data
      valid_items = []
      invalid_items = []
      total_amount = 0

      for item in order.get("items", []):
          product_id = item.get("product_id")
          quantity = item.get("quantity", 0)

          if not product_id or quantity <= 0:
              invalid_items.append({
                  "product_id": product_id,
                  "reason": "Invalid product ID or quantity"
              })
              continue

          # Look up product details
          product = product_reference_store.get(product_id)
          if not product:
              invalid_items.append({
                  "product_id": product_id,
                  "reason": "Product not found"
              })
              continue

          # Calculate item price
          item_price = product.get("price", 0) * quantity
          total_amount += item_price

          # Add to valid items
          valid_items.append({
              "product_id": product_id,
              "product_name": product.get("name"),
              "quantity": quantity,
              "unit_price": product.get("price", 0),
              "total_price": item_price
          })

      # Create validated order
      validated_order = {
          "order_id": order_id,
          "customer_id": order.get("customer_id"),
          "order_date": order.get("order_date"),
          "items": valid_items,
          "invalid_items": invalid_items,
          "total_amount": total_amount,
          "status": "validated" if valid_items else "invalid",
          "validation_timestamp": int(time.time() * 1000)
      }

      # Store order status
      order_status_store.put(order_id, {
          "status": validated_order["status"],
          "timestamp": validated_order["validation_timestamp"]
      })

      return (order_id, validated_order)
    stores:
      - product_reference_store
      - order_status_store

  # Create payment request
  create_payment_request:
    type: keyValueTransformer
    code: |
      order_id = key
      order = value

      # Only process validated orders with valid items
      if order.get("status") != "validated" or not order.get("items"):
          return None

      # Create payment request
      payment_request_id = str(uuid.uuid4())
      payment_request = {
          "request_id": payment_request_id,
          "order_id": order_id,
          "customer_id": order.get("customer_id"),
          "amount": order.get("total_amount"),
          "currency": "USD",
          "timestamp": int(time.time() * 1000)
      }

      # Store payment request for correlation
      payment_request_store.put(order_id, {
          "payment_request_id": payment_request_id,
          "timestamp": payment_request["timestamp"]
      })

      # Update order status
      order_status = order_status_store.get(order_id)
      order_status["status"] = "payment_pending"
      order_status["payment_request_id"] = payment_request_id
      order_status_store.put(order_id, order_status)

      return (payment_request_id, payment_request)
    stores:
      - payment_request_store
      - order_status_store

  # Process payment response
  process_payment_response:
    type: keyValueTransformer
    code: |
      payment_request_id = key
      payment_response = value

      # Extract data from payment response
      order_id = payment_response.get("order_id")
      status = payment_response.get("status")

      if not order_id:
          log.error("Payment response missing order ID: {}", payment_request_id)
          return None

      # Get current order status
      order_status = order_status_store.get(order_id)
      if not order_status:
          log.error("Order not found for payment response: {}", order_id)
          return None

      # Update order status based on payment result
      if status == "approved":
          order_status["status"] = "paid"
          order_status["payment_timestamp"] = payment_response.get("timestamp")
          order_status["payment_reference"] = payment_response.get("reference")
      else:
          order_status["status"] = "payment_failed"
          order_status["payment_error"] = payment_response.get("error")

      # Store updated status
      order_status_store.put(order_id, order_status)

      # Create order status update message
      status_update = {
          "order_id": order_id,
          "status": order_status["status"],
          "timestamp": int(time.time() * 1000),
          "payment_status": status,
          "payment_reference": payment_response.get("reference"),
          "payment_error": payment_response.get("error")
      }

      return (order_id, status_update)
    stores:
      - order_status_store

  # Create inventory request for paid orders
  create_inventory_request:
    type: keyValueTransformer
    code: |
      order_id = key
      status_update = value

      # Only process paid orders
      if status_update.get("status") != "paid":
          return None

      # Get the original order
      # In a real system, you might need to fetch this from a database or state store
      # For simplicity, we'll assume we have the order details in a state store
      order = order_status_store.get(order_id)
      if not order or "items" not in order:
          log.error("Order details not found for inventory request: {}", order_id)
          return None

      # Create inventory request
      inventory_request_id = str(uuid.uuid4())
      inventory_request = {
          "request_id": inventory_request_id,
          "order_id": order_id,
          "items": order.get("items", []),
          "timestamp": int(time.time() * 1000)
      }

      # Update order status
      order_status = order_status_store.get(order_id)
      order_status["status"] = "inventory_pending"
      order_status["inventory_request_id"] = inventory_request_id
      order_status_store.put(order_id, order_status)

      return (inventory_request_id, inventory_request)
    stores:
      - order_status_store

  # Process inventory response
  process_inventory_response:
    type: keyValueTransformer
    code: |
      inventory_request_id = key
      inventory_response = value

      # Extract data from inventory response
      order_id = inventory_response.get("order_id")
      status = inventory_response.get("status")

      if not order_id:
          log.error("Inventory response missing order ID: {}", inventory_request_id)
          return None

      # Get current order status
      order_status = order_status_store.get(order_id)
      if not order_status:
          log.error("Order not found for inventory response: {}", order_id)
          return None

      # Update order status based on inventory result
      if status == "fulfilled":
          order_status["status"] = "ready_for_shipment"
          order_status["inventory_timestamp"] = inventory_response.get("timestamp")
          order_status["warehouse_id"] = inventory_response.get("warehouse_id")
      else:
          order_status["status"] = "inventory_failed"
          order_status["inventory_error"] = inventory_response.get("error")

      # Store updated status
      order_status_store.put(order_id, order_status)

      # Create order status update message
      status_update = {
          "order_id": order_id,
          "status": order_status["status"],
          "timestamp": int(time.time() * 1000),
          "inventory_status": status,
          "warehouse_id": inventory_response.get("warehouse_id"),
          "inventory_error": inventory_response.get("error")
      }

      return (order_id, status_update)
    stores:
      - order_status_store

  # Send shipment notification via REST API
  send_shipment_notification:
    type: forEach
    globalCode: |
      import requests
      import json

      # API configuration
      NOTIFICATION_API_URL = "https://notifications.example.com/api/v1/shipment"
      API_KEY = "your-api-key"

      def send_notification(order_id, customer_id, status):
          try:
              payload = {
                  "order_id": order_id,
                  "customer_id": customer_id,
                  "status": status,
                  "timestamp": int(time.time() * 1000)
              }

              response = requests.post(
                  NOTIFICATION_API_URL,
                  headers={
                      "Content-Type": "application/json",
                      "Authorization": f"Bearer {API_KEY}"
                  },
                  data=json.dumps(payload),
                  timeout=5.0
              )

              response.raise_for_status()
              return True
          except Exception as e:
              log.error("Failed to send notification for order {}: {}", order_id, str(e))
              return False

    code: |
      order_id = key
      status_update = value

      # Only send notifications for orders ready for shipment
      if status_update.get("status") != "ready_for_shipment":
          return

      # Send notification
      customer_id = status_update.get("customer_id")
      success = send_notification(order_id, customer_id, "ready_for_shipment")

      if success:
          log.info("Sent shipment notification for order {}", order_id)
      else:
          log.warn("Failed to send shipment notification for order {}", order_id)

pipelines:
  # Initialize reference data
  load_reference_data:
    from: reference_data_trigger
    mapValues: load_product_data
    to: reference_data_loaded

  # Process new orders
  process_orders:
    from: order_events
    transformKeyValue: process_order
    to: validated_orders

  # Create payment requests
  request_payments:
    from: validated_orders
    transformKeyValue: create_payment_request
    filter: is_not_null
    to: payment_requests

  # Process payment responses
  handle_payments:
    from: payment_responses
    transformKeyValue: process_payment_response
    filter: is_not_null
    to: order_status_updates

  # Create inventory requests
  request_inventory:
    from: order_status_updates
    transformKeyValue: create_inventory_request
    filter: is_not_null
    to: inventory_requests

  # Process inventory responses
  handle_inventory:
    from: inventory_responses
    transformKeyValue: process_inventory_response
    filter: is_not_null
    to: order_status_updates

  # Send notifications
  send_notifications:
    from: order_status_updates
    forEach: send_shipment_notification
```

This example:

1. Loads product reference data from a database
2. Processes incoming orders and validates them against the reference data
3. Creates payment requests and sends them to a payment service
4. Processes payment responses and updates order status
5. Creates inventory requests for paid orders
6. Processes inventory responses and updates order status
7. Sends shipment notifications via a REST API for orders ready for shipment

## Integration with Specific External Systems

### 1. Relational Databases

For integrating with relational databases:

```yaml
functions:
  database_integration:
    type: valueTransformer
    globalCode: |
      import psycopg2
      from psycopg2.extras import RealDictCursor
      import json
      from contextlib import contextmanager

      # Database connection parameters
      DB_PARAMS = {
          "host": "db.example.com",
          "database": "app_db",
          "user": "app_user",
          "password": "app_password"
      }

      @contextmanager
      def get_db_connection():
          """Context manager for database connections"""
          conn = None
          try:
              conn = psycopg2.connect(**DB_PARAMS)
              yield conn
          except Exception as e:
              log.error("Database connection error: {}", str(e))
              raise
          finally:
              if conn is not None:
                  conn.close()

      def query_user_data(user_id):
          """Query user data from the database"""
          with get_db_connection() as conn:
              with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                  cursor.execute("""
                      SELECT user_id, name, email, account_type, created_at
                      FROM users
                      WHERE user_id = %s
                  """, (user_id,))
                  result = cursor.fetchone()
                  return dict(result) if result else None

      def insert_user_activity(user_id, activity_type, activity_data):
          """Insert user activity into the database"""
          with get_db_connection() as conn:
              with conn.cursor() as cursor:
                  cursor.execute("""
                      INSERT INTO user_activities (user_id, activity_type, activity_data, created_at)
                      VALUES (%s, %s, %s, NOW())
                      RETURNING id
                  """, (user_id, activity_type, json.dumps(activity_data)))
                  activity_id = cursor.fetchone()[0]
                  conn.commit()
                  return activity_id

    code: |
      user_id = key

      # Query user data
      user_data = query_user_data(user_id)

      if not user_data:
          log.warn("User not found: {}", user_id)
          return value

      # Enrich message with user data
      value["user_name"] = user_data.get("name")
      value["user_email"] = user_data.get("email")
      value["user_account_type"] = user_data.get("account_type")

      # Record activity in database
      try:
          activity_id = insert_user_activity(
              user_id, 
              value.get("activity_type", "unknown"),
              value
          )
          value["activity_id"] = activity_id
      except Exception as e:
          log.error("Failed to record activity: {}", str(e))

      return value
```

### 2. NoSQL Databases

For integrating with NoSQL databases like MongoDB:

```yaml
functions:
  mongodb_integration:
    type: valueTransformer
    globalCode: |
      from pymongo import MongoClient
      import json

      # MongoDB connection parameters
      MONGO_URI = "mongodb://user:password@mongodb.example.com:27017/app_db"

      # Create a MongoDB client
      client = MongoClient(MONGO_URI)
      db = client.get_database()

      def get_document(collection, query):
          """Get a document from MongoDB"""
          try:
              result = db[collection].find_one(query)
              return result
          except Exception as e:
              log.error("MongoDB query error: {}", str(e))
              return None

      def insert_document(collection, document):
          """Insert a document into MongoDB"""
          try:
              result = db[collection].insert_one(document)
              return str(result.inserted_id)
          except Exception as e:
              log.error("MongoDB insert error: {}", str(e))
              return None

    code: |
      # Get product data from MongoDB
      product_id = value.get("product_id")
      if product_id:
          product = get_document("products", {"_id": product_id})

          if product:
              # Enrich message with product data
              value["product_name"] = product.get("name")
              value["product_category"] = product.get("category")
              value["product_price"] = product.get("price")

      # Store the event in MongoDB
      event_id = insert_document("events", {
          "user_id": key,
          "event_type": value.get("type"),
          "timestamp": value.get("timestamp"),
          "data": value
      })

      if event_id:
          value["event_id"] = event_id

      return value
```

### 3. REST APIs

For integrating with REST APIs:

```yaml
functions:
  rest_api_integration:
    type: valueTransformer
    globalCode: |
      import requests
      import json
      from cachetools import TTLCache

      # API configuration
      API_BASE_URL = "https://api.example.com/v1"
      API_KEY = "your-api-key"

      # Create a cache with TTL of 5 minutes
      cache = TTLCache(maxsize=1000, ttl=300)

      def get_api_data(endpoint, params=None):
          """Get data from the API with caching"""
          # Create cache key
          cache_key = f"{endpoint}:{json.dumps(params or {})}"

          # Check cache
          if cache_key in cache:
              return cache[cache_key]

          # Make API request
          try:
              response = requests.get(
                  f"{API_BASE_URL}/{endpoint}",
                  params=params,
                  headers={
                      "Authorization": f"Bearer {API_KEY}",
                      "Accept": "application/json"
                  },
                  timeout=5.0
              )

              response.raise_for_status()
              result = response.json()

              # Cache the result
              cache[cache_key] = result

              return result
          except Exception as e:
              log.error("API request failed: {}", str(e))
              return None

      def post_api_data(endpoint, data):
          """Post data to the API"""
          try:
              response = requests.post(
                  f"{API_BASE_URL}/{endpoint}",
                  json=data,
                  headers={
                      "Authorization": f"Bearer {API_KEY}",
                      "Content-Type": "application/json"
                  },
                  timeout=5.0
              )

              response.raise_for_status()
              return response.json()
          except Exception as e:
              log.error("API post failed: {}", str(e))
              return None

    code: |
      # Get weather data for the user's location
      location = value.get("location")
      if location:
          weather_data = get_api_data("weather", {
              "lat": location.get("latitude"),
              "lon": location.get("longitude")
          })

          if weather_data:
              value["weather"] = {
                  "temperature": weather_data.get("temperature"),
                  "conditions": weather_data.get("conditions"),
                  "forecast": weather_data.get("forecast")
              }

      # Send analytics event to API
      analytics_result = post_api_data("analytics/events", {
          "user_id": key,
          "event_type": value.get("type"),
          "timestamp": value.get("timestamp"),
          "properties": value
      })

      if analytics_result:
          value["analytics_id"] = analytics_result.get("id")

      return value
```

### 4. Message Queues

For integrating with message queues like RabbitMQ:

```yaml
functions:
  rabbitmq_integration:
    type: forEach
    globalCode: |
      import pika
      import json

      # RabbitMQ connection parameters
      RABBITMQ_PARAMS = {
          "host": "rabbitmq.example.com",
          "port": 5672,
          "virtual_host": "/",
          "credentials": pika.PlainCredentials("user", "password")
      }

      # Create a connection and channel
      connection = None
      channel = None

      def get_channel():
          global connection, channel

          if connection is None or not connection.is_open:
              connection = pika.BlockingConnection(
                  pika.ConnectionParameters(**RABBITMQ_PARAMS)
              )
              channel = connection.channel()

              # Declare queues
              channel.queue_declare(queue="notifications", durable=True)
              channel.queue_declare(queue="alerts", durable=True)

          return channel

      def send_to_queue(queue, message):
          """Send a message to a RabbitMQ queue"""
          try:
              ch = get_channel()
              ch.basic_publish(
                  exchange="",
                  routing_key=queue,
                  body=json.dumps(message),
                  properties=pika.BasicProperties(
                      delivery_mode=2,  # Make message persistent
                      content_type="application/json"
                  )
              )
              return True
          except Exception as e:
              log.error("Failed to send message to RabbitMQ: {}", str(e))
              return False

    code: |
      # Determine which queue to use based on message type
      message_type = value.get("type", "unknown")

      if message_type in ["alert", "error", "warning"]:
          queue = "alerts"
      else:
          queue = "notifications"

      # Prepare message
      message = {
          "user_id": key,
          "type": message_type,
          "timestamp": value.get("timestamp", int(time.time() * 1000)),
          "content": value.get("content"),
          "priority": value.get("priority", "normal")
      }

      # Send to RabbitMQ
      success = send_to_queue(queue, message)

      if success:
          log.info("Sent message to RabbitMQ queue {}: {}", queue, message_type)
      else:
          log.error("Failed to send message to RabbitMQ queue {}: {}", queue, message_type)
```

## Best Practices for External Integration

### Performance and Reliability

- **Connection pooling**: Reuse connections to external systems
- **Timeouts**: Implement appropriate timeouts for external calls
- **Circuit breakers**: Prevent cascading failures when external systems are down
- **Retries with backoff**: Implement retries with exponential backoff for transient failures
- **Idempotent operations**: Ensure that operations can be safely retried

### Data Consistency

- **Transactions**: Use transactions when appropriate to ensure data consistency
- **Correlation IDs**: Use correlation IDs to track operations across systems
- **Idempotency keys**: Use idempotency keys to prevent duplicate processing
- **Compensating transactions**: Implement compensating transactions for rollbacks

### Security

- **Secure credentials**: Store credentials securely and rotate them regularly
- **TLS/SSL**: Use secure connections for all external communication
- **Authentication**: Implement proper authentication for all external systems
- **Authorization**: Ensure proper authorization for all operations
- **Data encryption**: Encrypt sensitive data in transit and at rest

### Monitoring and Observability

- **Logging**: Log all external interactions with appropriate context
- **Metrics**: Track performance metrics for external calls
- **Tracing**: Implement distributed tracing for end-to-end visibility
- **Alerting**: Set up alerts for abnormal patterns or failures

## Conclusion

Integrating KSML applications with external systems allows you to build comprehensive data processing solutions that
connect to your entire technology ecosystem. By using the patterns and techniques covered in this tutorial, you can
implement reliable, scalable, and maintainable integrations with databases, APIs, message queues, and other external
systems.

In the next tutorial, we'll explore [Advanced Error Handling](advanced-error-handling.md) to learn sophisticated
techniques for handling errors and implementing recovery mechanisms in complex KSML applications.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Intermediate Tutorial: Error Handling](../intermediate/error-handling.md)
- [Reference: Configuration Options](../../reference/configuration-reference.md)