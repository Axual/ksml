# Error Handling and Recovery in KSML

This tutorial explores strategies for handling errors and implementing recovery mechanisms in KSML applications, helping you build more robust and resilient stream processing pipelines.

## Introduction to Error Handling

Error handling is a critical aspect of any production-grade stream processing application. In streaming contexts, errors can occur for various reasons:

- Invalid or malformed input data
- External service failures
- Resource constraints
- Business rule violations
- Unexpected edge cases

Without proper error handling, these issues can cause your application to:

- Crash and stop processing
- Skip or lose important messages
- Produce incorrect results
- Create inconsistent state

KSML provides several mechanisms to handle errors gracefully and implement recovery strategies, allowing your applications to continue processing even when problems occur.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with Python exception handling

## Error Handling Strategies in KSML

### 1. Try-Except Blocks in Python Functions

The most basic form of error handling in KSML is using try-except blocks in your Python functions:

```yaml
functions:
  safe_transform:
    type: valueTransformer
    code: |
      try:
        # Potentially risky operation
        result = process_data(value)
        return result
      except Exception as e:
        # Log the error
        log.error("Error processing data: {}", str(e))
        # Return a default or fallback value
        return {"error": str(e), "original_data": value}
```

This approach:
- Prevents the function from failing completely
- Logs the error for troubleshooting
- Returns a fallback value that allows processing to continue

### 2. Validation and Filtering

Proactively validate data and filter out problematic messages before they cause errors:

```yaml
functions:
  validate_data:
    type: predicate
    code: |
      # Check if the message has all required fields
      if value is None:
        log.warn("Received null value for key: {}", key)
        return False
        
      required_fields = ["user_id", "timestamp", "action"]
      for field in required_fields:
        if field not in value:
          log.warn("Missing required field '{}' in message with key: {}", field, key)
          return False
          
      return True

pipelines:
  process_valid_data:
    from: input_stream
    filter: validate_data  # Only process messages that pass validation
    # Continue processing with valid data...
    to: processed_stream
```

### 3. Dead Letter Queues

Implement a "dead letter queue" pattern to capture and store messages that couldn't be processed:

```yaml
functions:
  process_with_dlq:
    type: keyValueTransformer
    code: |
      try:
        # Attempt to process the message
        processed_value = process_data(value)
        # Return the processed message to the main output
        return (key, processed_value)
      except Exception as e:
        # Create an error record
        error_record = {
          "original_key": key,
          "original_value": value,
          "error": str(e),
          "timestamp": int(time.time() * 1000)
        }
        # Send to the dead letter queue
        dlq_topic.send(key, error_record)
        # Return None to filter this message from the main output
        return None

pipelines:
  main_processing:
    from: input_stream
    transformKeyValue: process_with_dlq
    to: processed_stream
```

### 4. Error Classification and Routing

Classify errors and route messages to different handling paths based on the error type:

```yaml
functions:
  classify_errors:
    type: keyValueTransformer
    code: |
      try:
        # Attempt normal processing
        result = process_data(value)
        # Tag with success status
        result["status"] = "success"
        return (key, result)
      except ValidationError as e:
        # Handle validation errors
        log.warn("Validation error: {}", str(e))
        value["status"] = "validation_error"
        value["error_details"] = str(e)
        return (key, value)
      except TemporaryError as e:
        # Handle temporary errors that can be retried
        log.warn("Temporary error, will retry: {}", str(e))
        value["status"] = "retry"
        value["error_details"] = str(e)
        return (key, value)
      except Exception as e:
        # Handle unexpected errors
        log.error("Unexpected error: {}", str(e))
        value["status"] = "fatal_error"
        value["error_details"] = str(e)
        return (key, value)

pipelines:
  error_routing:
    from: input_stream
    transformKeyValue: classify_errors
    branch:
      - predicate: is_success
        to: success_stream
      - predicate: is_validation_error
        to: validation_errors
      - predicate: is_retry
        to: retry_queue
      - to: fatal_errors
```

## Implementing Recovery Mechanisms

### 1. Retry Logic

Implement retry logic for transient failures, such as temporary network issues:

```yaml
functions:
  retry_external_call:
    type: valueTransformer
    code: |
      max_retries = 3
      retry_count = 0
      
      while retry_count < max_retries:
        try:
          # Attempt to call external service
          result = call_external_service(value)
          return result
        except TemporaryError as e:
          # Log the retry attempt
          retry_count += 1
          log.warn("Retry {}/{} after error: {}", retry_count, max_retries, str(e))
          # Add exponential backoff
          time.sleep(0.1 * (2 ** retry_count))
        except Exception as e:
          # Non-retryable error
          log.error("Non-retryable error: {}", str(e))
          return {"error": str(e), "original_data": value}
      
      # If we've exhausted retries
      log.error("Failed after {} retries", max_retries)
      return {"error": "Max retries exceeded", "original_data": value}
```

### 2. Circuit Breakers

Implement circuit breaker patterns to prevent cascading failures when external systems are down:

```yaml
functions:
  circuit_breaker:
    type: valueTransformer
    globalCode: |
      # Circuit breaker state
      circuit_state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
      failure_count = 0
      last_failure_time = 0
      failure_threshold = 5
      reset_timeout = 30  # seconds
      
    code: |
      global circuit_state, failure_count, last_failure_time
      
      current_time = time.time()
      
      # Check if circuit is OPEN
      if circuit_state == "OPEN":
        # Check if it's time to try again
        if current_time - last_failure_time > reset_timeout:
          log.info("Circuit transitioning from OPEN to HALF_OPEN")
          circuit_state = "HALF_OPEN"
        else:
          # Circuit is still OPEN, fail fast
          log.warn("Circuit OPEN, skipping call")
          return {"error": "Circuit breaker open", "original_data": value}
      
      try:
        # Attempt the call
        result = call_external_service(value)
        
        # If successful and in HALF_OPEN, reset the circuit
        if circuit_state == "HALF_OPEN":
          log.info("Circuit transitioning from HALF_OPEN to CLOSED")
          circuit_state = "CLOSED"
          failure_count = 0
        
        return result
        
      except Exception as e:
        # Record the failure
        failure_count += 1
        last_failure_time = current_time
        
        # Check if we need to open the circuit
        if circuit_state == "CLOSED" and failure_count >= failure_threshold:
          log.warn("Circuit transitioning from CLOSED to OPEN after {} failures", failure_count)
          circuit_state = "OPEN"
        
        # Log and return error
        log.error("Service call failed: {}", str(e))
        return {"error": str(e), "original_data": value}
```

### 3. Compensating Transactions

For operations that need to maintain consistency across multiple systems, implement compensating transactions:

```yaml
functions:
  process_with_compensation:
    type: valueTransformer
    code: |
      # Track operations that need to be compensated if there's a failure
      completed_operations = []
      
      try:
        # First operation
        result1 = operation1(value)
        completed_operations.append("operation1")
        
        # Second operation
        result2 = operation2(result1)
        completed_operations.append("operation2")
        
        # Third operation
        result3 = operation3(result2)
        completed_operations.append("operation3")
        
        return result3
        
      except Exception as e:
        log.error("Error during processing: {}", str(e))
        
        # Perform compensating actions in reverse order
        for operation in reversed(completed_operations):
          try:
            if operation == "operation1":
              compensate_operation1(value)
            elif operation == "operation2":
              compensate_operation2(result1)
            elif operation == "operation3":
              compensate_operation3(result2)
          except Exception as comp_error:
            log.error("Error during compensation for {}: {}", operation, str(comp_error))
        
        # Return error information
        return {"error": str(e), "compensated": completed_operations, "original_data": value}
```

## Practical Example: Robust Order Processing

Let's build a complete example that implements robust error handling in an order processing system:

```yaml
streams:
  incoming_orders:
    topic: new_orders
    keyType: string  # Order ID
    valueType: json  # Order details
    
  validated_orders:
    topic: validated_orders
    keyType: string  # Order ID
    valueType: json  # Validated order details
    
  processed_orders:
    topic: processed_orders
    keyType: string  # Order ID
    valueType: json  # Processed order details
    
  invalid_orders:
    topic: invalid_orders
    keyType: string  # Order ID
    valueType: json  # Order with validation errors
    
  processing_errors:
    topic: order_processing_errors
    keyType: string  # Order ID
    valueType: json  # Order with processing errors
    
  retry_orders:
    topic: orders_to_retry
    keyType: string  # Order ID
    valueType: json  # Orders to retry later

functions:
  validate_order:
    type: keyValueTransformer
    code: |
      try:
        # Check if order has all required fields
        required_fields = ["customer_id", "items", "shipping_address", "payment_method"]
        missing_fields = [field for field in required_fields if field not in value or not value[field]]
        
        if missing_fields:
          # Return invalid order with details
          value["status"] = "invalid"
          value["validation_errors"] = {"missing_fields": missing_fields}
          value["timestamp"] = int(time.time() * 1000)
          return (key, value)
        
        # Check if items array is not empty
        if not value.get("items") or len(value["items"]) == 0:
          value["status"] = "invalid"
          value["validation_errors"] = {"reason": "Order contains no items"}
          value["timestamp"] = int(time.time() * 1000)
          return (key, value)
        
        # Order is valid
        value["status"] = "valid"
        value["validation_timestamp"] = int(time.time() * 1000)
        return (key, value)
        
      except Exception as e:
        # Handle unexpected errors during validation
        log.error("Error validating order {}: {}", key, str(e))
        value["status"] = "error"
        value["error_details"] = str(e)
        value["error_timestamp"] = int(time.time() * 1000)
        return (key, value)
  
  is_valid_order:
    type: predicate
    expression: value.get("status") == "valid"
    
  is_invalid_order:
    type: predicate
    expression: value.get("status") == "invalid"
    
  is_error_order:
    type: predicate
    expression: value.get("status") == "error"
    
  process_order:
    type: valueTransformer
    code: |
      if value is None or value.get("status") != "valid":
        log.warn("Received invalid order for processing: {}", key)
        return value
      
      try:
        # Simulate inventory check
        for item in value.get("items", []):
          # Simulate temporary failure for some items
          if "retry" in item.get("product_id", ""):
            raise TemporaryError(f"Temporary inventory issue with {item['product_id']}")
          
          # Simulate permanent failure for some items
          if "fail" in item.get("product_id", ""):
            raise PermanentError(f"Product {item['product_id']} is discontinued")
        
        # Simulate payment processing
        if value.get("payment_method") == "credit_card":
          # Add payment processing logic here
          value["payment_status"] = "processed"
        
        # Order successfully processed
        value["status"] = "processed"
        value["processing_timestamp"] = int(time.time() * 1000)
        return value
        
      except TemporaryError as e:
        # Handle temporary errors (can be retried)
        log.warn("Temporary error processing order {}: {}", key, str(e))
        value["status"] = "retry"
        value["retry_reason"] = str(e)
        value["retry_timestamp"] = int(time.time() * 1000)
        value["retry_count"] = value.get("retry_count", 0) + 1
        return value
        
      except Exception as e:
        # Handle permanent errors
        log.error("Error processing order {}: {}", key, str(e))
        value["status"] = "failed"
        value["error_details"] = str(e)
        value["error_timestamp"] = int(time.time() * 1000)
        return value
  
  is_processed_order:
    type: predicate
    expression: value.get("status") == "processed"
    
  is_retry_order:
    type: predicate
    expression: value.get("status") == "retry"
    
  is_failed_order:
    type: predicate
    expression: value.get("status") == "failed"

pipelines:
  # Validate incoming orders
  validate_orders:
    from: incoming_orders
    transformKeyValue: validate_order
    branch:
      - predicate: is_valid_order
        to: validated_orders
      - predicate: is_invalid_order
        to: invalid_orders
      - to: processing_errors  # Catch-all for unexpected errors
  
  # Process validated orders
  process_orders:
    from: validated_orders
    mapValues: process_order
    branch:
      - predicate: is_processed_order
        to: processed_orders
      - predicate: is_retry_order
        to: retry_orders
      - to: processing_errors  # Failed orders and unexpected errors
```

This example:
1. Validates incoming orders and routes them based on validation results
2. Processes valid orders with error handling for temporary and permanent failures
3. Routes processed orders to different destinations based on processing results
4. Implements a retry mechanism for orders with temporary issues

## Monitoring and Debugging

To effectively manage errors in your KSML applications:

### 1. Implement Comprehensive Logging

Use the KSML logging capabilities to track errors and their context:

```yaml
functions:
  log_with_context:
    type: forEach
    code: |
      # Log with different levels based on message status
      status = value.get("status", "unknown")
      
      if status == "error" or status == "failed":
        log.error("Order {} failed: {}", key, value.get("error_details", "Unknown error"))
      elif status == "retry":
        log.warn("Order {} needs retry: {}", key, value.get("retry_reason", "Unknown reason"))
      elif status == "invalid":
        log.warn("Order {} is invalid: {}", key, value.get("validation_errors", "Unknown validation error"))
      else:
        log.info("Processing order {}: status={}", key, status)
```

### 2. Use Metrics to Track Error Rates

Track error metrics to monitor the health of your application:

```yaml
functions:
  track_error_metrics:
    type: forEach
    code: |
      # Get status
      status = value.get("status", "unknown")
      
      # Update appropriate counter based on status
      if status == "valid":
        metrics.counter("orders.valid").increment()
      elif status == "invalid":
        metrics.counter("orders.invalid").increment()
      elif status == "processed":
        metrics.counter("orders.processed").increment()
      elif status == "failed":
        metrics.counter("orders.failed").increment()
      elif status == "retry":
        metrics.counter("orders.retry").increment()
      else:
        metrics.counter("orders.unknown").increment()
```

### 3. Implement Health Checks

Create health check streams that monitor error rates and alert when they exceed thresholds.

## Best Practices for Error Handling

- **Fail Fast**: Validate input data early to catch issues before expensive processing
- **Be Specific**: Catch specific exceptions rather than using broad exception handlers
- **Provide Context**: Include relevant information in error messages and logs
- **Design for Failure**: Assume that errors will occur and design your pipelines accordingly
- **Isolate Failures**: Use circuit breakers to prevent cascading failures
- **Monitor and Alert**: Set up monitoring and alerting for error conditions
- **Test Error Scenarios**: Explicitly test error handling code with simulated failures

## Conclusion

Robust error handling is essential for building reliable stream processing applications. KSML provides flexible mechanisms for handling errors at various levels, from simple try-except blocks to sophisticated patterns like dead letter queues and circuit breakers.

By implementing proper error handling and recovery mechanisms, you can build KSML applications that gracefully handle failures, maintain data integrity, and continue processing even when problems occur.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Reference: Error Handling](../../reference/error-handling-reference.md)