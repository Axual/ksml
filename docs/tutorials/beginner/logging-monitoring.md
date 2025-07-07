# Logging and Monitoring in KSML

This tutorial guides you through implementing effective logging and monitoring in your KSML applications. By the end, you'll understand how to track your pipeline's behavior, debug issues, and monitor performance.

## Prerequisites

- Basic understanding of KSML concepts
- Completed the [Building a Simple Data Pipeline](/docs/tutorials/beginner/simple-pipeline.md) tutorial
- A running Kafka environment (as set up in the previous tutorial)

## What You'll Learn

In this tutorial, you'll learn how to:
1. Implement different levels of logging in KSML
2. Use the peek operation for monitoring message flow
3. Create custom monitoring functions
4. Configure logging settings
5. Implement basic error handling with logging

## Understanding Logging in KSML

KSML provides built-in logging capabilities through the `log` object, which is available in Python functions and expressions. The logging system follows standard logging levels:

- **ERROR**: For error events that might still allow the application to continue running
- **WARN**: For potentially harmful situations
- **INFO**: For informational messages highlighting the progress of the application
- **DEBUG**: For detailed information, typically useful only when diagnosing problems
- **TRACE**: For even more detailed information than DEBUG

## Basic Logging Examples

### Step 1: Create a KSML File with Logging

Create a file named `logging-example.yaml` with the following content:

```yaml
streams:
  input_stream:
    topic: logging-input
    keyType: string
    valueType: json
  output_stream:
    topic: logging-output
    keyType: string
    valueType: json

functions:
  log_with_level:
    type: forEach
    parameters:
      - name: level
        type: string
      - name: message
        type: string
    code: |
      if level == "ERROR":
        log.error(message)
      elif level == "WARN":
        log.warn(message)
      elif level == "INFO":
        log.info(message)
      elif level == "DEBUG":
        log.debug(message)
      elif level == "TRACE":
        log.trace(message)
      else:
        log.info(message)

pipelines:
  logging_pipeline:
    from: input_stream
    via:
      - type: peek
        forEach:
          functionRef: log_with_level
          args:
            level: "INFO"
            message: "Received message with key: {} and value: {}"
      - type: filter
        if:
          expression: value.get('importance') > 3
      - type: peek
        forEach:
          functionRef: log_with_level
          args:
            level: "DEBUG"
            message: "Message passed filter: {}"
      - type: mapValues
        mapper:
          expression: dict(list(value.items()) + [("processed_at", time.time())])
      - type: peek
        forEach:
          code: |
            log.info("Sending processed message to output: {}", value)
    to: output_stream
```

This pipeline:
1. Logs every incoming message at INFO level
2. Filters messages based on an 'importance' field
3. Logs messages that pass the filter at DEBUG level
4. Adds a timestamp to each message
5. Logs the final processed message before sending it to the output topic

### Step 2: Create Kafka Topics

Create the input and output topics:

```bash
docker-compose exec kafka kafka-topics --create --topic logging-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic logging-output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Step 3: Run the Pipeline with Different Log Levels

Run the pipeline with different log levels to see how it affects the output:

```bash
# Run with INFO level (default)
ksml-runner --config logging-example.yaml

# Run with DEBUG level to see more detailed logs
ksml-runner --config logging-example.yaml --log-level DEBUG

# Run with TRACE level to see all logs
ksml-runner --config logging-example.yaml --log-level TRACE
```

### Step 4: Produce Test Messages

In a new terminal, produce some test messages:

```bash
docker-compose exec kafka kafka-console-producer --topic logging-input --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"
```

Enter messages in the format `key:value`, for example:

```
message1:{"importance": 5, "content": "High importance message"}
message2:{"importance": 2, "content": "Low importance message"}
message3:{"importance": 4, "content": "Medium-high importance message"}
```

## Advanced Logging Techniques

### Creating a Detailed Logging Function

Add a more detailed logging function to your KSML file:

```yaml
functions:
  detailed_logger:
    type: forEach
    parameters:
      - name: stage
        type: string
    code: |
      timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
      log.info("[{}] Stage: {} | Key: {} | Value Type: {} | Value: {}", 
               timestamp, stage, key, type(value).__name__, value)
```

Use this function in your pipeline:

```yaml
- type: peek
  forEach:
    functionRef: detailed_logger
    args:
      stage: "Pre-processing"
```

### Conditional Logging

You can implement conditional logging to only log messages that meet certain criteria:

```yaml
functions:
  conditional_logger:
    type: forEach
    code: |
      if "error" in value or value.get("status") == "failed":
        log.error("Error detected in message: {}", value)
      elif value.get("importance", 0) > 8:
        log.warn("High importance message detected: {}", value)
```

## Monitoring Message Flow

### Using Peek for Monitoring

The `peek` operation is a powerful tool for monitoring message flow without modifying the messages:

```yaml
- type: peek
  forEach:
    code: |
      global message_count
      if 'message_count' not in globals():
        message_count = 0
      message_count += 1
      if message_count % 100 == 0:
        log.info("Processed {} messages", message_count)
```

### Monitoring Processing Time

You can monitor how long operations take:

```yaml
functions:
  time_operation:
    type: mapper
    parameters:
      - name: operation_name
        type: string
    code: |
      start_time = time.time()
      result = yield
      end_time = time.time()
      log.info("Operation '{}' took {:.3f} ms", operation_name, (end_time - start_time) * 1000)
      return result
```

Use this function to wrap operations:

```yaml
- type: process
  processor:
    functionRef: time_operation
    args:
      operation_name: "Complex Transformation"
    next:
      - type: mapValues
        mapper:
          expression: # Your complex transformation here
```

## Error Handling with Logging

### Catching and Logging Errors

Use try-except blocks in your Python functions to catch and log errors:

```yaml
functions:
  safe_transform:
    type: mapper
    code: |
      try:
        # Attempt the transformation
        result = {"processed": value.get("data") * 2, "status": "success"}
        return result
      except Exception as e:
        # Log the error and return a fallback value
        log.error("Error processing message: {} - Error: {}", value, str(e))
        return {"processed": None, "status": "error", "error_message": str(e)}
```

## Configuring Logging

### Log Configuration in KSML Runner

The KSML Runner supports various logging configuration options:

```bash
# Set log level
ksml-runner --config your-pipeline.yaml --log-level INFO

# Log to a file
ksml-runner --config your-pipeline.yaml --log-file pipeline.log

# Configure log format
ksml-runner --config your-pipeline.yaml --log-format "%(asctime)s [%(levelname)s] %(message)s"
```

## Best Practices for Logging and Monitoring

1. **Use appropriate log levels**:
   - ERROR: For actual errors that need attention
   - WARN: For potential issues or unusual conditions
   - INFO: For normal but significant events
   - DEBUG: For detailed troubleshooting information
   - TRACE: For very detailed debugging information

2. **Include context in log messages**:
   - Message keys and values (or summaries for large values)
   - Operation or stage name
   - Timestamps
   - Relevant metrics or counters

3. **Avoid excessive logging**:
   - Don't log every message at high volume
   - Use sampling or periodic logging for high-throughput pipelines
   - Consider conditional logging based on message content

4. **Structure your logs**:
   - Use consistent formats
   - Include key-value pairs for easier parsing
   - Consider using JSON formatting for machine-readable logs

5. **Monitor performance metrics**:
   - Message throughput
   - Processing time
   - Error rates
   - Resource usage (memory, CPU)

## Next Steps

Now that you've learned about logging and monitoring in KSML, you can:

- Explore [intermediate tutorials](/docs/tutorials/intermediate/index.md) to learn about more advanced KSML features
- Learn about [error handling and recovery](/docs/tutorials/intermediate/error-handling.md) in more detail
- Dive into [performance optimization](/docs/tutorials/advanced/performance-optimization.md) techniques

## Conclusion

Effective logging and monitoring are essential for building robust KSML applications. By implementing the techniques covered in this tutorial, you'll be able to:

- Track the behavior of your pipelines
- Quickly identify and diagnose issues
- Monitor performance and resource usage
- Create more maintainable and reliable applications

Remember that good logging practices are a balance between capturing enough information to be useful and avoiding excessive logging that can impact performance.
