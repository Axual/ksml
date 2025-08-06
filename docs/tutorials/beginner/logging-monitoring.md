# Logging and Monitoring in KSML

Learn how to implement effective logging, monitoring, and error handling in your KSML stream processing pipelines using the built-in `log` object and peek operations.

## Prerequisites

Before we begin:

- Make sure there is a running Docker Compose KSML environment as described in the [Quick Start](../../getting-started/installation.md).
    - Also please make sure you have `ksml-runner.yaml` defined as described in the [Quick Start](../../getting-started/installation.md).
- We recommend to have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)

## Logging Levels

KSML provides standard logging levels through the `log` object available in Python functions:

- **ERROR**: Critical errors requiring attention
- **WARN**: Potential issues or unusual conditions  
- **INFO**: Normal operational events
- **DEBUG**: Detailed troubleshooting information
- **TRACE**: Very detailed debugging output

## Basic Logging with Different Levels

KSML allows you to log messages at different levels using the `log` object in Python functions.

This producer generates log messages with various importance levels and components:

??? info "Producer definition for logging messages (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup/examples/logging-producer.yaml"
    %}
    ```

This processor demonstrates logging at different levels and filtering based on message importance:

??? info "Processor definition with multi-level logging (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup/examples/logging-processor.yaml"
    %}
    ```

## Monitoring Message Flow with Peek

The `peek` operation allows non-intrusive monitoring of message flow without modifying the data.

This processor shows message counting and error detection using peek operations:

??? info "Processor definition for monitoring operations (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup/examples/monitoring-processor.yaml"
    %}
    ```

## Error Handling with Logging

Use try-catch blocks in Python functions to handle errors gracefully and log them appropriately.

This processor demonstrates safe transformations with comprehensive error handling:

??? info "Processor definition with error handling (click to expand)"

    ```yaml
    {%
      include "../../local-docker-compose-setup/examples/error-handling-processor.yaml"
    %}
    ```

## Running with Different Log Levels

Configure KSML Runner to show different amounts of logging detail:

```bash
# Run with INFO level (default) 
java -jar ksml-runner.jar examples/ksml-runner.yaml

# Run with DEBUG level for detailed logs
java -jar ksml-runner.jar examples/ksml-runner.yaml --logging.level.io.axual.ksml=DEBUG

# Run with TRACE level for maximum detail
java -jar ksml-runner.jar examples/ksml-runner.yaml --logging.level.root=TRACE
```

## Practical Logging Patterns

### Conditional Logging
Log only when specific conditions are met:

```python
# Only log high-importance messages
if value.get("importance", 0) > 8:
    log.warn("Critical message: {}", value.get("message"))
```

### Structured Logging
Include context for easier parsing:

```python
# Include timestamp and component info
timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
log.info("[{}] Component: {} | Message: {}", 
         timestamp, value.get("component"), value.get("message"))
```

### Performance Monitoring
Track processing time for operations:

```python
start_time = time.time()
# ... processing logic ...
duration = (time.time() - start_time) * 1000
log.debug("Processing took {:.2f}ms", duration)
```

### Message Counting
Periodically log throughput metrics:

```python
global message_count
message_count = message_count + 1 if 'message_count' in globals() else 1
if message_count % 100 == 0:
    log.info("Processed {} messages", message_count)
```

## Best Practices

1. **Use appropriate log levels**: ERROR for failures, WARN for issues, INFO for events, DEBUG for details
2. **Include context**: Add message keys, component names, and relevant metadata
3. **Avoid excessive logging**: Use sampling or conditional logging for high-volume streams
4. **Structure messages**: Use consistent formats with key-value pairs
5. **Monitor performance**: Track throughput, processing time, and error rates

## Conclusion

Effective logging and monitoring enable you to track pipeline behavior, diagnose issues quickly, and maintain reliable KSML applications. Use the `log` object for different severity levels and `peek` operations for non-intrusive monitoring.

## Next Steps

- [Error Handling and Recovery](../intermediate/error-handling.md) - Advanced error handling techniques
- [Performance Optimization](../advanced/performance-optimization.md) - Optimize pipeline performance  
- [Intermediate Tutorials](../intermediate/index.md) - More advanced KSML features