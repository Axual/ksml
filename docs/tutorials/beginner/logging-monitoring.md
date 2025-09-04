# Logging and Monitoring in KSML

Learn how to implement effective logging, monitoring, and error handling in your KSML stream processing pipelines using the built-in `log` object and peek operations.

## Prerequisites

Before we begin:

- Please make sure there is a running Docker Compose KSML environment as described in the [Quick Start](../../getting-started/quick-start.md).
- We recommend to have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    # Logging and Monitoring Tutorial
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_logging_input && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_logging_output && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_monitoring_output && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ksml_error_handled_output && \
    ```

## Basic Logging with Different Levels

KSML allows you to log messages at different levels using the `log` object in Python functions.

This producer generates log messages with various importance levels and components:

??? info "Producer definition for logging messages (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/logging-and-monitoring/logging-producer.yaml"
    %}
    ```

### KSML Features Demonstrated:
- **`log` object**: Built-in logger available in all Python functions
- **Log levels**: `log.error()`, `log.warn()`, `log.info()`, `log.debug()`
- **`peek` operation**: Non-intrusive message inspection without modification
- **`forEach` function type**: Process messages without transforming them

This processor demonstrates logging at different levels using the `log` object:

??? info "Processor definition with multi-level logging (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/logging-and-monitoring/logging-processor.yaml"
    %}
    ```

## Monitoring with Peek Operations

The `peek` operation allows non-intrusive monitoring of message flow without modifying the data.

### KSML Features Demonstrated:
- **`peek` operation**: Inspect messages in the pipeline without modifying them
- **Global variables**: Using `globals()` to maintain state across function calls
- **Conditional logging**: Log only when specific conditions are met
- **Message counting**: Track processed messages across invocations

This processor shows message counting and error detection using peek operations:

??? info "Processor definition for monitoring operations (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/logging-and-monitoring/monitoring-simple-processor.yaml"
    %}
    ```

## Error Handling with Logging

Use try-catch blocks in Python functions to handle errors gracefully and log them appropriately.

### KSML Features Demonstrated:
- **`transformValue` operation**: Transform message values with error handling
- **`valueTransformer` function type**: Returns transformed values
- **Try-except blocks**: Safe processing with error catching
- **Structured logging**: Format logs with timestamps and component info
- **`time.strftime()`**: Format timestamps for readable logs

This processor demonstrates basic error handling with logging:

??? info "Processor definition with error handling (click to expand)"

    ```yaml
    {%
      include "../../definitions/beginner-tutorial/logging-and-monitoring/error-handling-processor.yaml"
    %}
    ```

## Configuring Log Levels

KSML provides standard logging levels through the `log` object available in Python functions:

- **ERROR**: Critical errors requiring attention
- **WARN**: Potential issues or unusual conditions  
- **INFO**: Normal operational events
- **DEBUG**: Detailed troubleshooting information
- **TRACE**: Very detailed debugging output

By default, KSML shows INFO, WARN, and ERROR logs. You can enable DEBUG and TRACE logging without rebuilding the image.

### Enabling All Log Levels (Including DEBUG and TRACE)

- Create a custom logback configuration file `logback-trace.xml` in your `examples` directory:

??? info "Custom logback configuration for TRACE logging (click to expand)"

    ```xml
    {%
      include "../../other-files/logback-trace.xml"
    %}
    ```

- Update your `docker-compose.yml` to use the custom configuration:

```yaml
ksml:
  environment:
    - LOGBACK_CONFIGURATION_FILE=/ksml/logback-trace.xml
```

- Restart the containers to see all log levels including DEBUG and TRACE.
    - To test, add for example `log.trace("TRACE: Processing message with key={}", key)` into your processing definition.

## Best Practices

1. **Use appropriate log levels**: 
    - ERROR for failures, WARN for issues, INFO for events, DEBUG for details
2. **Include context**: 
    - Add message keys, component names, and relevant metadata
3. **Avoid excessive logging**:
    - Use sampling or conditional logging for high-volume streams
4. **Structure messages**:
    - Use consistent formats with key-value pairs
5. **Monitor performance**:
    - Track throughput, processing time, and error rates

## Conclusion

Effective logging and monitoring enable you to track pipeline behavior, diagnose issues quickly, and maintain reliable KSML applications. Use the `log` object for different severity levels and `peek` operations for non-intrusive monitoring.

## Next Steps

- [Error Handling and Recovery](../intermediate/error-handling.md) for advanced error handling techniques
- [Performance Optimization](../advanced/performance-optimization.md) for optimizing pipeline performance  
- [Intermediate Tutorials](../intermediate/index.md) for more advanced KSML features