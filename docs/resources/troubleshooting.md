# KSML Troubleshooting Guide

This guide provides solutions for common issues you might encounter when working with KSML. It covers problems related to development, deployment, and runtime behavior.

## Common Error Messages

### Syntax Errors

#### YAML Parsing Errors

**Error**: `Error parsing YAML: mapping values are not allowed here`

**Solution**: Check your YAML indentation. YAML is sensitive to spaces and tabs. Make sure you're using consistent indentation (preferably spaces) throughout your KSML file.

```yaml
# Incorrect
streams:
  my_stream:
  topic: my-topic  # Wrong indentation (topic should be indented under my_stream)

# Correct
streams:
  my_stream:
    topic: my-topic  # Proper indentation (topic is properly indented under my_stream)
```

**Error**: `Error parsing YAML: found character that cannot start any token`

**Solution**: Look for special characters that might be causing issues. Common culprits include:
- Unescaped quotes within strings
- Special characters in keys or values
- Tab characters instead of spaces

#### Function Definition Errors

**Error**: `Function 'process_data' is missing required parameter 'type'`

**Solution**: Ensure all functions have the required `type` parameter:

```yaml
functions:
  process_data:
    # Missing type parameter
    parameters:
      - name: value
        type: object
    code: |
      return value
```

Correct version:

```yaml
functions:
  process_data:
    type: mapValues  # Added type parameter
    parameters:
      - name: value
        type: object
    code: |
      return value
```

#### Pipeline Definition Errors

**Error**: `Pipeline 'my_pipeline' is missing required parameter 'from'`

**Solution**: Ensure all pipelines have the required `from` and `to` parameters:

```yaml
pipelines:
  my_pipeline:
    # Missing 'from' parameter
    via:
      - type: mapValues
        mapper:
          code: transform_data(value)
    to: output_stream
```

Correct version:

```yaml
pipelines:
  my_pipeline:
    from: input_stream  # Added 'from' parameter
    via:
      - type: mapValues
        mapper:
          code: transform_data(value)
    to: output_stream
```

### Runtime Errors

#### Serialization/Deserialization Errors

**Error**: `Failed to deserialize key/value for topic 'my-topic'`

**Solution**: 
1. Check that the `keyType` and `valueType` in your stream definition match the actual data format in the Kafka topic
2. Verify that your data conforms to the expected schema
3. For Avro or other schema-based formats, ensure the schema is correctly registered and accessible

```yaml
streams:
  my_stream:
    topic: my-topic
    keyType: string
    valueType: json  # Make sure this matches the actual data format
```

#### Python Code Errors

**Error**: `AttributeError: 'NoneType' object has no attribute 'get'`

**Solution**: Add null checks before accessing object properties:

```python
# Incorrect
result = value.get("field").get("nested_field")

# Correct
result = None
if value is not None:
    field_value = value.get("field")
    if field_value is not None:
        result = field_value.get("nested_field")
```

**Error**: `NameError: name 'some_variable' is not defined`

**Solution**: Ensure all variables are defined before use. Common issues include:
- Typos in variable names
- Using variables from outside the function scope
- Forgetting to import required modules

#### Join Operation Errors

**Error**: `Stream 'reference_data' not found for join operation`

**Solution**: Verify that all streams referenced in join operations are defined:

```yaml
streams:
  input_stream:
    topic: input-topic
    keyType: string
    valueType: json

  # Missing reference_data stream definition

pipelines:
  join_pipeline:
    from: input_stream
    via:
      - type: join
        with: reference_data  # This stream is not defined
    to: output_stream
```

**Error**: `Join operation requires keys of the same type`

**Solution**: Ensure that the key types of streams being joined are compatible:

```yaml
streams:
  orders:
    topic: orders
    keyType: string
    valueType: json

  products:
    topic: products
    keyType: long  # Different key type
    valueType: json
```

Correct version:

```yaml
streams:
  orders:
    topic: orders
    keyType: string
    valueType: json

  products:
    topic: products
    keyType: string  # Same key type as orders
    valueType: json
```

## Deployment Issues

### Configuration Problems

**Error**: `Failed to start KSML application: Could not connect to Kafka broker`

**Solution**:
1. Verify that the Kafka brokers are running and accessible
2. Check the `bootstrap.servers` configuration
3. Ensure network connectivity between the KSML application and Kafka brokers
4. Check firewall rules that might be blocking connections

```yaml
configuration:
  bootstrap.servers: kafka:9092  # Make sure this is correct
```

**Error**: `Topic 'my-topic' not found`

**Solution**:
1. Verify that the topic exists in your Kafka cluster
2. Check if you have the correct permissions to access the topic
3. Ensure the topic name is spelled correctly in your KSML definition

### Resource Constraints

**Error**: `OutOfMemoryError: Java heap space`

**Solution**:
1. Increase the JVM heap size for the KSML runner
2. Review your stateful operations (joins, aggregations) for potential memory leaks
3. Consider using smaller time windows for windowed operations
4. Implement proper cleanup for state stores when they're no longer needed

```bash
# Example of increasing heap size
java -Xmx2g -jar ksml-runner.jar
```

## Performance Issues

### High Latency

**Symptom**: Messages take a long time to process through the pipeline

**Solutions**:
1. **Optimize Python Code**: Review your Python functions for inefficiencies
2. **Reduce State Size**: Minimize the amount of data stored in stateful operations
3. **Adjust Parallelism**: Increase the number of partitions for input topics
4. **Monitor Resource Usage**: Check CPU, memory, and network utilization
5. **Simplify Complex Operations**: Break down complex operations into simpler ones

### Low Throughput

**Symptom**: Pipeline processes fewer messages per second than expected

**Solutions**:
1. **Batch Processing**: Use batch processing where appropriate
2. **Optimize Serialization**: Choose efficient serialization formats (e.g., Avro instead of JSON)
3. **Reduce External Calls**: Minimize calls to external systems
4. **Caching**: Implement caching for frequently accessed data
5. **Horizontal Scaling**: Run multiple instances of your KSML application

## Debugging Techniques

### Logging

Add logging statements to your KSML functions to track execution:

```yaml
- type: peek
  forEach:
    code: |
      log.info("Processing record: key={}, value={}", key, value)
```

For more detailed logging, you can include specific fields:

```yaml
- type: peek
  forEach:
    code: |
      log.info("Order details: id={}, amount={}, customer={}",
               value.get("order_id"),
               value.get("amount"),
               value.get("customer_id"))
```

### Dead Letter Queues

Implement dead letter queues to capture and analyze failed records:

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

  errors:
    topic: error-topic
    keyType: string
    valueType: json

pipelines:
  main_pipeline:
    from: input
    via:
      - type: mapValues
        mapper:
          code: process_data(value)
        onError:
          sendTo: errors
          withKey: key
          withValue: {"original": value, "error": exception.getMessage()}
    to: output
```

### Testing with Sample Data

Create test pipelines that process a small sample of data:

```yaml
streams:
  test_input:
    topic: test-input
    keyType: string
    valueType: json

  test_output:
    topic: test-output
    keyType: string
    valueType: json

pipelines:
  test_pipeline:
    from: test_input
    via:
      - type: peek
        forEach:
          code: |
            log.info("Input: {}", value)
      - type: mapValues
        mapper:
          code: process_data(value)
      - type: peek
        forEach:
          code: |
            log.info("Output: {}", value)
    to: test_output
```

## Common Patterns and Anti-Patterns

### Recommended Patterns

#### Proper Error Handling

Always implement error handling to prevent pipeline failures:

```yaml
- type: try
  operations:
    - type: mapValues
      mapper:
        code: process_data(value)
  catch:
    - type: mapValues
      mapper:
        code: |
          log.error("Failed to process data: {}", exception)
          return {"error": str(exception), "original": value}
```

#### Defensive Programming

Always check for null values and handle edge cases:

```python
def process_data(value):
    if value is None:
        return {"error": "Null input"}

    user_id = value.get("user_id")
    if user_id is None:
        return {"error": "Missing user_id"}

    # Process valid data
    return {"user_id": user_id, "processed": True}
```

#### Modular Functions

Break down complex logic into smaller, reusable functions:

```yaml
functions:
  validate_input:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      # Validation logic

  transform_data:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      # Transformation logic

  enrich_data:
    type: mapValues
    parameters:
      - name: value
        type: object
      - name: reference
        type: object
    code: |
      # Enrichment logic

pipelines:
  main_pipeline:
    from: input
    via:
      - type: mapValues
        mapper:
          code: validate_input(value)
      - type: mapValues
        mapper:
          code: transform_data(value)
      - type: join
        with: reference_data
      - type: mapValues
        mapper:
          code: enrich_data(value, foreignValue)
    to: output
```

### Anti-Patterns to Avoid

#### Excessive State

Avoid storing large amounts of data in state stores:

```yaml
# Anti-pattern: Storing entire message history
- type: aggregate
  initializer:
    expression: []
  aggregator:
    code: |
      if aggregate is None:
        return [value]
      else:
        return aggregate + [value]  # This will grow unbounded!
```

Better approach:

```yaml
# Better: Store only what you need
- type: aggregate
  initializer:
    expression: {"count": 0, "sum": 0}
  aggregator:
    code: |
      if aggregate is None:
        return {"count": 1, "sum": value.get("amount", 0)}
      else:
        return {
          "count": aggregate.get("count", 0) + 1,
          "sum": aggregate.get("sum", 0) + value.get("amount", 0)
        }
```

#### Complex Single Functions

Avoid putting too much logic in a single function:

```yaml
# Anti-pattern: Monolithic function
functions:
  do_everything:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      # 100+ lines of code that does validation, transformation,
      # business logic, error handling, etc.
```

#### Ignoring Errors

Don't ignore exceptions without proper handling:

```yaml
# Anti-pattern: Swallowing exceptions
- type: mapValues
  mapper:
    code: |
      try:
        # Complex processing
        return process_result
      except:
        # Silently return empty result
        return {}
```

## Getting Help

If you're still experiencing issues after trying the solutions in this guide:

1. **Check the Documentation**: Review the [KSML Language Reference](../reference/language-reference.md) and [Operations Reference](../reference/operations-reference.md)
2. **Search the Community Forums**: Look for similar issues in the community forums
3. **Examine Logs**: Check the KSML runner logs for detailed error information
4. **Create a Minimal Example**: Create a simplified version of your pipeline that reproduces the issue
5. **Contact Support**: Reach out to the KSML support team with:
   - A clear description of the issue
   - Steps to reproduce
   - Relevant logs and error messages
   - Your KSML definition file (with sensitive information removed)

## Conclusion

Troubleshooting KSML applications involves understanding both the KSML language itself and the underlying Kafka Streams concepts. By following the patterns and solutions in this guide, you can resolve common issues and build more robust stream processing applications.

Remember that proper logging, error handling, and testing are key to identifying and resolving issues quickly. When in doubt, start with a simplified version of your pipeline and gradually add complexity while testing at each step.
