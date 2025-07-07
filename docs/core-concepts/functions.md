# Functions

Discover how to use Python functions in your KSML applications and understand their role in stream processing.

## What are Functions in KSML?

Functions in KSML allow you to implement custom logic for processing your streaming data. They provide the flexibility to go beyond the built-in operations and implement specific business logic, transformations, and data processing requirements.

KSML functions are written in Python, making them accessible to data scientists, analysts, and developers who may not be familiar with Java or Kafka Streams API. This approach combines the power of Kafka Streams with the simplicity and expressiveness of Python.

## Types of Functions in KSML

KSML supports various function types, each designed for specific purposes in stream processing:

### Stateless Functions

These functions process each message independently without maintaining state between invocations:

- **Predicates**: Return true or false based on message content, used for filtering and branching
- **Mappers**: Transform messages from one form to another
- **ForEach**: Process each message for side effects (like logging)

### Stateful Functions

These functions maintain state across multiple messages:

- **Aggregators**: Incrementally build aggregated results from multiple messages
- **Reducers**: Combine two values into one
- **Initializers**: Provide initial values for aggregations

### Special Purpose Functions

- **Transformers**: Advanced functions that can access state stores and process records
- **Joiners**: Combine data from multiple streams
- **Extractors**: Extract specific information from messages (timestamps, topic names, etc.)

## Writing Python Functions

KSML functions are defined in the `functions` section of your KSML definition file. A typical function definition includes:

- **Type**: Specifies the function's purpose and behavior
- **Parameters**: Input parameters the function accepts
- **Code**: Python code implementing the function's logic
- **Expression**: A shorthand for simple return expressions

Functions can range from simple one-liners to complex implementations with multiple operations.

## Function Parameters and Return Types

Each function type in KSML has specific parameters and return types. For example:

- A **predicate** receives a key and value and returns a boolean
- A **mapper** receives a key and value and returns a transformed key and value
- An **aggregator** receives a key, value, and aggregated value, and returns a new aggregated value

Understanding these signatures is crucial for implementing functions correctly.

## Function Execution Context

When your Python functions execute, they have access to:

- **Logger**: For outputting information to the application logs
- **Metrics**: For monitoring function performance and behavior
- **State Stores**: For maintaining state between function invocations (when configured)

This execution context provides the tools needed for debugging, monitoring, and implementing stateful processing.

## Best Practices for Functions

- **Keep functions focused**: Each function should do one thing well
- **Handle errors gracefully**: Use try/except blocks to prevent pipeline failures
- **Consider performance**: Python functions introduce some overhead, so keep them efficient
- **Use appropriate function types**: Choose the right function type for your use case
- **Leverage state stores**: For complex stateful operations, use state stores rather than global variables

## Examples

### Simple Predicate Function

```yaml
functions:
  temperature_filter:
    type: predicate
    expression: value.get('temperature') > 30
```

### Value Transformation Function

```yaml
functions:
  celsius_to_fahrenheit:
    type: valueTransformer
    code: |
      if value is not None and 'temperature' in value:
        celsius = value['temperature']
        value['temperature_f'] = (celsius * 9/5) + 32
      return value
```

### Stateful Aggregation Function

```yaml
functions:
  average_calculator:
    type: aggregator
    code: |
      if aggregatedValue is None:
        return {'count': 1, 'sum': value['amount'], 'average': value['amount']}
      else:
        count = aggregatedValue['count'] + 1
        sum = aggregatedValue['sum'] + value['amount']
        return {'count': count, 'sum': sum, 'average': sum / count}
```

## Related Topics

- [Pipelines](pipelines.md): Learn how functions fit into the overall pipeline structure
- [Operations](operations.md): Discover the operations that use functions
- [Streams and Data Types](streams-and-data-types.md): Understand the data types that functions work with

By mastering functions in KSML, you gain the ability to implement custom logic that goes beyond the built-in operations, allowing you to solve complex stream processing challenges with elegant Python code.