# Functions

Discover how to use Python functions in your KSML applications and understand their role in stream processing.

## What are Functions in KSML?

Functions in KSML allow you to implement custom logic for processing your streaming data. They provide the flexibility
to go beyond the built-in operations and implement specific business logic, transformations, and data processing
requirements.

KSML functions are written in Python, making them accessible to data scientists, analysts, and developers who may not be
familiar with Java or Kafka Streams API. This approach combines the power of Kafka Streams with the simplicity and
expressiveness of Python.

## Types of Functions in KSML

KSML supports various function types, each designed for specific purposes in stream processing:

### Functions used by stateless operations

These functions process each message independently without maintaining state between invocations:

- [forEach](../reference/function-reference.md#foreach): Process each message for side effects
- [keyTransformer](../reference/function-reference.md#keytransformer): Convert a key to another type or value
- [keyValueToKeyValueListTransformer](../reference/function-reference.md#keyvaluetokeyvaluelisttransformer): Convert key and value to a list of key/values
- [keyValueToValueListTransformer](../reference/function-reference.md#keyvaluetovaluelisttransformer): Convert key and value to a list of values
- [keyValueTransformer](../reference/function-reference.md#keyvaluetransformer): Convert key and value to another key and value
- [predicate](../reference/function-reference.md#predicate): Return true/false based on message content
- [valueTransformer](../reference/function-reference.md#valuetransformer): Convert value to another type or value

### Functions used by stateful operations

These functions maintain state across multiple messages:

- [aggregator](../reference/function-reference.md#aggregator): Incrementally build aggregated results
- [initializer](../reference/function-reference.md#initializer): Provide initial values for aggregations
- [merger](../reference/function-reference.md#merger): Merge two aggregation results into one
- [reducer](../reference/function-reference.md#reducer): Combine two values into one

### Special Purpose Functions

- [foreignKeyExtractor](../reference/function-reference.md#foreignkeyextractor): Extract a key from a join table's record
- [generator](../reference/function-reference.md#generator): Function used in producers to generate a message
- [keyValueMapper](../reference/function-reference.md#keyvaluemapper): Convert key and value into a single output value
- [keyValuePrinter](../reference/function-reference.md#keyvalueprinter): Output key and value
- [metadataTransformer](../reference/function-reference.md#metadatatransformer): Convert Kafka headers and timestamps
- [valueJoiner](../reference/function-reference.md#valuejoiner): Combine data from multiple streams

### Stream Related Functions

- [timestampExtractor](../reference/function-reference.md#timestampextractor): Extract timestamps from messages
- [topicNameExtractor](../reference/function-reference.md#topicnameextractor): Derive a target topic name from key and value
- [streamPartitioner](../reference/function-reference.md#streampartitioner): Determine to which partition(s) a record is produced

### Other Functions
- [generic](../reference/function-reference.md#generic): Generic custom function

## Writing Python Functions

KSML functions are defined in the `functions` section of your KSML definition file. A typical function definition
includes:

- **Type**: Specifies the function's purpose and behavior
- **Parameters**: Input parameters the function accepts
- **GlobalCode**: Python code that is executed only once upon application start
- **Code**: Python code implementing the function's logic
- **Expression**: Shorthand for simple return expressions

Functions can range from simple one-liners to complex implementations with multiple operations.

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
{% include "../../examples/reference/functions/predicate-example.yaml" %}
```

### Value Transformation Function

```yaml
{% include "../../examples/reference/functions/valuetransformer-example.yaml" %}
```

### Stateful Aggregation Function

```yaml
{% include "../../examples/reference/functions/aggregator-example.yaml" %}
```

## Related Topics

- [Pipelines](pipelines.md): Learn how functions fit into the overall pipeline structure
- [Operations](operations.md): Discover the operations that use functions
- [Streams and Data Types](../reference/stream-types-reference.md): Understand the data types that functions work with

By mastering functions in KSML, you gain the ability to implement custom logic that goes beyond the built-in operations,
allowing you to solve complex stream processing challenges with elegant Python code.
