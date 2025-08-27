# Reference Documentation

Welcome to the KSML Reference Documentation! This section provides comprehensive technical details about all aspects of KSML. It serves as a complete reference for KSML syntax, operations, functions, data types, and configuration options.

Understanding these fundamental components will give you a solid foundation for building effective stream processing applications with KSML, regardless of the specific use case or complexity level.

## Reference Sections

### [KSML Language Reference](language-reference.md)

Complete documentation of the KSML language syntax and structure:

- YAML structure and formatting
- Definition file organization
- Syntax rules and conventions
- Schema validation
- Common syntax patterns

### [Stream Type Reference](stream-type-reference.md)

Learn about the fundamental building blocks of KSML applications:

- Understanding stream types (KStream, KTable, GlobalKTable)
- Choosing the right stream type
- Best practices
- Examples

### [Data Type Reference](data-type-reference.md)

Detailed information about all supported data types in KSML:

- Primitive data types
- Complex data types
- Schema management
- Key and value types
- Function parameter types
- Function result types
- Type conversion
- Serialization and deserialization
- Custom data types

### [Notation Reference](notation-reference.md)

Find out how to use different notations for Kafka topics:

- Key and value types
- Working with different data formats (Avro, JSON, CSV, etc.)
- Schema management
- Serialization and deserialization
- Introduction to notations
- How to configure notations
- List of available supported variations

### [Function Reference](function-reference.md)

Discover how to use Python functions in your KSML applications:

- Types of functions in KSML
- Writing Python functions
- Function parameters and return types
- Reusing functions across pipelines
- Function execution context and limitations
- Function types (forEach, mapper, predicate, etc.)
- Python code integration
- Built-in functions
- Best practices for function implementation

### [Operation Reference](operation-reference.md)

Learn about the various operations you can perform on your data:

- Stateless operations (map, filter, etc.)
- Stateful operations (aggregate, count, etc.)
- Windowing operations
- Joining streams and tables
- Sink operations
- Each operation includes:
  - Syntax and parameters
  - Return values
  - Examples
  - Common use cases
  - Performance considerations

### [Pipelines](pipelines.md)

Explore how data flows through KSML applications:

- Pipeline structure and components
- Input and output configurations
- Connecting pipelines
- Best practices for pipeline design
- Error handling in pipelines

### [Configuration Reference](configuration-reference.md)

Complete documentation of KSML configuration options:

- Runner configuration
- Kafka client configuration
- Schema Registry configuration
- Performance tuning options
- Security settings
- Logging configuration
- Environment variables

## How to Use This Reference

You can read through these reference topics in order for a comprehensive understanding, or jump to specific topics as needed:

1. Start with [KSML Language Reference](language-reference.md) and [Stream Type Reference](stream-type-reference.md) to understand the basic structure and data model
2. Move on to [Data Types](data-type-reference.md) and [Notations](notation-reference.md) to learn about data handling
3. Explore [Functions](function-reference.md) to see how to implement custom logic
4. Learn about [Operations](operation-reference.md) to understand all the ways you can process your data
5. Study [Pipelines](pipelines.md) to learn how data flows through your application
6. Review advanced tutorials for production-ready applications
7. Finish with [Configuration Reference](configuration-reference.md) for deployment settings