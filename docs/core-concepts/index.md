# Core Concepts

Welcome to the KSML Core Concepts section! This section provides in-depth explanations of the fundamental components and concepts that make up KSML.

Understanding these core concepts will give you a solid foundation for building effective stream processing applications with KSML, regardless of the specific use case or complexity level.

## Key KSML Components

### [ Data Types ](../reference/data-types-reference.md)

Check out which data types KSML applications can handle. What you'll learn is:

- Primitive data types
- Complex data types
- Schema 
- Key and value types
- Function parameter types
- Function result types

### [Stream Types](../reference/stream-types-reference.md)

Learn about the fundamental building blocks of KSML applications:

- Understanding stream types (KStream, KTable, GlobalKTable)
- Choosing the right stream type
- Best practices
- Examples

### [Notations](../reference/notation-reference.md)

Find out how to use different notations for Kafka topics:

- Key and value types
- Working with different data formats (Avro, JSON, CSV, etc.)
- Schema management
- Serialization and deserialization

### [Functions](functions.md)

Discover how to use Python functions in your KSML applications:

- Types of functions in KSML
- Writing Python functions
- Function parameters and return types
- Reusing functions across pipelines
- Function execution context and limitations

### [Pipelines](pipelines.md)

Explore how data flows through KSML applications:

- Pipeline structure and components
- Input and output configurations
- Connecting pipelines
- Best practices for pipeline design
- Error handling in pipelines

### [Operations](operations.md)

Learn about the various operations you can perform on your data:

- Stateless operations (map, filter, etc.)
- Stateful operations (aggregate, count, etc.)
- Windowing operations
- Joining streams and tables
- Sink operations

## Conceptual Understanding

Beyond just the syntax and components, this section helps you understand:

- **The "why" behind KSML patterns**: Not just how to use features, but when and why to use them
- **Mental models for stream processing**: How to think about your data as streams and transformations
- **Design principles**: Guidelines for creating maintainable and efficient KSML applications
- **Performance considerations**: How different operations and patterns affect performance

## How to Use This Section

You can read through these core concepts in order for a comprehensive understanding, or jump to specific topics as needed:

1. Start with [Streams and Data Types](../reference/stream-types-reference.md) to understand the basic data model
2. Move on to [Pipelines](pipelines.md) to learn how data flows through your application
3. Explore [Functions](functions.md) to see how to implement custom logic
4. Finish with [Operations](operations.md) to learn about all the ways you can process your data

Each page includes conceptual explanations, code examples, and best practices to help you master KSML.

## Additional Resources

- [Getting Started: KSML Basics Tutorial](../getting-started/basics-tutorial.md) - A hands-on introduction to KSML
- [Reference Documentation](../reference/language-reference.md) - Complete technical reference
- [Examples Library](../resources/examples-library.md) - Ready-to-use examples demonstrating these concepts