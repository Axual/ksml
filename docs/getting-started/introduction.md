# Introduction to KSML

## Background

Kafka Streams is at the heart of many organisations' event sourcing, analytical processing, real-time, batch or ML
workloads. It has a beautiful Java DSL that allows developers to focus on what their applications need to do, not how to
do it. But the Java requirement also holds people back. If you don’t know Java, does that mean you cannot use Kafka
Streams?

Enter KSML.

KSML is a wrapper language and interpreter around Kafka Streams that lets you express any topology in a YAML syntax.
Simply define your topology as a processing pipeline with a series of steps that your data passes through. Your custom
functions can be expressed inline in Python. KSML will read your definition and construct the topology dynamically via
the Kafka Streams DSL and run it in GraalVM.

This documentation takes you for a deep-dive into KSML. It covers the basic concepts, how to run, and of course,
contains lots of useful and near-real-world examples. After going through these docs, you will understand how easily and
quickly you can develop Kafka Streams applications without writing a single line of Java.

## What is KSML?

KSML is a declarative language that allows you to build powerful Kafka Streams applications using YAML and Python,
without writing Java code. KSML makes stream processing accessible to a wider audience by removing the need for Java
development skills and complex build pipelines.

With KSML, you can:

- Define streaming data pipelines using simple YAML syntax
- Process data using Python functions embedded directly in your KSML definitions
- Deploy applications without compiling Java code
- Rapidly prototype and iterate on streaming applications

## Why Use KSML?

### Simplified Development

KSML dramatically reduces the complexity of building Kafka Streams applications. Instead of writing hundreds of lines of
Java code, you can define your streaming logic in a concise YAML file with embedded Python functions.

### No Java Required

While Kafka Streams is a powerful Java library, KSML eliminates the need to write Java code. This opens up Kafka Streams
to data engineers, analysts, and other professionals who may not have Java expertise.

### Rapid Prototyping

KSML allows you to quickly prototype and test streaming applications. Changes can be made to your KSML definition and
deployed immediately, without a compile-package-deploy cycle.

### Full Access to Kafka Streams Capabilities

KSML provides access to the full power of Kafka Streams, including:

- Stateless operations (map, filter, etc.)
- Stateful operations (aggregate, count, etc.)
- Windowing operations
- Stream and table joins
- And more

## Key Concepts and Terminology

### Streams

In KSML, a stream represents a flow of data from or to a Kafka topic. Streams are defined with a name, a topic, and data
types for keys and values.

```yaml
{% include "definitions/introduction/example-stream-definition.yaml" %}
```

### Functions

Functions in KSML are reusable pieces of Python code that can be called from your pipelines. They can transform data,
filter messages, or perform side effects like logging.

```yaml
{% include "definitions/introduction/example-function-definition.yaml" %}
```

### Pipelines

Pipelines define the flow and processing of data. A pipeline starts with a source stream, applies a series of
operations, and typically ends with a sink operation that writes to another stream or performs a terminal action.

```yaml
{% include "definitions/introduction/example-pipeline-definition.yaml" %}
```

### Operations

Operations are the building blocks of pipelines. They define how data is processed as it flows through a pipeline. KSML
supports a wide range of operations, including:

- **Stateless operations**: filter, map, flatMap, peek
- **Stateful operations**: aggregate, count, reduce
- **Joining operations**: join, leftJoin, outerJoin
- **Windowing operations**: windowedBy
- **Sink operations**: to, forEach, branch

## How KSML Relates to Kafka Streams

KSML is built on top of Kafka Streams and translates your YAML definitions into Kafka Streams topologies. This means:

1. You get all the benefits of Kafka Streams (exactly-once processing, fault tolerance, scalability)
2. Your KSML applications can integrate with existing Kafka Streams applications
3. Performance characteristics are similar to native Kafka Streams applications

When you run a KSML definition, the KSML runner:

1. Parses your YAML definition
2. Translates it into a Kafka Streams topology
3. Executes the topology using the Kafka Streams runtime

This translation happens automatically, allowing you to focus on your business logic rather than the details of a normal
Kafka Streams implementation.

## Next Steps

Now that you understand what KSML is and its key concepts, you can:

- Learn how to [install and set up KSML](installation.md)
- Follow the [KSML Basics Tutorial](basics-tutorial.md) to build your first application
- Explore the [Core Concepts](../reference/stream-type-reference.md) in more detail
