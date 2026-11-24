# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KSML is a Java-based wrapper around Apache Kafka Streams that allows users to define stream processing topologies using YAML configuration files with inline Python functions. It eliminates the need to write Java code for stream processing applications.

## Development Commands

### Building
```bash
# Clean build and package all modules
mvn clean package

# Install to local Maven repository
mvn clean install

# Build Docker image locally
docker buildx create --name ksml
docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
docker buildx rm ksml
```

### Testing
```bash
# Run all unit tests
mvn clean test

# Run tests with coverage report
mvn clean test jacoco:report

# Run integration tests
mvn clean verify

# Run a single test class
mvn test -Dtest=ClassName

# Run a single test method
mvn test -Dtest=ClassName#methodName
```

### Running Locally
```bash
# Start local development environment (Kafka, Schema Registry, etc.)
docker compose up -d

# Run KSML with example definitions
./examples/run-local.sh

# Run specific KSML definition
java io.axual.ksml.runner.KSMLRunner workspace/local/ksml-runner-local.yaml
```

### Code Quality
```bash
# Check license headers
mvn license:check-file-header

# Update license headers
mvn license:update-file-header

# Check dependencies for updates
mvn versions:display-dependency-updates
```

## Architecture Overview

### Module Structure
The project follows a multi-module Maven structure:

- **ksml** - Core module that parses KSML YAML definitions and builds Kafka Streams topologies
- **ksml-data** - Core data type and schema logic
- **ksml-data-*** - Data format modules (avro, json, xml, csv, protobuf, binary, soap)
- **ksml-kafka-clients** - Custom Kafka clients with namespace support
- **ksml-runner** - Standalone application to run KSML definitions
- **ksml-query** - REST API for querying Kafka Streams state stores
- **ksml-reporting** - Reporting and metrics functionality

### Key Design Patterns
1. **Definition Parser**: The core module uses a hierarchical parser to convert YAML definitions into Kafka Streams topologies. Start with `io.axual.ksml.parser.TopologyParser` to understand the parsing flow.
2. **Function Execution**: Python functions in YAML are executed using GraalVM's polyglot capabilities. See `io.axual.ksml.python` package for implementation.
3. **Data Type System**: Extensible data type system with support for multiple formats. Each data format has its own module implementing common interfaces from `ksml-data`.
4. **Operation Types**: Stream processing operations (filter, map, join, aggregate, etc.) are defined in `io.axual.ksml.operation` with corresponding definitions in `io.axual.ksml.definition`.

## KSML Language Syntax
KSML uses YAML files to define stream processing topologies. The syntax is defined in `docs/ksml-language-spec.json` as a JSON Schema.

### Top-Level Structure
```yaml
# Optional metadata
name: topology-name
version: 1.0.0

# Data sources
streams:
  stream_name:
    topic: kafka-topic-name
    keyType: string
    valueType: avro:RecordType
    offsetResetPolicy: latest

tables:
  table_name:
    topic: kafka-topic-name
    keyType: string
    valueType: json

globalTables:
  global_table_name:
    topic: kafka-topic-name
    keyType: string
    valueType: xml

# Processing pipelines
pipelines:
  pipeline_name:
    from: source_name  # or inline topic definition
    via:              # list of operations
      - type: filter
        if:
          expression: value["field"] > 10
      - type: transformValue
        mapper:
          expression: value["field"] * 2
    to: output_topic  # or inline topic definition

# Data producers
producers:
  producer_name:
    to: topic_name
    code: |
      # Python code to generate records

# Reusable functions
functions:
  function_name:
    type: predicate
    expression: value is not None

# State stores
stores:
  store_name:
    type: keyValue
    keyType: string
    valueType: long
```

### Operation Types
All operations are specified in the `via` array with a `type` field:

- **Stream Operations**: filter, filterNot, transformKey, transformValue, transformKeyValue, peek, forEach
- **Aggregation Operations**: groupBy, groupByKey, windowByTime, windowBySession, aggregate, count, reduce
- **Join Operations**: join, leftJoin, outerJoin (with stream/table/globalTable)
- **Conversion Operations**: convertKey, convertValue, convertKeyValue, toStream, toTable
- **Other Operations**: branch, merge, repartition, suppress, cogroup

### Function Types
Functions can be defined inline or referenced by name:
- **predicate**: Returns boolean for filtering
- **keyValueMapper**: Maps to new key
- **valueTransformer**: Transforms values with state store access
- **aggregator**: Combines values in aggregations
- **initializer**: Provides initial aggregation value
- **merger**: Merges two aggregation results
- **forEach**: Side-effect operations

### Python Function Structure
```yaml
functions:
  # Inline function with expression
  to_upper_case:
    type: valueTransformer
    expression: value.upper()
    resultType: string

  # Inline function with code
  times_two_multiplier:
    type: keyValueTransformer
    code: |
      return (key, value * 2)
    resultType: (string, long)

  # With global code and parameters
  flexible_multiplier:
    type: keyValueTransformer
    globalCode: |
      multiplier = 2
    code: |
      return (key, value*multiplier)
    resultType: (string, long)
```

### Testing Approach
- Unit tests for individual components in each module
- Integration tests for full pipeline execution in `ksml/src/test/java`
- Test resources include sample YAML pipelines and expected outputs
- Tests use embedded Kafka for integration testing

## Important Considerations
1. **Java Version**: Requires GraalVM Java 21 or later for Python execution support
2. **Schema Registry**: Supports both Apicurio and Confluent schema registries
3. **State Stores**: Managed through KSML definitions, queryable via REST API
4. **Error Handling**: Exceptions in Python functions are propagated to Kafka Streams error handlers
5. **Performance**: Python execution adds overhead; use for business logic, not high-frequency operations
6. **Data Types**: Supports string, boolean, byte, short, int, long, float, double, bytes, and format-specific types (avro:, json:, xml:, csv:, protobuf:, binary:, soap:)

## Common Development Tasks
When implementing new features:
1. Data format support goes in a new `ksml-data-*` module
2. New operation types require definition class in `ksml.definition` and operation class in `ksml.operation`
3. Parser logic updates go in `ksml.parser` package
4. Always include unit tests and integration tests for new functionality
5. Update documentation in `docs/` directory when adding user-facing features
6. KSML syntax changes require updating the JSON schema in `docs/ksml-language-spec.json`

## Python Proxies
- Python runs with `HostAccess.EXPLICIT`. Every Java proxy class that Python code uses must be listed in `ALLOWED_JAVA_CLASSES` in `io.axual.ksml.python.PythonContext`, and every method Python may call must be annotated with `@HostAccess.Export`. Methods without the annotation stay invisible to Python, which is how proxies hide the parts of the wrapped Kafka object that are not safe to expose.
- GraalPy does not pass Python keyword arguments to Java methods, and Python looks up special methods such as `__enter__` on the type, which a Java object can not offer. When Python code needs either, add a small Python class that calls the exported methods by position. These classes live in the `ksml` Python module (`ksml/src/main/resources/io/axual/ksml/python/ksml.py`), which KSML loads into every Python context; name the class in the proxy's `InjectedParameter` (see `ProcessorContextProxy.INJECT_CONTEXT`), or wrap the proxy from Java (see `StoreIteratorProxy.toPython`). The classes must hold no state besides the proxy, because one Python context serves all stream threads.
- `log` and `metrics` are Python builtins, set up by the `ksml` module. `log` finds the logger of the running KSML function through a context variable that each generated `_caller` sets and restores, so it works in helper functions and modules too.
- Objects are made available to Python functions through the injections map of `UserFunction.call` (`stores`, `context`), not as function parameters. A function only accepts the variables it declares: `stores` when it uses state stores, and the `InjectedParameter`s of its definition. `PythonFunction` imports an injected parameter's Python class from the `ksml` module and wraps the injected object in it.

## Line Endings
- All files use LF (`\n`) line endings, without exception. Write new files with LF only, and never introduce CRLF.
- If a CRLF file shows up anyway (`git ls-files --eol | grep -E 'i/crlf|w/crlf|mixed'` lists it), convert it to LF in a separate commit, never mixed with functional changes, so the functional diff stays readable.
- After editing files, check with `git diff --stat` that only the edited lines changed; a diff that rewrites a whole file usually means its line endings changed.

## Interaction
Claude should use output-style concise in conversations.

## Code style
All text files use LF line endings, on every platform
