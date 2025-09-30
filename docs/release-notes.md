# Release Notes

## Releases

* [Release Notes](#release-notes)
    * [Releases](#releases)
        * [1.1.0 (2025-09-18)](#110-2025-09-18)
        * [1.0.8 (2025-06-20)](#108-2025-06-20)
        * [1.0.7 (2025-06-09)](#107-2025-06-09)
        * [1.0.6 (2025-03-24)](#106-2025-03-24)
        * [1.0.5 (2025-01-14)](#105-2025-01-14)
        * [1.0.4 (2024-11-22)](#104-2024-11-22)
        * [1.0.3 (2024-10-18)](#103-2024-10-18)
        * [1.0.2 (2024-09-20)](#102-2024-09-20)
        * [1.0.1 (2024-07-17)](#101-2024-07-17)
        * [1.0.0 (2024-06-28)](#100-2024-06-28)
        * [0.8.0 (2024-03-08)](#080-2024-03-08)
        * [0.9.1 (2024-06-21)](#091-2024-06-21)
        * [0.9.0 (2024-06-05)](#090-2024-06-05)
        * [0.2.2 (2024-01-30)](#022-2024-01-30)
        * [0.2.1 (2023-12-20)](#021-2023-12-20)
        * [0.2.0 (2023-12-07)](#020-2023-12-07)
        * [0.1.0 (2023-03-15)](#010-2023-03-15)
        * [0.0.4 (2022-12-02)](#004-2022-12-02)
        * [0.0.3 (2021-07-30)](#003-2021-07-30)
        * [0.0.2 (2021-06-28)](#002-2021-06-28)
        * [0.0.1 (2021-04-30)](#001-2021-04-30)

## 1.1.0 (2025-09-18)

### BREAKING CHANGES

#### State Store Configuration ([#136](https://github.com/Axual/ksml/pull/136))
The `store` property in aggregate operations is now **required**.

**Migration Required:**
```yaml
# Before (1.0.8)
- type: aggregate
  initializer:
    expression: 0
    resultType: long
  aggregator:
    expression: aggregatedValue+1
    resultType: long

# After (1.1.0) - store is now required
- type: aggregate
  store:                    # REQUIRED
    type: window
    windowSize: 10m
    retention: 1h
  initializer:
    expression: 0
    resultType: long
  aggregator:
    expression: aggregatedValue+1
    resultType: long
```
See example: [09-example-aggregate.yaml](https://github.com/Axual/ksml/blob/main/examples/09-example-aggregate.yaml)

### KEY NEW FEATURES

#### JsonSchema Support
Full JsonSchema support added for better data validation and schema management. Follows JSON Schema specification where missing `additionalProperties` defaults to `true` (allowing additional fields).

**Tutorial:** [Working with JsonSchema Data](tutorials/beginner/data-formats.md#working-with-jsonschema-data)

#### Notation and Schema Registry Configuration
New configuration system for data serialization and schema registries. Configure different implementations for each notation type (Avro, Protobuf, JsonSchema) and schema registry connections.

**Tutorial:** [Working with JsonSchema Data](tutorials/beginner/data-formats.md#specifying-data-formats)
**Example:** [ksml-runner.yaml](https://github.com/Axual/ksml/blob/main/examples/ksml-runner.yaml)
```yaml
ksml:
  schemaRegistries:
    confluent:
      config:
        schema.registry.url: http://confluent-schema-registry:8081
    apicurio:
      config:
        apicurio.registry.url: http://apicurio-registry:8080

  notations:
    avro:
      type: confluent_avro
      schemaRegistry: confluent
    jsonschema:
      type: apicurio_json
      schemaRegistry: apicurio
    protobuf:
      type: apicurio_protobuf
      schemaRegistry: apicurio
```

#### Protobuf Support (BETA)
Added experimental Protocol Buffers support with both Confluent and Apicurio registry compatibility.

**Example:** [00-example-generate-sensordata-protobuf.yaml](https://github.com/Axual/ksml/blob/main/examples/00-example-generate-sensordata-protobuf.yaml)
```yaml
producers:
  sensordata_protobuf_producer:
    generator: generate_sensordata_message
    interval: 3s
    to:
      topic: ksml_sensordata_protobuf
      keyType: string
      valueType: protobuf:sensor_data  # New protobuf type
```

#### Enhanced Type System
- **List and Tuple Types** ([#285](https://github.com/Axual/ksml/pull/285))
- **Map Type** ([#269](https://github.com/Axual/ksml/pull/269))
- Multiple message generation support for producers

#### Kubernetes & Monitoring
- **Helm Charts** for production deployments ([#85](https://github.com/Axual/ksml/pull/85))
- **Kafka Streams Metrics Reporter** with KSML tag enrichment
- **Health Probes**: Separate liveness, readiness, and startup probes
- **NetworkPolicy** and **PrometheusRules** support

### IMPROVEMENTS

#### Syntax Enhancements ([#133](https://github.com/Axual/ksml/pull/133))
Enhanced "to" operation with clearer definitions. Both syntaxes continue to work:
```yaml
# Simple syntax (still supported)
to: my-topic

# Detailed syntax (recommended for clarity)
to:
  topic: my-topic
  keyType: string
  valueType: json
```

### BUG FIXES

* Fixed AVRO CharSequence crash with nested objects ([#163](https://github.com/Axual/ksml/pull/163))
* Resolved excessive CPU usage issue ([#157](https://github.com/Axual/ksml/pull/157))
* Fixed multiple join operation issues ([#136](https://github.com/Axual/ksml/pull/136), [#143](https://github.com/Axual/ksml/pull/143), [#225](https://github.com/Axual/ksml/pull/225))
* Storage and state management improvements
* Apicurio Registry 2.x compatibility

### DOCUMENTATION & EXAMPLES

#### Improved Documentation with Docker Compose Examples
Updated KSML documentation with:

- **Working examples** that run in Docker Compose
- **Step-by-step tutorials** from beginner to advanced
- **Real-world use cases** with complete implementations
- **Interactive testing** - all examples can be run locally

**Key Resources:**

- **[Getting Started](../getting-started/quick-start.md)** - Quick start guide with Docker Compose
- **[Tutorials](../tutorials/index.md)** - From basics to advanced patterns
- **[Examples](https://github.com/Axual/ksml/tree/main/examples)** - 20+ working examples
- **[Reference](../reference/index.md)** - Complete KSML language reference

### INFRASTRUCTURE UPDATES

* Upgraded to **Kafka 4.0.0** ([#151](https://github.com/Axual/ksml/pull/151))
* Java 23 Security Manager support
* Multi-architecture Docker builds (ARM64/AMD64)
* Configurable GraalVM security options

## 1.0.8 (2025-06-20)

KSML

* Axual header cleaning interceptor to KSML

Helm charts

* Run as Job
* Prometheus alert rules
* Network policies

## 1.0.7 (2025-06-09)

* Fix KSML crash when topic creation is needed and pattern configurations were provided

## 1.0.6 (2025-03-24)

* Fix storage issues, Persistent Volumes always created and never cleaned
* Fix slow and failing builds with multiple architectures

## 1.0.5 (2025-01-14)
* Fix store serde regression

## 1.0.4 (2024-11-22)

* Fix crash when using Avro CharSequence encodings and nested objects

## 1.0.3 (2024-10-18)

* Fix high CPU usage
* Upgrade to Avro 1.11.4 to fix CVE-2024-47561

## 1.0.2 (2024-09-20)

KSML

* Upgrade to Kafka Streams 3.8.0
* Avro Schema Registry settings no longer required if Avro not used 
* Add missing object in KSML Json Schema
* Fix serialisation and list handling issues

Helm charts

* Use liveness and readiness and startup probes to fix state issues
* Fix conflicting default configuration Prometheus export and ServiceMonitor

## 1.0.1 (2024-07-17)

* Topology Optimization can be applied
* Runtime dependencies, like LZ4 compression support, are back in the KSML image
* Fix parse error messages during join
* Fix windowed aggregation flow errors
* Update windowed object support in multiple operations and functions

## 1.0.0 (2024-06-28)

* Reworked parsing logic, allowing alternatives for operations and other definitions to co-exist in the KSML language
  specification. This allows for better syntax checking in IDEs.
* Lots of small fixes and completion modifications.

## 0.9.1 (2024-06-21)

* Fix failing test in GitHub Actions during release
* Unified build workflows

## 0.9.0 (2024-06-05)

* Collectable metrics
* New topology test suite
* Python context hardening
* Improved handling of Kafka tombstones
* Added flexibility to producers (single shot, n-shot, or user condition-based)
* JSON Logging support
* Bumped GraalVM to 23.1.2
* Bumped several dependency versions
* Several fixes and security updates

## 0.8.0 (2024-03-08)

* Reworked all parsing logic, to allow for exporting the JSON schema of the KSML specification:
    * docs/specification.md is now derived from internal parser logic, guaranteeing consistency and completeness.
    * examples/ksml.json contains the JSON schema, which can be loaded into IDEs for syntax validation and completion.
* Improved schema handling:
    * Better compatibility checking between schema fields.
* Improved support for state stores:
    * Update to state store typing and handling.
    * Manual state stores can be defined and referenced in pipelines.
    * Manual state stores are also available in Python functions.
    * State stores can be used 'side-effect-free' (e.g. no Avro schema registration)
* Python function improvements:
    * Automatic variable assignment for state stores.
    * Every Python function can use a Java Logger, integrating Python output with KSML log output.
    * Type inference in situations where parameters or result types can be derived from the context.
* Lots of small language updates:
    * Improve readability for store types, filter operations and windowing operations
    * Introduction of the "as" operation, which allows for pipeline referencing and chaining.
* Better data type handling:
    * Separation of data types and KSML core, allowing for easier addition of new data types in the future.
    * Automatic conversion of data types, removing common pipeline failure scenarios.
    * New implementation for CSV handling.
* Merged the different runners into a single runner.
    * KSML definitions can now include both producers (data generators) and pipelines (Kafka Streams topologies).
    * Removal of Kafka and Axual backend distinctions.
* Configuration file updates, allowing for running multiple definitions in a single runner (each in its own namespace).
* Examples updated to reflect the latest definition format.
* Documentation updated.

## 0.2.2 (2024-01-30)

* Fix KSML java process not stopping on exception
* Fix stream-stream join validation and align other checks
* Bump logback to 1.4.12
* Fix to enable Streams optimisations to be applied to topology
* Fix resolving admin client issues causing warning messages

## 0.2.1 (2023-12-20)

* Fixed an issue with Avro and field validations

## 0.2.0 (2023-12-07)

* Optimized Docker build
* Merged KSML Runners into one module, optimize Docker builds and workflow
* KSML documentation updates
* Docker image, GraalPy venv, install and GU commands fail
* Update GitHub Actions
* Small robustness improvements
* Issue #72 - Fix build failures when trying to use venv and install python packages
* Manual state store support, Kafka client cleanups and configuration changes
* Update and clean up dependencies
* Update documentation to use new runner configurations
* Update to GraalVM for JDK 21 Community

## 0.1.0 (2023-03-15)

* Added XML/SOAP support
* Added data generator
* Added Automatic Type Conversion
* Added Schema Support for XML, Avro, JSON, Schema
* Added Basic Error Handling

## 0.0.4 (2022-12-02)

* Update to Kafka 3.2.3
* Update to Java 17
* Support multiple architectures in KSML, linux/amd64 and linux/arm64
* Refactored internal typing system, plus some fixes to store operations
* Introduce queryable state stores
* Add better handling of NULL keys and values from Kafka
* Implement schema support
* Added Docker multistage build
* Bug fix for windowed objects
* Store improvements
* Support Liberica NIK
* Switch from Travis CI to GitHub workflow
* Build snapshot Docker image on pull request merged

## 0.0.3 (2021-07-30)

* Support for Python 3 through GraalVM
* improved data structuring
* bug fixes

## 0.0.2 (2021-06-28)

* Added JSON support, Named topology and name store supported

## 0.0.1 (2021-04-30)

* First alpha release 
