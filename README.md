[![Build and test](https://github.com/axual/ksml/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/axual/ksml/actions/workflows/build-and-test.yml)

# KSML – Kafka Streams without Java

KSML is a wrapper language and interpreter around Kafka Streams
that lets you express any topology in a YAML syntax. Simply define
your topology as a processing pipeline with a series of steps that
your data passes through. Your custom functions can be expressed
inline in Python. KSML will read your definition and construct the
topology dynamically via the Kafka Streams DSL and run it in GraalVM.

KSML was started by Axual in early 2021 and open-sourced in May 2021.

## Why KSML?

Kafka Streams is powerful but **Java-centric**. KSML eliminates the Java boiler-plate through:

* **Declarative YAML** for topology wiring
* **User-defined functions in Python** for customized business logic
* **One command** to package and run (container image or in your own JVM)

## Language

To quickly jump to the KSML specification, use this link: https://axual.github.io/ksml/

## Examples

The following examples are provided in the `examples` directory:

| Filename                                                                                    | Description                                                                                      |
|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| [01-example-inspect.yaml](examples/01-example-inspect.yaml)                                 | Reads messages from a topic and output them on stdout                                            |
| [02-example-copy.yaml](examples/02-example-copy.yaml)                                       | Provides two ways to copy all messages from one topic to another topic                           |
| [03-example-filter.yaml](examples/03-example-filter.yaml)                                   | Reads messages from a topic, filters them and sends the results to an output topic               |
| [04-example-branch.yaml](examples/04-example-branch.yaml)                                   | Uses values in the message data to branch out into different handling sub-flows                  |
| [05-example-route.yaml](examples/05-example-route.yaml)                                     | Routes messages from one topic to several others based on message contents                       |
| [06-example-duplicate.yaml](examples/06-example-duplicate.yaml)                             | Reads messages from a topic, duplicates them in-memory and sends the results to an output topic  |
| [07-example-convert.yaml](examples/07-example-convert.yaml)                                 | Reads from an AVRO stream, and converts to JSON/XML before writing to the output topic           |
| [08-example-count.yaml](examples/08-example-count.yaml)                                     | Reads from an input topic, groups by key, applies windows and then counts owners per time window |
| [09-example-aggregate.yaml](examples/09-example-aggregate.yaml)                             | Same as `count` above, but performs steps manually through the `aggregate` operation             |
| [10-example-queryable-table.yaml](examples/10-example-queryable-table.yaml)                 | Shows how to use the queryable state store feature                                               |
| [11-example-field-modification.yaml](examples/11-example-field-modification.yaml)           | Modifies fields from an input message before sending to an output topic                          |
| [12-example-byte-manipulation.yaml](examples/12-example-byte-manipulation.yaml)             | Reads data from a binary input topic, modifies some bytes and writes to an output topic          |
| [13-example-join.yaml](examples/13-example-join.yaml)                                       | Joins a stream with a table and writes to an output topic                                        |
| [14-example-manual-state-store.yaml](examples/14-example-manual-state-store.yaml)           | Explains how to use a manual state store                                                         |
| [15-example-pipeline-linking.yaml](examples/15-example-pipeline-linking.yaml)               | Shows how the result of one pipeline can be used to start the next                               |
| [16-example-transform-metadata.yaml](examples/16-example-transform-metadata.yaml)           | Reads from an input topic, transforms headers and writes to an output topic                      |
| [17-example-inspect-with-metrics.yaml](examples/17-example-inspect-with-metrics.yaml)       | Metric-keeping version of first example above                                                    |
| [18-example-timestamp-extractor.yaml](examples/18-example-timestamp-extractor.yaml)         | Shows how to apply timestamps from message contents to the message itself                        |
| [19-example-performance-measurement.yaml](examples/19-example-performance-measurement.yaml) | Shows how to measure performance of your KSML app manually                                       |

## Project Overview

The project is divided into modules based functionality in order to be included separately depending
on the use case.

The submodules are as follows:

| Module                                      | Description                                                                                                  |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| [`ksml-data`](ksml-data/)                   | contains core data type and schema logic.                                                                    |
| [`ksml-data-avro`](ksml-data-avro/)         | extension to the data library for AVRO support.                                                              |
| [`ksml-data-binary`](ksml-data-binary/)     | extension to the data library for BINARY support.                                                            |
| [`ksml-data-csv`](ksml-data-csv/)           | extension to the data library for CSV support.                                                               |
| [`ksml-data-json`](ksml-data-json/)         | extension to the data library for JSON support.                                                              |
| [`ksml-data-protobuf`](ksml-data-protobuf/) | extension to the data library for PROTOBUF support.                                                          |
| [`ksml-data-soap`](ksml-data-soap/)         | extension to the data library for SOAP support.                                                              |
| [`ksml-data-xml`](ksml-data-xml/)           | extension to the data library for XML support.                                                               |
| [`ksml`](ksml/)                             | the core component that parses KSML definitions and converts them to a Kafka Streams topology.               |
| [`ksml-kafka-clients`](ksml-kafka-clients/) | the set of Kafka clients for KSML, injected into Kafka Streams, allowing for namespaced Kafka installations. |
| [`ksml-query`](ksml-query/)                 | allows an active KSML application to be queries via REST for its internal state stores.                      |
| [`ksml-runner`](ksml-runner/)               | standalone Java application for running KSML definitions.                                                    |

## Building KSML

KSML depends on GraalVM for compilation and runtime. There are two ways to build the KSML runner:

1. Using the provided multistage Dockerfile, which includes GraalVM and builds the project in a containerized
   environment.
2. Installing GraalVM locally and building the project with Maven.

Details for each method are outlined below.

#### Option 1: Using the multistage Docker build

You can build the KSML runner using `docker buildx`:

```shell
    # Create a Buildx builder named 'ksml'
    docker buildx create --name ksml

    # Build and load the KSML runner image
    docker buildx --builder ksml build --load \
    -t axual/ksml:local --target ksml -f Dockerfile .

    # Remove the builder when done
    docker buildx rm ksml
```

> 💡 To build for multiple platforms (e.g. amd64 and arm64), add the --platform flag: \
> `--platform linux/amd64,linux/arm64`
>
> This is useful for creating images that run on both Intel/AMD and ARM systems (e.g., servers and Raspberry Pi
> devices).
> Make sure your Docker setup supports this. You may need QEMU and additional configuration.

#### Option 2: Build locally with GraalVM

Download GraalVM for Java 21 or later from [the official downloads page](https://www.graalvm.org/downloads/) and follow
the installation instructions for your platform.

Once installed, configure GraalVM as your default JVM, then build the project using Maven:

```mvn clean package```

## Running KSML

Requirements:

- Docker Engine v20.10.x
- Docker Compose Plugin v2.17.x

To run the KSML demo locally, a [Docker Compose file](./docker-compose.yml) is provided. It sets up all required
components, including Kafka, the demo topics, and a demo producer container. To launch the demo:

```shell
docker compose up -d
```

Once the environment is up, you can run your KSML topology using the KSML runner and a KSML
configuration file of your choice:

1. For full details about the KSML configuration file, see the [KSML Runner documentation](docs/runners.md).
2. For a locally running demo with an example KSML configuration file, please
   follow [Setting Up the Project for Local Development](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).

### Contributing ###

Axual is interested in building the community; we would welcome any thoughts or
[patches](https://github.com/Axual/ksml/issues).
You can reach us [here](https://axual.com/contact/).

See [Contributing](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).
