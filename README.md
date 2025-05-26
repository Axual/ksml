[![Build and test](https://github.com/axual/ksml/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/axual/ksml/actions/workflows/build-and-test.yml)

# Axual KSML – Low-Code Stream Processing on Kafka Streams

KSML lets you describe Kafka Streams topologies with concise YAML and embedded Python instead of large Java projects. 

KSML is a wrapper around Kafka Streams that allows the development of low-code stream processing applications. It was
developed by Axual in early 2021 and open-sourced in May 2021.

## Why KSML?

Kafka Streams is powerful but **Java-centric**. KSML removes the boiler-plate:

* **Declarative YAML** for topology wiring
* **Python snippets** for lightweight transformation logic
* **One command** to package & run (native image or JVM)

## Introduction

Kafka Streams has captured the hearts and minds of many developers that want to develop streaming applications on top of
Kafka. But as powerful as the framework is, Kafka Streams has had a hard time getting around the requirement of writing
Java code and setting up complex build pipelines. There were some attempts to rebuild Kafka Streams, but up until now popular
languages like Python did not receive equally powerful (and maintained) stream processing frameworks. KSML provides a
new declarative approach to unlock Kafka Streams to a wider audience. Using only a few simple basic rules and Python
snippets, you will be able to write streaming applications in very little time.

## Language

To quickly jump to the KSML specification, use this link: https://axual.github.io/ksml/

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
1. Using the provided multistage Dockerfile, which includes GraalVM and builds the project in a containerized environment.
2. Installing GraalVM locally and building the project with Maven.

Details for each method are outlined below.

#### Option 1: Using the multistage Docker build

You can build the KSML runner using Docker Buildx:

```shell
    # Create a Buildx builder named 'ksml'
    docker buildx create --name ksml

    # Build and load the KSML runner image
    docker buildx --builder ksml build --load \
    -t axual/ksml:local --target ksml -f Dockerfile .

    # Remove the builder when done
    docker buildx rm ksml
```
>💡 To build for multiple platforms (e.g. amd64 and arm64), add the --platform flag: \
> `--platform linux/amd64,linux/arm64`
> 
> This is useful for creating images that run on both Intel/AMD and ARM systems (e.g., servers and Raspberry Pi devices).
> Make sure your Docker setup supports this. You may need QEMU and additional configuration.

#### Option 2: Build locally with GraalVM

Download GraalVM for Java 21 or later from [the official downloads page](https://www.graalvm.org/downloads/) and follow the installation instructions for your platform.

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
2. For a locally running demo with an example KSML configuration file, please follow [Setting Up the Project for Local Development](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md). 

### Contributing ###

Axual is interested in building the community; we would welcome any thoughts or
[patches](https://github.com/Axual/ksml/issues).
You can reach us [here](https://axual.com/contact/).

See [Contributing](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).
