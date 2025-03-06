[![Build and test](https://github.com/axual/ksml/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/axual/ksml/actions/workflows/build-and-test.yml)

# Axual KSML

KSML is a wrapper around Kafka Streams that allows for development of low code stream processing applications. It was
developed by Axual early 2021 and released as open source in May 2021.

## Introduction

Kafka Streams has captured the hearts and minds of many developers that want to develop streaming applications on top of
Kafka. But as powerful as the framework is, Kafka Streams has had a hard time getting around the requirement of writing
Java code and setting up build pipelines. There were some attempts to rebuild Kafka Streams, but up until now popular
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
| [`ksml-data-binary`](ksml-data-avro/)       | extension to the data library for BINARY support.                                                            |
| [`ksml-data-csv`](ksml-data-csv/)           | extension to the data library for CSV support.                                                               |
| [`ksml-data-json`](ksml-data-avro/)         | extension to the data library for JSON support.                                                              |
| [`ksml-data-protobuf`](ksml-data-protobuf/) | extension to the data library for PROTOBUF support.                                                          |
| [`ksml-data-soap`](ksml-data-soap/)         | extension to the data library for SOAP support.                                                              |
| [`ksml-data-xml`](ksml-data-xml/)           | extension to the data library for XML support.                                                               |
| [`ksml`](ksml/)                             | the core component that parses KSML definitions and converts them to a Kafka Streams topology.               |
| [`ksml-kafka-clients`](ksml-kafka-clients/) | the set of Kafka clients for KSML, injected into Kafka Streams, allowing for namespaced Kafka installations. |
| [`ksml-query`](ksml-query/)                 | allows an active KSML application to be queries via REST for its internal state stores.                      |
| [`ksml-runner`](ksml-runner/)               | standalone Java application for running KSML definitions.                                                    |

## Building KSML

Building and running KSML requires an installation of GraalVM and the corresponding Python module.
There are two ways to do this:

1. Use the supplied multistage Docker build file
2. Install GraalVM locally

See the paragraphs below for details.

#### Using the multistage Docker build

You can build either the standard KSML runner, or the runner for the Axual platform using one of the following commands:

    # Create the BuildX builder for KSML 
    docker buildx create --name ksml
    # Build KSML Runner
    docker buildx --builder ksml build --load --platform linux/amd64,linux/arm64 -t axual/ksml:local --target ksml -f Dockerfile .
    # Remove the BuildX builder for KSML
    docker buildx rm ksml

If you get the following error it means that your setup cannot build for multiple platforms yet.

    ERROR: docker exporter does not currently support exporting manifest lists

You can perform a build for just your platform by removing the `--platform linux/amd64,linux/arm64` arguments from the
commands above

    # Create the BuildX builder for KSML 
    docker buildx create --name ksml
    # Build KSML Runner
    docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
    # Remove the BuildX builder for KSML
    docker buildx rm ksml

#### Install GraalVM locally

Download GraalVM for Java 21 or later from [this page](https://www.graalvm.org/downloads/) and install it for your
platform as explained.

Once installed, select GraalVM as your default Java JVM. Then you can build KSML using the normal
Maven commands:

```mvn clean package```

## Running KSML

To run the KSML demo, we provide a Docker compose file which will start Kafka, create the demo topics, and start a
container
with a demo producer. You can then start the runner you generated in the previous step, passing in a KSML configuration
of your choice.
See [Getting started](docs/quick-start) or [Runners](docs/runners.md) for details.

To run the demo, Docker 19.x is required.

### Contributing ###

Axual is interested in building the community; we would welcome any thoughts or
[patches](https://github.com/Axual/ksml/issues).
You can reach us [here](https://axual.com/contact/).

See [Contributing](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).
