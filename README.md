[![Build Status](https://app.travis-ci.com/tonvanbart/ksml.svg?branch=main)](https://app.travis-ci.com/tonvanbart/ksml)

Axual KSML
--------

KSML is a wrapper around Kafka Streams that allows for development of low code stream processing applications. It was developed by Axual early 2021 and released as open source in May 2021.

## Introduction
Kafka Streams has captured the hearts and minds of many developers that want to develop streaming applications on top of Kafka. But as powerful as the framework is, Kafka Streams has had a hard time getting around the requirement of writing Java code and setting up build pipelines. There were some attempts to rebuild Kafka Streams, but up until now popular languages like Python did not receive equally powerful (and maintained) stream processing frameworks. KSML provides a new declarative approach to unlock Kafka Streamsto a wider audience. Using only a few simple basic rules and Python snippets, you will be able to write streaming applications in very little time.

## Project Overview
The project is divided into modules based functionality in order to be included separately depending
on the use case.

The submodules are as follows:

* [`ksml`](ksml/) 
  the core component that can parse KSML definition files and convert them to a Kafka Streams topology

* [`ksml-axual`](ksml-axual/) 
  adaption of the KSML serde generator, intended for use with Axual Platform and/or Axual Cloud

* [`ksml-runner`](ksml-runner/) 
  standalone Java application, that combines both Kafka Streams and KSML in a runnable jar

* [`ksml-runner-axual`](ksml-runner-axual/) 
  adaption of the standard runner, intended for use with Axual Platform and/or Axual Cloud

The following example project is also included:
* [`ksml-example-producer`](ksml-example-producer/)
  example producer, which writes random SensorData messages to a Kafka topic for demo purposes

## Building KSML
Building and running KSML requires an installation of GraalVM and the corresponding Python module.
There are two ways to do this:
1. Use the supplied multistage Docker build file
2. Install GraalVM locally

See the paragraphs below for details.

#### Using the multistage Docker build
You can build either the standard KSML runner, or the runner for the Axual platform using one of the following commands:

    docker build -t axual/ksml -f Dockerfile-build-runner . --build-arg runner=ksml-runner
    docker build -t axual/ksml -f Dockerfile-build-runner . --build-arg runner=ksml-runner-axual

If you do not specify a `--build-arg`, the plain Kafka runner will be built by default.

#### Install GraalVM locally
Download GraalVM from [this page](https://www.graalvm.org/downloads/) and install it for your
platform as explained. Once installed, use the command ```gu install python``` to install the Python
module. For more information, check out the [Python Reference](https://www.graalvm.org/reference-manual/python/) pages.

Once installed, select GraalVM as your default Java JVM. Then you can build KSML using the normal
Maven commands:

```mvn clean package```

Then build the runtime container by passing in either `./ksml-runner` or `ksml-runner-axual` as build context:

    docker build -t axual/ksml -f Dockerfile ./ksml-runner
    docker build -t axual/ksml -f Dockerfile ./ksml-runner-axual

## Running KSML
To run the KSML demo, we provide a Docker compose file which will start Kafka, create the demo topics, and start a container
with a demo producer. You can then start the runner you generated in the previous step, passing in a KSML configuration of your choice.
See [Getting started](docs/getting-started.md) or [Runners](docs/runners.md) for details.

To run the demo, Docker 19.x is required.

### Contributing ###

Axual is interested in building the community; we would welcome any thoughts or 
[patches](https://github.com/Axual/ksml/issues).
You can reach us [here](https://axual.com/contact/).

See [Contributing](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).
