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
Download GraalVM from [this page](https://www.graalvm.org/downloads/) and install it for your
platform as explained. Once installed, use the command ```gu install python``` to install the Python
module. For more information, check out the [Python Reference](https://www.graalvm.org/reference-manual/python/) pages.

Once installed, select GraalVM as your default Java JVM. Then you can build KSML using the normal
Maven commands:

```mvn clean package```

## Running KSML
There are several ways to use KSML:
* As a Java library: you can include the KSML library in your own application to convert KSML definition files into a Kafka Streams topology.
* As a runnable jar: bundled in the repository is a [KSML runner](docs/runners.md) that allow you to run any KSML definition files from the command line against a Kafka and/or Axual backend.
* As a Docker container: same as the previous option, but the runnable jar is contained in a Docker image which executes as a standalone executable.

For the first and second options you need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) and [Maven](https://maven.apache.org/download.cgi) installed.

If you want to run KSML as a Docker container, Docker 19.x is required.

For more information, see the [Runners](docs/runners.md) page.

## Examples
See [Getting started](docs/getting-started.md) to run a demonstration setup.

### Contributing ###

Axual is interested in building the community; we would welcome any thoughts or 
[patches](https://github.com/Axual/ksml/issues).
You can reach us [here](https://axual.com/contact/).

See [Contributing](https://github.com/Axual/ksml/blob/main/CONTRIBUTING.md).
