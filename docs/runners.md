[<< Back to index](index.md)

# Runners

### Table of Contents
1. [Introduction](#introduction)
2. [Generic configuration](#generic-configuration)
    * [Kafka backend](#kafka-backend)
    * [Axual backend](#axual-backend)
3. [Starting a container](#starting-a-container)

## Introduction

The core of KSML is a library that allows KSML definition files to be parsed and translated into Kafka Streams topologies.
To keep the core small, the logic to actually run the topology is kept separate. This allows for other programs to
include KSML parsing/interpreting as a library.
A standalone runner connects the topology to a standard Kafka Streams implementation and execute it.

## Generic configuration

The configuration file passed to the KSML runner is in YAML format. It contains two main sections.

* ksml: section to instruct which definitions files to run, and how to run them.
* kafka: the normal properties that define how to connect to Kafka.

An example of a runner configuration is shown below.

```yaml
ksml:
  configDirectory: <absolute/relative path>    # directory where ksml definitions are located
  schemaDirectory: <absolute/relative path>    # (optional) directory containing schema definitions, default is same as configDirectory
  storageDirectory: <absolute/relative path>   # (optional) storage directory for local state stores, default is same as configDirectory
  applicationServer:                           # (optional) default is to not run the application server
    enabled: true                             # true if you want to enable REST querying of state stores
    host: 0.0.0.0                             # by default listen on all interfaces
    port: 8080                                # port to listen on
  errorHandling:                               # (optional) how to handle errors
    consume:                                   # (optional) how to handle message consumption errors
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ConsumeError                 # logger name
      handler: continueOnFail                  # continue or stop on error
    process:                                   # (optional) how to handle message processing errors
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ProcessError                 # logger name
      handler: stopOnFail                      # continue or stop on error
    produce:                                   # (optional) how to handle message production errors
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ProduceError                 # logger name
      handler: continueOnFail                  # continue or stop on error
    definitions:                               # KSML definition files from the working directory
      - definition1.yaml
      - definition2.yaml
      - <more here...>

kafka:
  application.id: my.app.id
  bootstrap.servers: kafka:9093
  schema.registry.url: http://localhost:8083
  acks: all
  ...
```

## Starting a container
To start a container the KSML definitions and Runner configuration files need to be available in a directory mounted inside the docker container.

The default Runner configuration filename is **_ksml-runner.yaml_**.
If no arguments are given, the runner will look for this file in the home directory

```
## -w sets the current working directory in the container

docker run --rm -ti -v /path/to/local/ksml/directory:/ksml -w /ksml axual/ksml-axual:latest

## or

docker run --rm -ti -v /path/to/local/ksml/directory:/ksml -w /ksml axual/ksml-axual:latest /ksml/ksml-runner.yaml
```

or, if the runner configuration is in a different file, like **_my-runner.yaml_**.

```
docker run --rm -ti -v /path/to/local/ksml/directory:/ksml axual/ksml-axual:latest /ksml/my-runner.yaml
```
