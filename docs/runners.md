[<< Back to index](index.md)

# Runners

### Table of Contents
1. [Introduction](#introduction)
2. [Generic configuration](#generic-configuration)
    * [Kafka backend](#kafka-backend)
    * [Axual backend](#axual-backend)
3. [Starting a container](#starting-a-container)

## Introduction

The core of KSML is a library that allows KSML Definition files to be parsed and translated into Kafka Streams topologies. Because we wanted to keep KSML low-overhead, KSML does not run these topologies itself. Two runners were created to execute the generated topologies:

|Type|Description
|:---|:---
|Kafka|Runs the topology against a Kafka backend, using plain Kafka configuration parameters.
|Axual|Runs the topology against an Axual backend, a multi-tenant Kafka platform by Axual, which requires slightly different configuration.

Examples of runner configurations are shown below.

## Generic configuration

The configuration file passed to the KSML runner is in YAML format and should contain at least the following:

```yaml
ksml:
  workingDirectory: <absolute path>
    definitions:
      - definition1.yaml
      - definition2.yaml
      - <more here...>

backend:
  type: <backend type>
  config:
    <backend specific configuration>
```

### Kafka backend

The default KSML runner uses a Kafka backend, which requires the following structure and parameters:

```yaml
backend:
  type: kafka
  config:
    bootstrapUrl: localhost:9092
    applicationId: io.ksml.example.processor
    schemaRegistryUrl: http://localhost:8083
```

### Axual backend

The Axual backend takes the following structure and parameters:

```yaml
backend:
  type: axual
  config:
    tenant: <tenant>
    environment: <environment>
    endpoint: <axual discovery endpoint url>
    applicationId: io.axual.ksml.example.processor
    applicationVersion: 0.0.1
    sslConfig:
      enableHostnameVerification: false
      keystoreLocation: <path to keystore.jks>
      keystorePassword: <password>
      keyPassword: <password>
      truststoreLocation: <path to truststore.jks>
      truststorePassword: <password>
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
