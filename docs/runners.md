[<< Back to index](index.md)

# Runners

### Table of Contents
1. [Introduction](#introduction)
2. [Configuration](#configuration)
   - [Namespace support](#using-with-axual-platform-or-other-namespaced-kafka-clusters)
3. [Starting a container](#starting-a-container)

## Introduction

The core of KSML is a library that allows KSML Definition files to be parsed and translated into Kafka Streams topologies. Because we wanted to keep KSML low-overhead, KSML does not run these topologies itself. A runner was created to execute the generated topologies.
This runner supports plain Kafka connections, and contains an advanced configurations that helps running against Kafka clusters using namespacing. 

Examples of runner configurations are shown below.

## Configuration

The configuration file passed to the KSML runner is in YAML format and should contain at least the following:

```yaml
ksml:
  applicationServer:                           # The application server is currently only offering REST querying of state stores
    enabled: true                              # true if you want to enable REST querying of state stores
    host: 0.0.0.0                              # by default listen on all interfaces
    port: 8080                                 # port to listen on
  configDirectory: /ksml/config                # Location of the KSML definitions. Default is the current working directory
  schemaDirectory: /ksml/schemas               # Location of the schema definitions. Default is the config directory
  storageDirectory: /ksml/data                 # Where the stateful data is written. Defaults is the default JVM temp directory
  errorHandling:                               # how to handle errors
    consume:
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ConsumeError                 # logger name
      handler: continueOnFail                  # continue or stop on error
    process:
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ProcessError                 # logger name
      handler: stopOnFail                      # continue or stop on error
    produce:
      log: true                                # log errors
      logPayload: true                         # log message payloads upon error
      loggerName: ProduceError                 # logger name
      handler: continueOnFail                  # continue or stop on error
    definitions:                               # KSML definition files from the working directory
      - definition1.yaml
      - definition2.yaml
      - <more here...>

kafka: # Kafka streams configuration options 
  application.id: io.ksml.example.processor
  
  bootstrap.servers: broker-1:9092,broker-2:9092
  security.protocol: SSL
  ssl.protocol: TLSv1.3
  ssl.enabled.protocols: TLSv1.3,TLSv1.2
  ssl.endpoint.identification.algorithm: ""
  ssl.truststore.location: /ksml/config/truststore.jks
  ssl.truststore.password: password-for-truststore
  
  # Schema Registry client configuration, needed when schema registry is used
  schema.registry.url: http://schema-registry:8083
  schema.registry.ssl.truststore.location: /ksml/config/truststore.jks
  schema.registry.ssl.truststore.password: password-for-truststore
```

### Using with Axual platform or other namespaced Kafka clusters

A special mode for connecting to clusters that use namespaced Kafka resources is available. This mode
can be activated by specifying the namespace pattern to use. This pattern will be resolved to a complete
name by KSML using the provided configuration options.

The following config will resolve the backing topic of a stream or table

```yaml
kafka:
  # The patterns for topics, groups and transactional ids.
  # Each field between the curly braces must be specified in the configuration, except the topic,
  # group.id and transactional.id fields, which is used to identify the place where the resource name
  # is used
  topic.pattern: "{tenant}-{instance}-{environment}-{topic}"                       
  group.id.pattern: "{tenant}-{instance}-{environment}-{group.id}"
  transactional.id.pattern: "{tenant}-{instance}-{environment}-{transactional.id}"

  # Additional configuration options used for resolving the pattern to values
  tenant: "ksmldemo"
  instance: "dta"
  environment: "dev"
```


## Starting a container
To start a container the KSML definitions and Runner configuration files need to be available in a directory mounted inside the docker container.

The default Runner configuration filename is **_ksml-runner.yaml_**.
If no arguments are given, the runner will look for this file in the home directory

```
## -w sets the current working directory in the container

docker run --rm -ti -v /path/to/local/ksml/directory:/ksml -w /ksml axual/ksml-axual:snapshot

## or

docker run --rm -ti -v /path/to/local/ksml/directory:/ksml -w /ksml axual/ksml-axual:snapshot /ksml/ksml-runner.yaml
```

or, if the runner configuration is in a different file, like **_my-runner.yaml_**.

```
docker run --rm -ti -v /path/to/local/ksml/directory:/ksml axual/ksml-axual:latest /ksml/my-runner.yaml
```
