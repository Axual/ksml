# ksml

[KSML](https://ksml.io) - Kafka Streaming for Low Code Environments

* [Introduction](#introduction)
* [Prerequisites](#prerequisites)
* [Installing the Chart](#installing-the-chart)
* [Uninstalling the Chart](#uninstalling-the-chart)
* [Deployment modes](#deployment-modes)
  * [StatefulSet deployments for pipelines](#statefulset-deployment-for-pipeline-processing)
  * [Job deployments for data generators](#job-deployment-for-data-generators)
* [Configuration](#configuration)
* [Examples](#examples)
  * [Plain Kafka SASL Connection](#plain-kafka-sasl-connection)
  * [Axual Kafka SASL Connection](#axual-kafka-sasl-connection)
  * [Run as a Kubernetes Job](#example-run-ksml-as-a-kubernetes-job)

## Introduction

This chart deploys an easy way to define, run and deploy KSML applications on
a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes v1.21+

## Installing the Chart

To install/upgrade the chart with the release name `ksml`:

```bash
$ helm upgrade -i ksml oci://registry.axual.io/opensource/charts/ksml -n streaming --create-namespace --version=| 0.0.0-snapshot
```

The command deploys An easy way to define, run and deploy Kafka streaming applications on the
Kubernetes cluster in the default configuration. The [configuration](#configuration) section lists
the parameters that can be configured during installation.

> **Tip**: List all releases using `helm list`

## Uninstalling the Chart

To uninstall the `ksml`:

```bash
$ helm uninstall ksml -n streaming
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Deployment Modes

KSML offers both data generation and pipeline processing logic. To better support these use cases,
Deployment modes have been introduced, controlled by the `deploymentMode` configuration value

### StatefulSet deployment for pipeline processing

By default a KSML application deployed with this chart will run as a Kubernetes `StatefulSet`. 
This enables predictable horizontal scaling and identities for the replicas.

A `StatefulSet` deployment will always try to meet the number of running replicas, and will restart if the 
KSML application completed successfully.

### Job deployment for data generators

The `Job` deployment mode will start the KSML application as a Kubernetes `Job`.

The created KSML job will run once, and won't restart on failure or successful completion. This is very
useful for data generators which are defined with an end clause. This allows application owners to
check the deployment data and logs even after completion. 

> **Warning**: The `replicas` and `volumeClaimTemplates` configuration options are ignored for Job
> deployments.

> **Warning**: Job deployments cannot be updated and must be removed and recreated.


## Configuration

The following table lists the configurable parameters of the `ksml` chart and their default values.

| Parameter                                          | Description                                                                                                                                                                                                                         | Default                                                                                        |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| replicaCount                                       |                                                                                                                                                                                                                                     | <code>1</code>                                                                                 |
| deploymentMode                                     | Determing if the KSML Application runs as a StatefulSet or a Job. Valid values are <code>StatefulSet</code> and <code>Job</code>                                                                                                    | <code>StatefulSet</code>                                                                       |
| image.repository                                   | Registry to pull the image from and the name of the image.                                                                                                                                                                          | <code>docker.io/axual/ksml</code>                                                              |
| image.pullPolicy                                   | One of `Always`, `IfNotPresent`, or `Never`.                                                                                                                                                                                        | <code>IfNotPresent</code>                                                                      |
| image.tag                                          | Override the image tag whose default is the chart `appVersion`.                                                                                                                                                                     | <code>""</code>                                                                                |
| job.ttlSecondsAfterFinished                        | Specify how many seconds to wait before cleaning up a completed job pod. Only used when `deploymentMode` is set to `Job`                                                                                                            | <code>259200</code>                                                                            |
| job.finalizers                                     | Specify finalizers for the job resource. Only used when `deploymentMode` is set to `Job`                                                                                                                                            | <code>[]</code>                                                                                |
| job.pod.finalizers                                 | Specify finalizers for the pod created for the job. Only used when `deploymentMode` is set to `Job`                                                                                                                                 | <code>[]</code>                                                                                |
| nameOverride                                       | Override the app name generated by the chart.                                                                                                                                                                                       | <code>""</code>                                                                                |
| imagePullSecrets                                   | Override the list of ImagePullSecrets provided                                                                                                                                                                                      | <code>[]</code>                                                                                |
| fullnameOverride                                   | Override the fully qualified app name generated by the chart.                                                                                                                                                                       | <code>""</code>                                                                                |
| applicationServer.enabled                          | Start the application server for the component                                                                                                                                                                                      | <code>false</code>                                                                             |
| applicationServer.port                             |                                                                                                                                                                                                                                     | <code>"8080"</code>                                                                            |
| ksmlDefinitions                                    | Use this dictionary to specify one or more KSML definitions and filenames                                                                                                                                                           |                                                                                                |
| ksmlRunnerConfig.definitionDirectory               | Directory containing KSML Definition files. This should only be changed if an external volume mount is used.                                                                                                                        | <code>'/ksml'</code>                                                                           |
| ksmlRunnerConfig.definitions                       | A dictionary specifying a namespace, and the KSML definition file that should be executed in that namespace                                                                                                                         | <code>'/ksml'</code>                                                                           |
| ksmlRunnerConfig.schemaDirectory                   | Directory containing the Schema files. This should only be changed if an external volume mount is used.                                                                                                                             | <code>'/ksml'</code>                                                                           |
| ksmlRunnerConfig.errorHandling.consume.log         | log a message when consume errors occur                                                                                                                                                                                             | <code>true</code>                                                                              |
| ksmlRunnerConfig.errorHandling.consume.logPayload  | Add the record payload to the log message                                                                                                                                                                                           | <code>false</code>                                                                             |
| ksmlRunnerConfig.errorHandling.consume.loggerName  | Specify the name of the logger to use                                                                                                                                                                                               | <code>ConsumeError</code>                                                                      |
| ksmlRunnerConfig.errorHandling.consume.stopOnError | Should the KSML application stop when a consume error occurs                                                                                                                                                                        | <code>true</code>                                                                              |
| ksmlRunnerConfig.errorHandling.produce.log         | log a message when produce errors occur                                                                                                                                                                                             | <code>true</code>                                                                              |
| ksmlRunnerConfig.errorHandling.produce.logPayload  | Add the record payload to the log message                                                                                                                                                                                           | <code>false</code>                                                                             |
| ksmlRunnerConfig.errorHandling.produce.loggerName  | Specify the name of the logger to use                                                                                                                                                                                               | <code>ProduceErrpr</code>                                                                      |
| ksmlRunnerConfig.errorHandling.produce.stopOnError | Should the KSML application stop when a consume error occurs                                                                                                                                                                        | <code>true</code>                                                                              |
| ksmlRunnerConfig.errorHandling.process.log         | log a message when processing errors occur                                                                                                                                                                                          | <code>true</code>                                                                              |
| ksmlRunnerConfig.errorHandling.process.logPayload  | Add the record payload to the log message                                                                                                                                                                                           | <code>false</code>                                                                             |
| ksmlRunnerConfig.errorHandling.process.loggerName  | Specify the name of the logger to use                                                                                                                                                                                               | <code>ProcessError</code>                                                                      |
| ksmlRunnerConfig.errorHandling.process.stopOnError | Should the KSML application stop when a consume error occurs                                                                                                                                                                        | <code>true</code>                                                                              |
| ksmlRunnerConfig.producersEnabled                  | Data generation can be disabled for deployments to prevent accidental test data in non test environments                                                                                                                            | <code>true</code>                                                                              |
| ksmlRunnerConfig.pipelinesEnabled                  | Pipelines can be disabled allow for data generation only deployments.                                                                                                                                                               | <code>true</code>                                                                              |
| ksmlRunnerConfig.kafka                             | Dictionary of Kafka connection properties and application id                                                                                                                                                                        | <code></code>                                                                                  |
| ksmlRunnerConfig.kafka.application.id              | The application id for the app. Also used in resources and metrics                                                                                                                                                                  | <code></code>                                                                                  |
| ksmlRunnerConfig.kafka.bootstrap.servers           | The hostname and port number for connecting to the Kafka cluster                                                                                                                                                                    | <code></code>                                                                                  |
| schemaDefinitions                                  | Use this dictionary to specify one or more schema definitions and filenames                                                                                                                                                         |                                                                                                |
| serviceAccount.create                              | Specifies whether a service account should be created                                                                                                                                                                               | <code>true</code>                                                                              |
| serviceAccount.automount                           | Automatically mount a ServiceAccount's API credentials?                                                                                                                                                                             | <code>true</code>                                                                              |
| serviceAccount.name                                | The name of the service account to use. If not set and create is true, a name is generated using the fullname template                                                                                                              | <code>""</code>                                                                                |
| service.type                                       |                                                                                                                                                                                                                                     | <code>ClusterIP</code>                                                                         |
| service.port                                       |                                                                                                                                                                                                                                     | <code>80</code>                                                                                |
| ingress.enabled                                    | Enable creation of the Ingress resource to expose this service.                                                                                                                                                                     | <code>false</code>                                                                             |
| ingress.className                                  | The name of the IngressClass cluster resource. The associated IngressClass defines which controller will implement the resource.                                                                                                    | <code>""</code>                                                                                |
| ingress.hosts                                      | The Ingress Host definitions                                                                                                                                                                                                        |                                                                                                |
| ingress.tls                                        | The Ingress TLS configuration                                                                                                                                                                                                       |                                                                                                |
| applicationServer.enabled                          | Start an application server for the component                                                                                                                                                                                       | <code>false</code>                                                                             |
| applicationServer.port                             | Port on which Application server should be initialized                                                                                                                                                                              | <code>"8080"</code>                                                                            |
| logging.configFile                                 | Location of the logging configuration file                                                                                                                                                                                          | <code>'/ksml-logging/logback.xml'</code>                                                       |
| logging.jsonEnabled                                | Enable JSON logging                                                                                                                                                                                                                 | <code>false</code>                                                                             |
| logging.patterns.stdout                            | Pattern for the log message to standard out. See the [Logback](https://logback.qos.ch/manual/layouts.html#conversionWord) documentation for the syntax                                                                              | <code>'%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} [%t] %-5level %logger{36} - %msg%n'</code> |
| logging.patterns.stderr                            | Pattern for the log message to standard error. See the [Logback](https://logback.qos.ch/manual/layouts.html#conversionWord) documentation for the syntax                                                                            | <code>'%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} [%t] %-5level %logger{36} - %msg%n'</code> |
| logging.rootLogLevel                               | The default log level to use for all loggers                                                                                                                                                                                        | <code>INFO</code>                                                                              |
| logging.loggers                                    | A map of the logger name and corresponding loglevel                                                                                                                                                                                 |                                                                                                |
| store.spec                                         | Specify the VolumeClaimTemplate used by the StatefulSet. Default it uses access mode "ReadWriteOnce" and requests 1Gi                                                                                                               |                                                                                                |
| prometheus.enabled                                 | Enable Prometheus Metrics export                                                                                                                                                                                                    | <code>false</code>                                                                             |
| prometheus.port                                    | Port on which Prometheus metrics endpoint should be initialized                                                                                                                                                                     | <code>"9999"</code>                                                                            |
| prometheus.config                                  | The configuration for the [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter/blob/1.0.1/README.md). The 1.x.x config is used                                                                                      |                                                                                                |
| serviceMonitor.enabled                             | Enables creation of Prometheus Operator [ServiceMonitor](https://prometheus-operator.dev/docs/operator/api/#monitoring.coreos.com/v1.ServiceMonitor). Ignored if API `monitoring.coreos.com/v1` is not available.                   | <code>false</code>                                                                             |
| serviceMonitor.interval                            | Interval at which metrics should be scraped.                                                                                                                                                                                        | <code>30s</code>                                                                               |
| serviceMonitor.scrapeTimeout                       | Timeout after which the scrape is ended.                                                                                                                                                                                            | <code>10s</code>                                                                               |
| serviceMonitor.labels                              | A dictionary with additional labels for the service monitor                                                                                                                                                                         |                                                                                                |
| affinity                                           | The pod's scheduling constraints. See the Kubernetes documentation on [Affinity and Anti-affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity)                             |                                                                                                |
| topologySpreadConstraints                          | Describes how a group of pods ought to spread across topology domains. See the Kubernetes documentation on [Pod Topology Spread Constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/). | <code>[]</code>                                                                                |
| tolerations                                        | The tolerations on this pod. See the Kubernetes documentation on [Taints and Tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/).                                                           | <code>[]</code>                                                                                |
| nodeSelector                                       | Selector which must match a node's labels for the pod to be scheduled on that node.                                                                                                                                                 |                                                                                                |
| volumeMounts                                       | Additional volumeMounts on the output Deployment definition                                                                                                                                                                         | <code>[]</code>                                                                                |
| volumes                                            | Additional volumes on the output Deployment definition                                                                                                                                                                              | <code>[]</code>                                                                                |
| resources                                          | Resource requests and limits for KSML.                                                                                                                                                                                              |                                                                                                |
| securityContext                                    | Defines the security options the container should be run with. If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext                                                                           |                                                                                                |
| podSecurityContext                                 | Pod-level security attributes and common container settings                                                                                                                                                                         |                                                                                                |
| podLabels                                          | Extra labels to add to the Pods                                                                                                                                                                                                     |                                                                                                |
| podAnnotations                                     | Extra annotations to add to the Pods                                                                                                                                                                                                |                                                                                                |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm upgrade -i`. For
example:

```bash
$ helm upgrade -i ksml oci://registry.axual.io/opensource/charts/ksml -n streaming --create-namespace --version=| 0.0.0-snapshot --set replicaCount=1
```

Alternatively, a YAML file that specifies the values for the parameters can be provided while
installing the chart. For example:

```bash
$ helm upgrade -i ksml oci://registry.axual.io/opensource/charts/ksml -n streaming --create-namespace --version=| 0.0.0-snapshot --values values.yaml
```
## Examples

### Plain Kafka SASL Connection
<details>
  <summary>Example configuration plain Kafka SASL </summary>

```YAML
image:
  tag: snapshot

replicaCount: 3

resources:
  requests:
    memory: "128Mi"
    cpu: "250m"
  limits:
    memory: "768Mi"
    cpu: "500m"

ksmlRunnerConfig:
  definitions:
    generate: generator.yaml
  kafka:
    application.id: example.datagen

    bootstrap.servers: 'testing-sasl-broker:9095'
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-512
    sasl.jaas.config: 'org.apache.kafka.common.security.scram.ScramLoginModule required username=example_user password=12345678 ;'

ksmlDefinitions:
  generator.yaml: |
    # This example shows how to generate data and have it sent to a target topic in a given format.

    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate some random sensor measurement data
          types = { 0: { "type": "AREA", "unit": random.choice([ "m2", "ft2" ]), "value": str(random.randrange(1000)) },
                    1: { "type": "HUMIDITY", "unit": random.choice([ "g/m3", "%" ]), "value": str(random.randrange(100)) },
                    2: { "type": "LENGTH", "unit": random.choice([ "m", "ft" ]), "value": str(random.randrange(1000)) },
                    3: { "type": "STATE", "unit": "state", "value": random.choice([ "off", "on" ]) },
                    4: { "type": "TEMPERATURE", "unit": random.choice([ "C", "F" ]), "value": str(random.randrange(-100, 100)) }
                  }

          # Build the result value using any of the above measurement types
          value = { "name": key, "timestamp": str(round(time.time()*1000)), **random.choice(types) }
          value["color"] = random.choice([ "black", "blue", "red", "yellow", "white" ])
          value["owner"] = random.choice([ "Alice", "Bob", "Charlie", "Dave", "Evan" ])
          value["city"] = random.choice([ "Amsterdam", "Xanten", "Utrecht", "Alkmaar", "Leiden" ])

          if random.randrange(10) == 0:
            value = None
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Indicate the type of key and value

    producers:
      sensordata_json_producer:
        generator: generate_sensordata_message
        interval: 444
        to:
          topic: ksml_sensordata_json
          keyType: string
          valueType: json

applicationServer:
  enabled: true
  port: "8080"

prometheus:
  enabled: true
  port: "9993"
  config:
    lowercaseOutputName: true
    lowercaseOutputLabelNames: true
    rules:
      - pattern: 'ksml<type=app-info, app-id=(.+), app-name=(.+), app-version=(.+), build-time=(.+)>Value:'
        name: ksml_app_info
        labels:
          app_id: "$1"
          name: "$2"
          version: "$3"
          build_time: "$4"
        type: GAUGE
        value: 1
        cached: true
      - pattern: .*

logging:
  configFile: '/ksml-logging/logback.xml'
  jsonEnabled: false
  patterns:
    stdout: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
    stderr: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
  loggers:
    org.apache.kafka: INFO
    org.apache.kafka.streams.StreamsConfig: INFO
    org.apache.kafka.clients.consumer.ConsumerConfig: INFO

```
</details>

### Axual Kafka SASL Connection
<details>
  <summary>Example configuration to connect to an Axual Kafka cluster</summary>
See the ksmlRunnerConfig.kafka section for the special additions to automatically resolve resource names to the Axual namespaces

```YAML
image:
  tag: snapshot

replicaCount: 3

resources:
  requests:
    memory: "128Mi"
    cpu: "250m"
  limits:
    memory: "768Mi"
    cpu: "500m"

ksmlRunnerConfig:
  definitions:
    generate: generator.yaml
  kafka:
    application.id: example.datagen

    bootstrap.servers: 'testing-sasl-broker.axual.cloud:9095'
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-512
    sasl.jaas.config: 'org.apache.kafka.common.security.scram.ScramLoginModule required username=example_user password=12345678 ;'

    # Use these configuration properties when connecting to a cluster using the Axual naming patterns.
    # These patterns are resolved into the actual name used on Kafka using the values in this configuration map
    # and the topic names specified in the definition YAML files
    
    tenant: 'custom'
    instance: 'dta'
    environment: 'test'
    group.id.pattern: '{tenant}-{instance}-{environment}-{group.id}'
    topic.pattern: '{tenant}-{instance}-{environment}-{topic}'
    transactional.id.pattern: '{tenant}-{instance}-{environment}-{transactional.id}'

ksmlDefinitions:
  generator.yaml: |
    # This example shows how to generate data and have it sent to a target topic in a given format.

    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate some random sensor measurement data
          types = { 0: { "type": "AREA", "unit": random.choice([ "m2", "ft2" ]), "value": str(random.randrange(1000)) },
                    1: { "type": "HUMIDITY", "unit": random.choice([ "g/m3", "%" ]), "value": str(random.randrange(100)) },
                    2: { "type": "LENGTH", "unit": random.choice([ "m", "ft" ]), "value": str(random.randrange(1000)) },
                    3: { "type": "STATE", "unit": "state", "value": random.choice([ "off", "on" ]) },
                    4: { "type": "TEMPERATURE", "unit": random.choice([ "C", "F" ]), "value": str(random.randrange(-100, 100)) }
                  }

          # Build the result value using any of the above measurement types
          value = { "name": key, "timestamp": str(round(time.time()*1000)), **random.choice(types) }
          value["color"] = random.choice([ "black", "blue", "red", "yellow", "white" ])
          value["owner"] = random.choice([ "Alice", "Bob", "Charlie", "Dave", "Evan" ])
          value["city"] = random.choice([ "Amsterdam", "Xanten", "Utrecht", "Alkmaar", "Leiden" ])

          if random.randrange(10) == 0:
            value = None
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Indicate the type of key and value

    producers:
      sensordata_json_producer:
        generator: generate_sensordata_message
        interval: 444
        to:
          topic: ksml_sensordata_json
          keyType: string
          valueType: json
    
applicationServer:
  enabled: true
  port: "8080"

prometheus:
  enabled: true
  port: "9993"
  config:
    lowercaseOutputName: true
    lowercaseOutputLabelNames: true
    rules:
      - pattern: 'ksml<type=app-info, app-id=(.+), app-name=(.+), app-version=(.+), build-time=(.+)>Value:'
        name: ksml_app_info
        labels:
          app_id: "$1"
          name: "$2"
          version: "$3"
          build_time: "$4"
        type: GAUGE
        value: 1
        cached: true
      - pattern: .*

logging:
  configFile: '/ksml-logging/logback.xml'
  jsonEnabled: false
  patterns:
    stdout: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
    stderr: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
  loggers:
    org.apache.kafka: INFO
    org.apache.kafka.streams.StreamsConfig: INFO
    org.apache.kafka.clients.consumer.ConsumerConfig: INFO
```
</details>

### Example: Run KSML as a Kubernetes Job
<details>
  <summary>Example configuration to deploy the KSML application as a Job</summary>
By deploying as a job the pod running the KSML code will not restart on completion of the pods.

```YAML
image:
  tag: "snapshot"

deploymentMode: "Job"

resources:
  requests:
    memory: "128Mi"
    cpu: "250m"
  limits:
    memory: "768Mi"
    cpu: "500m"

ksmlRunnerConfig:
  definitions:
    generate: "generator.yaml"
  kafka:
    application.id: "example.datagen"
    bootstrap.servers: "broker:9092"
    security.protocol: PLAINTEXT


ksmlDefinitions:
  generator.yaml: |
    # This example shows how to generate data and have it sent to a target topic in a given format.

    functions:
      generate_sensordata_message:
        type: generator
        globalCode: |
          import time
          import random
          sensorCounter = 0
        code: |
          global sensorCounter

          key = "sensor"+str(sensorCounter)           # Set the key to return ("sensor0" to "sensor9")
          sensorCounter = (sensorCounter+1) % 10      # Increase the counter for next iteration

          # Generate some random sensor measurement data
          types = { 0: { "type": "AREA", "unit": random.choice([ "m2", "ft2" ]), "value": str(random.randrange(1000)) },
                    1: { "type": "HUMIDITY", "unit": random.choice([ "g/m3", "%" ]), "value": str(random.randrange(100)) },
                    2: { "type": "LENGTH", "unit": random.choice([ "m", "ft" ]), "value": str(random.randrange(1000)) },
                    3: { "type": "STATE", "unit": "state", "value": random.choice([ "off", "on" ]) },
                    4: { "type": "TEMPERATURE", "unit": random.choice([ "C", "F" ]), "value": str(random.randrange(-100, 100)) }
                  }

          # Build the result value using any of the above measurement types
          value = { "name": key, "timestamp": str(round(time.time()*1000)), **random.choice(types) }
          value["color"] = random.choice([ "black", "blue", "red", "yellow", "white" ])
          value["owner"] = random.choice([ "Alice", "Bob", "Charlie", "Dave", "Evan" ])
          value["city"] = random.choice([ "Amsterdam", "Xanten", "Utrecht", "Alkmaar", "Leiden" ])

          if random.randrange(10) == 0:
            value = None
        expression: (key, value)                      # Return a message tuple with the key and value
        resultType: (string, json)                    # Indicate the type of key and value

    producers:
      sensordata_json_producer:
        generator: generate_sensordata_message
        interval: 444
        to:
          topic: ksml_sensordata_json
          keyType: string
          valueType: json
    
applicationServer:
  enabled: true
  port: "8080"

prometheus:
  enabled: true
  port: "9993"
  config:
    lowercaseOutputName: true
    lowercaseOutputLabelNames: true
    rules:
      - pattern: 'ksml<type=app-info, app-id=(.+), app-name=(.+), app-version=(.+), build-time=(.+)>Value:'
        name: ksml_app_info
        labels:
          app_id: "$1"
          name: "$2"
          version: "$3"
          build_time: "$4"
        type: GAUGE
        value: 1
        cached: true
      - pattern: .*

logging:
  configFile: '/ksml-logging/logback.xml'
  jsonEnabled: false
  patterns:
    stdout: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
    stderr: '%date{"yyyy-MM-dd''T''HH:mm:ss,SSSXXX", UTC} %-5level %logger{36} - %msg%n'
  loggers:
    org.apache.kafka: INFO
    org.apache.kafka.streams.StreamsConfig: INFO
    org.apache.kafka.clients.consumer.ConsumerConfig: INFO
```
</details>

