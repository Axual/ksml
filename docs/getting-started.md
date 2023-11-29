[<< Back to index](index.md)

# Getting started

### Table of Contents
1. [Introduction](#introduction)
2. [Generic configuration](#generic-configuration)
    * [Kafka runner](#kafka-runner)
    * [Axual runner](#axual-runner)
3. [Starting a container](#starting-a-container)

## Introduction
KSML comes with an example setup, where a producer outputs SensorData messages to Kafka, which are then processed by KSML pipelines.

## Starting a demo setup
After checking out the repository, go to the KSML directory and execute the following:

```
docker compose up -d
```

This will start Zookeeper, Kafka and a Schema Registry in the background. It will also start the demo producer, which outputs two random messages per second on a `ksml_sensordata_avro` topic.

You can check the valid starting of these containers using the following command:

```
docker compose logs -f
```

Press CTRL-C when you verified data is produced. This typically looks like this:
```
example-producer_1  | 2021-05-11T07:28:31,005Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Producing: 1787
example-producer_1  | 2021-05-11T07:28:31,006Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Produced message to topic ksml_sensordata_avro partition 0 offset 1787
example-producer_1  | 2021-05-11T07:28:31,506Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Producing: 1788
example-producer_1  | 2021-05-11T07:28:31,508Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Produced message to topic ksml_sensordata_avro partition 0 offset 1788
example-producer_1  | 2021-05-11T07:28:32,008Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Producing: 1789
example-producer_1  | 2021-05-11T07:28:32,010Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Produced message to topic ksml_sensordata_avro partition 0 offset 1789
example-producer_1  | 2021-05-11T07:28:32,510Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Producing: 1790
example-producer_1  | 2021-05-11T07:28:32,512Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Produced message to topic ksml_sensordata_avro partition 0 offset 1790
example-producer_1  | 2021-05-11T07:28:33,012Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Producing: 1791
example-producer_1  | 2021-05-11T07:28:33,013Z [system] [main] INFO  i.a.k.e.producer.KSMLExampleProducer - Produced message to topic ksml_sensordata_avro partition 0 offset 1791
```


## Starting a KSML runner

To start a container which executes the example KSML definitions, type

```
./examples/run.sh
```

This will start the KSML docker container. You should see the following typical output:

```
2021-05-11T07:20:22,844Z [system] [pool-1-thread-1] INFO  i.a.k.r.runner.kafka.KafkaBackend - Starting Kafka Backend
key=sensor0, value={'owner': 'Evan', 'color': 'blue', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor0', 'value': 'off', 'timestamp': 1620717212876L}
key=sensor1, value={'owner': 'Alice', 'color': 'red', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'F', 'name': 'sensor1', 'value': '811', 'timestamp': 1620717213896L}
key=sensor2, value={'owner': 'Charlie', 'color': 'white', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor2', 'value': 'on', 'timestamp': 1620717214404L}
key=sensor3, value={'owner': 'Alice', 'color': 'blue', 'city': 'Alkmaar', '@type': 'io.axual.ksml.example.SensorData', 'type': 'AREA', 'unit': 'ft2', 'name': 'sensor3', 'value': '580', 'timestamp': 1620717214908L}
key=sensor4, value={'owner': 'Charlie', 'color': 'blue', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor4', 'value': 'off', 'timestamp': 1620717215412L}
key=sensor5, value={'owner': 'Alice', 'color': 'black', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'm', 'name': 'sensor5', 'value': '307', 'timestamp': 1620717215917L}
key=sensor6, value={'owner': 'Evan', 'color': 'yellow', 'city': 'Amsterdam', '@type': 'io.axual.ksml.example.SensorData', 'type': 'HUMIDITY', 'unit': '%', 'name': 'sensor6', 'value': '98', 'timestamp': 1620717216421L}
key=sensor7, value={'owner': 'Charlie', 'color': 'black', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'AREA', 'unit': 'ft2', 'name': 'sensor7', 'value': '391', 'timestamp': 1620717216924L}
key=sensor8, value={'owner': 'Dave', 'color': 'yellow', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'ft', 'name': 'sensor8', 'value': '592', 'timestamp': 1620717217428L}
key=sensor9, value={'owner': 'Charlie', 'color': 'blue', 'city': 'Xanten', '@type': 'io.axual.ksml.example.SensorData', 'type': 'TEMPERATURE', 'unit': 'C', 'name': 'sensor9', 'value': '558', 'timestamp': 1620717217932L}
key=sensor0, value={'owner': 'Alice', 'color': 'black', 'city': 'Leiden', '@type': 'io.axual.ksml.example.SensorData', 'type': 'LENGTH', 'unit': 'ft', 'name': 'sensor0', 'value': '706', 'timestamp': 1620717218435L}
key=sensor1, value={'owner': 'Bob', 'color': 'yellow', 'city': 'Utrecht', '@type': 'io.axual.ksml.example.SensorData', 'type': 'STATE', 'unit': 'state', 'name': 'sensor1', 'value': 'off', 'timestamp': 1620717218940L
```

## Next steps

Check out the examples in the [Examples](../examples/) directory. By modifying the file `examples/ksml-runner.yaml` you can select the example(s) to run.

For a more elaborate introduction, you can start [here](introduction.md) or refer to the [documentation](index.md).
