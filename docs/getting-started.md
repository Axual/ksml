[<< Back to index](index.md)

# Getting started

### Table of Contents
1. [Introduction](#introduction)
2. [Starting a demo setup](#starting-a-demo-setup)
3. [Starting a KSML runner](#starting-a-ksml-runner)
4. [Next steps](#next-steps)

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
example-producer-1  | 2024-03-06T20:24:49,480Z INFO  i.a.k.r.backend.KafkaProducerRunner  Calling generate_sensordata_message
example-producer-1  | 2024-03-06T20:24:49,480Z INFO  i.a.k.r.backend.ExecutableProducer   Message: key=sensor2, value=SensorData: {"city":"Utrecht", "color":"white", "name":"sensor2", "owner":"Alice", "timestamp":1709756689480, "type":"HUMIDITY", "unit":"%", "value":"66"}
example-producer-1  | 2024-03-06T20:24:49,481Z INFO  i.a.k.r.backend.ExecutableProducer   Produced message to ksml_sensordata_avro, partition 0, offset 1975
example-producer-1  | 2024-03-06T20:24:49,481Z INFO  i.a.k.r.backend.KafkaProducerRunner  Calling generate_sensordata_message
example-producer-1  | 2024-03-06T20:24:49,481Z INFO  i.a.k.r.backend.ExecutableProducer   Message: key=sensor3, value=SensorData: {"city":"Alkmaar", "color":"black", "name":"sensor3", "owner":"Dave", "timestamp":"1709756689481", "type":"STATE", "unit":"state", "value":"off"}
example-producer-1  | 2024-03-06T20:24:49,483Z INFO  i.a.k.r.backend.Execut^Cestamp":1709756689814, "type":"AREA", "unit":"ft2", "value":"76"}
example-producer-1  | 2024-03-06T20:24:49,815Z INFO  i.a.k.r.backend.ExecutableProducer   Produced message to ksml_sensordata_xml, partition 0, offset 1129
example-producer-1  | 2024-03-06T20:24:49,921Z INFO  i.a.k.r.backend.KafkaProducerRunner  Calling generate_sensordata_message
example-producer-1  | 2024-03-06T20:24:49,922Z INFO  i.a.k.r.backend.ExecutableProducer   Message: key=sensor6, value=SensorData: {"city":"Amsterdam", "color":"yellow", "name":"sensor6", "owner":"Evan", "timestamp":1709756689922, "type":"AREA", "unit":"m2", "value":"245"}
example-producer-1  | 2024-03-06T20:24:49,923Z INFO  i.a.k.r.backend.ExecutableProducer   Produced message to ksml_sensordata_avro, partition 0, offset 1976
example-producer-1  | 2024-03-06T20:24:50,035Z INFO  i.a.k.r.backend.KafkaProducerRunner  Calling generate_sensordata_message
example-producer-1  | 2024-03-06T20:24:50,035Z INFO  i.a.k.r.backend.ExecutableProducer   Message: key=sensor7, value=SensorData: {"city":"Alkmaar", "color":"black", "name":"sensor7", "owner":"Dave", "timestamp":"1709756690035", "type":"TEMPERATURE", "unit":"C", "value":"0"}

```


## Starting a KSML runner

To start a container which executes the example KSML definitions, type

```
./examples/run.sh
```

This will start the KSML docker container. You should see the following typical output:

```
2024-03-06T20:24:51,921Z INFO  io.axual.ksml.runner.KSMLRunner      Starting KSML Runner 1.76.0.0
...
...
...
...
2024-03-06T20:24:57,196Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Alkmaar', 'color': 'yellow', 'name': 'sensor9', 'owner': 'Bob', 'timestamp': 1709749917190, 'type': 'LENGTH', 'unit': 'm', 'value': '562', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:24:57,631Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor3, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor3', 'owner': 'Bob', 'timestamp': 1709749917628, 'type': 'HUMIDITY', 'unit': 'g/m3', 'value': '23', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:24:58,082Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor6, value={'city': 'Amsterdam', 'color': 'white', 'name': 'sensor6', 'owner': 'Bob', 'timestamp': 1709749918078, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '64', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:24:58,528Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor9, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor9', 'owner': 'Evan', 'timestamp': 1709749918524, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '87', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:24:58,970Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor1, value={'city': 'Amsterdam', 'color': 'black', 'name': 'sensor1', 'owner': 'Bob', 'timestamp': 1709749918964, 'type': 'TEMPERATURE', 'unit': 'F', 'value': '75', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}
2024-03-06T20:24:59,412Z INFO  ksml.functions.log_message  Consumed AVRO message - key=sensor5, value={'city': 'Amsterdam', 'color': 'blue', 'name': 'sensor5', 'owner': 'Bob', 'timestamp': 1709749919409, 'type': 'LENGTH', 'unit': 'm', 'value': '658', '@type': 'SensorData', '@schema': { <<Cleaned KSML Representation of Avro Schema>>}}

```

## Next steps

Check out the examples in the [Examples]({{ site.github.repository_url }}/tree/main/examples/) directory. By modifying the file `examples/ksml-runner.yaml` you can select the example(s) to run.

For a more elaborate introduction, you can start [here](introduction.md) or refer to the [documentation](index.md).
