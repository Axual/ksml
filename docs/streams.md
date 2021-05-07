# Streams

### Table of Contents
1. [Introduction](#introduction)
2. [Stream](#stream)
3. [Table](#table)
4. [GlobalTable](#globaltable)

## Introduction

Every KSML definition file contains a list of declared streams. There are three types of streams supported:

|Type|Kafka Streams equivalent|Description|
|:----|----|----|
|Stream|[`KStream`](https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/kstream/KStream.html)|KStream is an abstraction of a record stream of KeyValue pairs, i.e., each record is an independent entity/event in the real world. For example a user X might buy two items I1 and I2, and thus there might be two records <K:I1>, <K:I2> in the stream.<br/><br/>A KStream is either defined from one or multiple Kafka topics that are consumed message by message or the result of a KStream transformation. A KTable can also be converted into a KStream.<br/><br/>A KStream can be transformed record by record, joined with another KStream, KTable, GlobalKTable, or can be aggregated into a KTable.
|Table|[`KTable`](https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/kstream/KTable.html)|KTable is an abstraction of a changelog stream from a primary-keyed table. Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.<br/><br/>A KTable is either defined from a single Kafka topic that is consumed message by message or the result of a KTable transformation. An aggregation of a KStream also yields a KTable.<br/><br/>A KTable can be transformed record by record, joined with another KTable or KStream, or can be re-partitioned and aggregated into a new KTable.
|GlobalTable|[`GlobalKTable`](link:https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/kstream/GlobalKTable.html)|GlobalKTable is an abstraction of a changelog stream from a primary-keyed table. Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.<br/><br/>GlobalKTable can only be used as right-hand side input for stream-table joins.<br/><br/>In contrast to a KTable that is partitioned over all KafkaStreams instances, a GlobalKTable is fully replicated per KafkaStreams instance. Every partition of the underlying topic is consumed by each GlobalKTable, such that the full set of data is available in every KafkaStreams instance. This provides the ability to perform joins with KStream without having to repartition the input stream. All joins with the GlobalKTable require that a KeyValueMapper is provided that can map from the KeyValue of the left hand side KStream to the key of the right hand side GlobalKTable.

The definitions of these stream types are done as described below.

## Stream

Example:

```yaml
streams:
  - topic: some_kafka_topic
    keyType: string
    valueType: string
```

## Table

Example:

```yaml
tables:
  - topic: some_kafka_compaction_topic
    keyType: string
    valueType: string
```

## GlobalTable

Example:

```yaml
globalTables:
  - topic: some_kafka_compaction_topic
    keyType: string
    valueType: string
```
