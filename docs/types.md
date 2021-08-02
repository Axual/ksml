[<< Back to index](index.md)

# Types

### Table of Contents
1. [Introduction](#introduction)
1. [Native types](#native types)
1. [Duration](#duration)
1. [Avro](#avro)
1. [Json](#json)

## Introduction

Types are very useful.

## Native types

The following native types are supported.

|Type|Description|Remarks|
|---|---|---|
|?|Any type||
|boolean|Boolean values, ie. true or false|
|bytes|Byte array|
|double|Double precision floating point|
|float|Single precision floating point|
|int|32-bit integer|
|long|64-bit long|
|string|String of characters|

## Duration

A string representing a duration in time. It can be defined as follows:
```
###x
```

where `#` is a positive number between 0 and 999999 and `x` is an optional letter from the following table:

|Letter|Description|
|--------|------------|
|_none_|Duration in milliseconds
|s|Duration in seconds
|m|Duration in minutes
|h|Duration in hours
|d|Duration in days
|w|Duration in weeks

Examples:

```
100 ==> hundred milliseconds
30s ==> thirty seconds
8h ==> eight hours
2w ==> two weeks
```

## AVRO

Avro types are supported through the "avro" prefix in types. The notation is ```avro:schema```, where schema is the schema fqdn, or just the schema name itself.

On Kafka topics, Avro types are serialized in binary format. Internally they are represented as records.

Examples
```
avro:SensorData
avro:io.axual.ksml.example.SensorData
```

Note: when referencing a schema, please ensure that the respective Avro schema file can be found in the KSML working directory.

### Windowed

Some Kafka Streams operations modify the key type from _K_ to _Windowed\<K>_. KSML generates an AVRO
schema on-the-fly for any windowed type. These Avro windowed definitions will not be found on disk,
so during the loading of the KSML definition this may lead to problems. To circumvent, use the
following notation to refer to a windowed key type:
```
avro:windowed(string)
avro:windowed(SensorData)
```

## JSON

JSON is supported through built-in serializers and deserializers. The representation on Kafka will always be ```string```. Internally JSON objects are either records or lists.

For windowed keys, the JSON representation modifies the _Windowed_ type to a plain JSON object,
containing 4 timestamp fields and a key field with the original key.
