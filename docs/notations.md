# Notations

### Table of Contents
1. [Introduction](#introduction)
1. [Avro](#avro)
1. [CSV](#csv)
1. [JSON](#json)
1. [SOAP](#soap)
1. [XML](#xml)

## Introduction

KSML is able to express its internal data types in a number of external representations. Internally these are called _notations_.
The different notations are described below.

## AVRO

Avro types are supported through the "avro" prefix in types. The notation is ```avro:schema```, where schema is the schema fqdn, or just the schema name itself.

On Kafka topics, Avro types are serialized in binary format. Internally they are represented as structs.

Examples
```
avro:SensorData
avro:io.axual.ksml.example.SensorData
```

Note: when referencing an AVRO schema, you have to ensure that the respective schema file can be found in the KSML working directory and has the .avsc file extension.

## CSV

Comma-separated values are supported through the "csv" prefix in types. The notation is ```csv:schema```, where schema is the schema fqdn, or just the schema name itself.

On Kafka topics, CSV types are serialized as `string`. Internally they are represented as structs.

Examples
```
csv:SensorData
csv:io.axual.ksml.example.SensorData
```

Note: when referencing an CSV schema, you have to ensure that the respective schema file can be found in the KSML working directory and has the .csv file extension.

## JSON

JSON types are supported through the "json" prefix in types. The notation is ```json:schema```, where `schema` is the schema fqdn, or just the schema name itself.

On Kafka topics, JSON types are serialized as `string`. Internally they are represented as structs or lists.

Examples
```
json:SensorData
json:io.axual.ksml.example.SensorData
```

If you want to use JSON without a schema, you can leave out the colon and schema name:

```
json
```
Note: when referencing an JSON schema, you have to ensure that the respective schema file can be found in the KSML working directory and has the .json file extension.

## SOAP

SOAP is supported through built-in serializers and deserializers. The representation on Kafka will always be ```string```. Internally SOAP objects are structs with their own schema. Field names are derived from the SOAP standards.

## XML

XML is supported through built-in serializers and deserializers. The representation on Kafka will always be ```string```. Internally XML objects are structs.

Examples
```
xml:SensorData
xml:io.axual.ksml.example.SensorData
```

Note: when referencing an XML schema, you have to ensure that the respective schema file can be found in the KSML working directory and has the .xsd file extension.
