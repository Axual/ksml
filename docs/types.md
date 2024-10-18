# Types

### Table of Contents

1. [Introduction](#introduction)
2. [Notations](#notations)
3. [Primitives](#primitives)
4. [Any](#any)
5. [Duration](#duration)
6. [Enum](#enum)
7. [List](#list)
8. [Struct](#struct)
9. [Tuple](#tuple)
10. [Windowed](#windowed)

## Introduction

KSML supports a wide range of simple and complex types. In addition, each data type
can be read from Kafka, and written to Kafka, using _Notations_. More about Notations
in the following paragraph. The paragraphs after will dive more into the data
types that KSML supports.

## Notations

KSML has a number of simple and complex data types. Simple data types contain a
single value. For example, a `long` or a `string` type can hold only one value.
Complex data types contain multiple values. For example, a `list` or a `struct`
can contain zero or more other simple or complex values. For example, a `struct`
is comparable to a key/value map, where all keys are of type `string`. Its contents
could be represented as:

```json
{
  "name": "Albert",
  "lastName": "Einstein",
  "dateOfBirth": "14-08-1879",
  "profession": "Patent clerk",
  "children": 3,
  "isAlive": false
}
```

There are several ways in which structured objects are read from or written to
Kafka topics. For instance, using AVRO, JSON or XML. Each use the same `struct`
but translate it differently to a binary or string representation that can be
stored in Kafka.

To facilitate these different ways of writing, KSML supports _Notations_. Each
notation can be seen as 'the format that data is written in' to Kafka. Below is
a list of all supported notations:

| Notation | Link                                                                                                         | Implementations               |
|----------|--------------------------------------------------------------------------------------------------------------|-------------------------------|
| AVRO     | [https://avro.apache.org](https://avro.apache.org)                                                           | apicurio_avro, confluent_avro |
| CSV      | [https://en.wikipedia.org/wiki/Comma-separated_values](https://en.wikipedia.org/wiki/Comma-separated_values) | <internal, Jackson>           |
| JSON     | [https://json.org](https://json.org)                                                                         | <internal, Jackson>           |
| SOAP     | [https://en.wikipedia.org/wiki/SOAP](https://en.wikipedia.org/wiki/SOAP)                                     | <internal, Jakarta>           |
| XML      | [https://en.wikipedia.org/wiki/XML](https://en.wikipedia.org/wiki/XML)                                       | <internal, Jackson>           |

Notations are a natural extension to data types. They can be used as:
```
notation:datatype
```

For example:
```
avro:SensorData     # AVRO with schema SensorData (loaded from SensorData.avsc)
json                # Schemaless JSON
xml:PersonSchema    # XML with schema PersonSchema (loaded from PersonSchema.xsd)
```

## Primitives

The following native types are supported.

| Type          | Description                                                                                                |
|---------------|------------------------------------------------------------------------------------------------------------|
| ?, or any     | Any type                                                                                                   |
| null, or none | Null type, available for variables without a value (eg. Kafka tombstone messages, or optional AVRO fields) |
| boolean       | Boolean values, ie. true or false                                                                          |
| double        | Double precision floating point                                                                            |
| float         | Single precision floating point                                                                            |
| byte          | 8-bit integer                                                                                              |
| short         | 16-bit integer                                                                                             |
| int           | 32-bit integer                                                                                             |
| long          | 64-bit long                                                                                                |
| bytes         | Byte array                                                                                                 |
| string        | String of characters                                                                                       |
| struct        | Key-value map, where with `string` keys and values of any type                                             |

## Any

The special type `?` or `any` can be used in places where input is uncertain. Code that deals
with input of this type should always perform proper type checking before assuming
any specific underlying type.

## Duration

Some fields in the KSML spec are of type `duration`. These fields have a fixed format `123x`, where `123` is an
integer and `x` is any of the following:

* _<none>_: milliseconds
* `s`: seconds
* `m`: minutes
* `h`: hours
* `d`: days
* `w`: weeks

## Enum

Enumerations can be defined as individual types, through:

```
enum(literal1, literal2, ...)
```

## List

Lists contain elements of the same type. They are defined using:

```
[elementType]
```

Examples:

```
[string]
[long]
[avro:SensorData]
[(long,string)]
```

## Struct

Structs are key-value maps, as usually found in AVRO or JSON messages. They are defined
using:

```
struct
```

## Tuple

Tuples combine multiple subtypes into one. For example `(1, "text")` is a tuple containing an integer and a string
element.
Tuple types always have a fixed number of elements.

Examples:

```
(long, string)
(avro:SensorData, string, long, string)
([string], long)
```

## Union

Unions are 'either-or' types. They have their own internal structure and can be described
by respective data schema. Unions are defined using:

```
union(type1, type2, ...)
```

Examples:

```
union(null, string)
union(avro:SensorData, long, string)
```

## Windowed

Some Kafka Streams operations modify the key type from _K_ to _Windowed\<K>_. Kafka Streams uses the
_Windowed\<K>_ type to group Kafka messages with similar keys together. The result is always a time-bound
window, with a defined start and end time.

KSML can convert the internal _Windowed\<K>_ type into a struct type with five fields:

* `start`: The window start timestamp (type `long`)
* `end`: The window end timestamp (type `long`)
* `startTime`: The window start time (type `string`)
* `endTime`: The window end time (type `string`)
* `key`: The key used to group items together in this window (type is the same as the original key type)

However, in pipelines or topic definitions users may need to refer to this type explicitly. This
is done in the following manner:

```
notation:windowed(keytype)
```

For example:

```
avro:windowed(avro:SensorData)
xml:windowed(long)
```
