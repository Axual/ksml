# Operations

### Table of Contents

1. [Introduction](#introduction)
2. [Operations](#transform-operations)
    * [aggregate](#aggregate)
    * [cogroup](#cogroup)
    * [convertKey](#convertkey)
    * [convertKeyValue](#convertkeyvalue)
    * [convertValue](#convertvalue)
    * [count](#count)
    * [filter](#filter)
    * [filterNot](#filternot)
    * [flatMap](#flatmap)
    * [flatMapValues](#flatmapvalues)
    * [groupBy](#groupby)
    * [groupByKey](#groupbykey)
    * [join](#join)
    * [leftJoin](#leftjoin)
    * [map](#map)
    * [mapKey](#mapkey)
    * [mapValue](#mapvalue)
    * [mapValues](#mapvalues)
    * [merge](#merge)
    * [outerJoin](#outerjoin)
    * [peek](#peek)
    * [reduce](#reduce)
    * [repartition](#repartition)
    * [selectKey](#selectkey)
    * [suppress](#suppress)
    * [toStream](#tostream)
    * [transformKey](#transformkey)
    * [transformKeyValue](#transformkeyvalue)
    * [transformKeyValueToKeyValueList](#transformkeyvaluetokeyvaluelist)
    * [transformKeyValueToValueList](#transformkeyvaluetovaluelist)
    * [transformValue](#transformvalue)
    * [windowBySession](#windowbysession)
    * [windowByTime](#windowbytime)
3. [Sink Operations](#sink-operations)
    * [as](#as)
    * [branch](#branch)
    * [forEach](#foreach)
    * [print](#print)
    * [to](#to)
    * [toTopicNameExtractor](#totopicnameextractor)

[Duration]: types.md#duration

[Store]: reference/state-store-reference.md

[KStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html

[KTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html

[GlobalKTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/GlobalKTable.html

[KGroupedStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html

[KGroupedTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html

[SessionWindowedKStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html

[SessionWindowedCogroupedKStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedCogroupedKStream.html

[TimeWindowedKStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html

[TimeWindowedCogroupedKStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedCogroupedKStream.html

[Aggregator]: functions.md#function-types

[Initializer]: functions.md#function-types

[KeyTransformer]: functions.md#function-types

[KeyValueMapper]: functions.md#function-types

[KeyValueToKeyValueListTransformer]: functions.md#function-types

[KeyValueToValueListTransformer]: functions.md#function-types

[KeyValueTransformer]: functions.md#function-types

[Merger]: functions.md#function-types

[Predicate]: functions.md#function-types

[Reducer]: functions.md#function-types

[StreamPartitioner]: functions.md#function-types

[ValueTransformer]: functions.md#function-types

[Windowed]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/Windowed.html

## Introduction

Pipelines in KSML have a beginning, a middle and (optionally) an end. Operations form the middle part of pipelines. They
are modeled as separate YAML entities, where each operation takes input from the previous operation and applies its own
logic. The returned stream then serves as input for the next operation.

## Transform Operations

Transformations are operations that take an input stream and convert it to an output stream. This section lists all
supported transformations. Each one states the type of stream it returns.

| Parameter | Value Type | Description                |
|:----------|:-----------|:---------------------------|
| `name`    | string     | The name of the operation. |

Note that not all combinations of output/input streams are supported by Kafka Streams. The user that writes the KSML
definition needs to make sure that streams that result from one operation can actually serve as input to the next. KSML
does type checking and will exit with an error when operations that can not be chained together are listed after another
in the KSML definition.

### aggregate

This operation aggregates multiple values into a single one by repeatedly calling an aggregator function. It can
operate on a range of stream types.

| Stream Type                              | Returns                    | Parameter     | Value Type          | Required | Description                                                                                                                                                                                                                                        |
|:-----------------------------------------|:---------------------------|:--------------|:--------------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream]`<K,V>`                  | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                                                                                   |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
|                                          |                            | `aggregator`  | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `VR`.      |
| [KGroupedTable]`<K,V>`                   | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                                                                                   |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
|                                          |                            | `adder`       | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `VR`.      |
|                                          |                            | `subtractor`  | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should remove the key/value from the previously calculated `aggregateValue` and return a new aggregate value of type `VR`. |
| [SessionWindowedKStream]`<K,V>`          | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `session`.                                                                                                                                                                                    |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
|                                          |                            | `aggregator`  | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `VR`.      |
|                                          |                            | `merger`      | Inline or reference | Yes      | A [Merger] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the merged result, also of type `V`.                                                                                   |
| [TimeWindowedKStreamObject]`<K,V>`       | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                                                                                     |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
|                                          |                            | `aggregator`  | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `VR`.      |
| [CogroupedKStream]`<K,V>`                | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                                                                                   |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
| [SessionWindowedCogroupedKStream]`<K,V>` | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `session`.                                                                                                                                                                                    |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |
|                                          |                            | `merger`      | Inline or reference | Yes      | A [Merger] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the merged result, also of type `V`.                                                                                   |
| [TimeWindowedCogroupedKStream]`<K,V>`    | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                                                                                     |
|                                          |                            | `initializer` | Inline or reference | Yes      | An [Initializer] function, which takes no arguments and returns a value of type `VR`.                                                                                                                                                              |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: aggregate
        initializer:
          expression: 0
        aggregator:
          expression: aggregatedValue + value
      - type: toStream
    to: output_stream
```

### cogroup

This operation cogroups multiple values into a single one by repeatedly calling an aggregator function. It can
operate on a range of stream types.

| Stream Type               | Returns                    | Parameter    | Value Type          | Required | Description                                                                                                                                                                                                                                   |
|:--------------------------|:---------------------------|:-------------|:--------------------|:---------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream]`<K,V>`   | [CogroupedKStream]`<K,VR>` | `aggregator` | Inline or reference | Yes      | An [Aggregator] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `VR`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `VR`. |
| [CogroupedKStream]`<K,V>` | n/a                        | n/a          | n/a                 | n/a      | This method is currently not supported in KSML.                                                                                                                                                                                               |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: cogroup
        aggregator:
          expression: aggregatedValue + value
      - type: toStream
    to: output_stream
```

_Note: this operation was added to KSML for completion purposes, but is not considered ready or fully functional. Feel
free to experiment, but don't rely on this in production. Syntax changes may occur in future KSML releases._

### convertKey

This built-in operation takes a message and converts the key into a given type.

| Stream Type      | Returns           | Parameter | Value Type | Description                                                           |
|:-----------------|:------------------|:----------|:-----------|:----------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<KR,V>` | `into`    | string     | The type to convert the key into. Conversion to `KR` is done by KSML. |

Example:

```yaml
pipelines:
  example_pipeline:
    from:
      topic: input_stream
      keyType: string
      valueType: string
    via:
      - type: convertKey
        into: json
    to: output_stream
```

### convertKeyValue

This built-in operation takes a message and converts the key and value into a given type.

| Stream Type      | Returns            | Parameter | Value Type | Description                                                                                                  |
|:-----------------|:-------------------|:----------|:-----------|:-------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<KR,VR>` | `into`    | string     | The type to convert the key and value into. Conversion of key into `KR` and value into `VR` is done by KSML. |

Example:

```yaml
pipelines:
  example_pipeline:
    from:
      topic: input_stream
      keyType: string
      valueType: string
    via:
      - type: convertKeyValue
        into: (json,xml)
    to: output_stream
```

### convertValue

This built-in operation takes a message and converts the value into a given type.

| Stream Type      | Returns           | Parameter | Value Type | Description                                                                        |
|:-----------------|:------------------|:----------|:-----------|:-----------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `into`    | string     | The type to convert the value into. Conversion of value into `VR` is done by KSML. |

Example:

```yaml
pipelines:
  example_pipeline:
    from:
      topic: input_stream
      keyType: string
      valueType: string
    via:
      - type: convertValue
        into: xml
    to: output_stream
```

### count

This operation counts the number of messages and returns a table multiple values into a single one by repeatedly
calling an aggregator function. It can operate on a range of stream types.

| Stream Type                        | Returns                      | Parameter | Value Type          | Required | Description                                                      |
|:-----------------------------------|:-----------------------------|:----------|:--------------------|:---------|:-----------------------------------------------------------------|
| [KGroupedStream]`<K,V>`            | [KTable]`<K,Long>`           | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`. |
| [KGroupedTable]`<K,V>`             | [KTable]`<K,Long>`           | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`. |
| [SessionWindowedKStream]`<K,V>`    | [KTable]`<Windowed<K>,Long>` | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `session`.  |
| [TimeWindowedKStreamObject]`<K,V>` | [KTable]`<Windowed<K>,Long>` | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `window`.   |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: count
      - type: toStream
    to: output_stream
```

### filter

Filter all incoming messages according to some predicate. The predicate function is called for every message. Only when
the predicate returns `true`, then the message will be sent to the output stream.

| Stream Type      | Returns          | Parameter | Value Type          | Required            | Description                                                                                         |
|:-----------------|:-----------------|:----------|:--------------------|:--------------------|:----------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,V>` | `if`      | Yes                 | Inline or reference | A [Predicate] function, which returns `True` if the message can pass the filter, `False` otherwise. |
|                  |                  |           |                     |                     |                                                                                                     |
| [KTable]`<K,V>`  | [KTable]`<K,V>`  | `store`   | Store configuration | No                  | An optional [Store] configuration, should be of type `keyValue`.                                    |
|                  |                  | `if`      | Yes                 | Inline or reference | A [Predicate] function, which returns `True` if the message can pass the filter, `False` otherwise. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: key.startswith('a')
    to: output_stream
```

### filterNot

This operation works exactly like [filter](#filter), but negates all predicates before applying them. That means
messages for which the predicate returns `False` are accepted, while those that the predicate returns `True` for are
filtered out.
See [filter](#filter) for details on how to implement.

### flatMap

This operation takes a message and transforms it into zero, one or more new messages, which may have different key and
value types than the source.

| Stream Type      | Returns            | Parameter | Value Type          | Required | Description                                                                                                                                                                                            |
|:-----------------|:-------------------|:----------|:--------------------|:---------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<KR,VR>` | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return type should be a list of type `[(KR,VR)]` containing a list of transformed `key` and `value` pairs. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: flatMap
        mapper:
          expression: [ (key,value), (key,value) ]   # duplicate all incoming messages
    to: output_stream
```

### flatMapValues

This operation takes a message and transforms it into zero, one or more new values, which may have different value types
than the source. Every entry in the result list is combined with the source key and produced on the output stream.

| Stream Type      | Returns           | Parameter | Value Type          | Description                                                                                                                                                                        |
|:-----------------|:------------------|:----------|:--------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `mapper`  | Inline or reference | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return type should be a list of type `[VR]` containing a list of transformed `value`s. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: flatMapValues
        mapper:
          expression: [ value+1, value+2, value+3 ]   # creates 3 new messages [key,VR] for every input message
    to: output_stream
```

### groupBy

Group the records of a stream by value resulting from a KeyValueMapper.

| Stream Type      | Returns                  | Parameter | Value Type          | Required | Description                                                                                                                                     |
|:-----------------|:-------------------------|:----------|:--------------------|:---------|:------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KGroupedStream]`<KR,V>` | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                |
|                  |                          | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V` and returns a value of type `KR` to group the stream by. |
| [KTable]`<K,V>`  | [KGroupedTable]`<KR,V>`  | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                |
|                  |                          | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V` and returns a value of type `KR` to group the stream by. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper:
          expression: value["some_field"]
          resultType: string
      - type: aggregate
        initializer:
          expression: 0
        aggregator:
          expression: value1+value2
      - type: toStream
    to: output_stream
```

### groupByKey

Group the records of a stream by the stream's key.

| Stream Type      | Returns                 | Parameter | Value Type          | Required | Description                                                      |
|:-----------------|:------------------------|:----------|:--------------------|:---------|:-----------------------------------------------------------------|
| [KStream]`<K,V>` | [KGroupedStream]`<K,V>` | `store`   | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupByKey
      - type: aggregate
        initializer:
          expression: 0
        aggregator:
          expression: value1+value2
      - type: toStream
    to: output_stream
```

### join

Join records of this stream with another stream's records using inner join. The join is computed on the
records' key with join predicate `thisStream.key == otherStream.key`. If both streams are not tables, then
their timestamps need to be close enough as defined by timeDifference.

| Stream Type      | Returns           | Parameter             | Value Type          | Required | Description                                                                                                                                                                            |
|:-----------------|:------------------|:----------------------|:--------------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `store`               | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                         |
|                  |                   | `stream`              | `string`            | Yes      | The name of the stream to join with. The stream should be of key type `K` and value type `VR`.                                                                                         |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the joined value of type `VR`.                        |
|                  |                   | `timeDifference`      | `duration`          | Yes      | The maximum allowed between two joined records.                                                                                                                                        |
|                  |                   | `grace`               | `duration`          | No       | A grace period during with out-of-order to-be-joined records may still arrive.                                                                                                         |
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `table`               | `string`            | Yes      | The name of the table to join with. The table should be of key type `K` and value type `VO`.                                                                                           |                                                                    |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `value1` of type `V` from the source table and a `value2` of type `VO` from the join table. The return value is the joined value of type `VR`. |
|                  |                   | `grace`               | `duration`          | No       | A grace period during with out-of-order to-be-joined records may still arrive.                                                                                                         |
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `globalTable`         | `string`            | Yes      | The name of the global table to join with. The global table should be of key type `GK` and value type `GV`.                                                                            |
|                  |                   | `mapper`              | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return value is the key of type `GK` of the records from the GlobalTable to join with.     |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the joined value of type `VR`.                        |
| [KTable]`<K,V>`  | [KTable]`<K,VR>`  | `store`               | Store configuration | No       | The [Store] configuration, should be of type `keyValue`.                                                                                                                               |
|                  |                   | `table`               | `string`            | Yes      | The name of the table to join with. The table should be of key type `K` and value type `VO`.                                                                                           |                                                                    |
|                  |                   | `foreignKeyExtractor` | Inline or reference | No       | A [ForeignKeyExtractor] function, which takes a `value` of type `V`, which needs to be converted into the key type `KO` of the table to join with.                                     |                                                  
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `value1` of type `V` from the source table and a `value2` of type `VO` from the join table. The return value is the joined value of type `VR`. |
|                  |                   | `partitioner`         | Inline or reference | No       | A [Partitioner] function, which partitions the records on the primary stream.                                                                                                          |                                                                                                           |
|                  |                   | `otherPartitioner`    | Inline or reference | No       | A [Partitioner] function, which partitions the records on the join table.                                                                                                              |

Example:

```yaml
pipelines:
   example_pipeline:
    from: input_stream
    via:
      - type: join
        stream: second_stream
        valueJoiner: my_key_value_mapper
        timeDifference: 1m
    to: output_stream
```

### leftJoin

Join records of this stream with another stream's records using left join. The join is computed on the
records' key with join predicate `thisStream.key == otherStream.key`. If both streams are not tables, then
their timestamps need to be close enough as defined by timeDifference.

| Stream Type      | Returns           | Parameter             | Value Type          | Required | Description                                                                                                                                                                            |
|:-----------------|:------------------|:----------------------|:--------------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `store`               | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                         |
|                  |                   | `stream`              | `string`            | Yes      | The name of the stream to join with. The stream should be of key type `K` and value type `VR`.                                                                                         |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the joined value of type `VR`.                        |
|                  |                   | `timeDifference`      | `duration`          | Yes      | The maximum allowed between two joined records.                                                                                                                                        |
|                  |                   | `grace`               | `duration`          | No       | A grace period during with out-of-order to-be-joined records may still arrive.                                                                                                         |
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `table`               | `string`            | Yes      | The name of the table to join with. The table should be of key type `K` and value type `VO`.                                                                                           |                                                                    |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `value1` of type `V` from the source table and a `value2` of type `VO` from the join table. The return value is the joined value of type `VR`. |
|                  |                   | `grace`               | `duration`          | No       | A grace period during with out-of-order to-be-joined records may still arrive.                                                                                                         |
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `globalTable`         | `string`            | Yes      | The name of the global table to join with. The global table should be of key type `GK` and value type `GV`.                                                                            |
|                  |                   | `mapper`              | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return value is the key of type `GK` of the records from the GlobalTable to join with.     |
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the joined value of type `VR`.                        |
| [KTable]`<K,V>`  | [KTable]`<K,VR>`  | `store`               | Store configuration | No       | The [Store] configuration, should be of type `keyValue`.                                                                                                                               |
|                  |                   | `table`               | `string`            | Yes      | The name of the table to join with. The table should be of key type `K` and value type `VO`.                                                                                           |                                                                    |
|                  |                   | `foreignKeyExtractor` | Inline or reference | No       | A [ForeignKeyExtractor] function, which takes a `value` of type `V`, which needs to be converted into the key type `KO` of the table to join with.                                     |                                                  
|                  |                   | `valueJoiner`         | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `value1` of type `V` from the source table and a `value2` of type `VO` from the join table. The return value is the joined value of type `VR`. |
|                  |                   | `partitioner`         | Inline or reference | No       | A [Partitioner] function, which partitions the records on the primary stream.                                                                                                          |                                                                                                           |
|                  |                   | `otherPartitioner`    | Inline or reference | No       | A [Partitioner] function, which partitions the records on the join table.                                                                                                              |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: leftJoin
        stream: second_stream
        valueJoiner: my_key_value_mapper
        timeDifference: 1m
    to: output_stream
```

### map

This operation takes a message and transforms the key and value into a new key and value, which can each have a
different type than the source message key and value.

| Stream Type      | Returns            | Parameter | Value Type          | Required | Description                                                                                                                                                                               |
|:-----------------|:-------------------|:----------|:--------------------|:---------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<KR,VR>` | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return type should be a tuple of type `(KR,VR)` containing the transformed `key` and `value`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: map
        mapper:
          expression: (str(key), str(value))   # convert key and value from source type to string
    to: output_stream
```

### mapKey

This is an alias for [selectKey](#selectkey).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: mapKey
        mapper:
          expression: str(key)   # convert key from source type to string
    to: output_stream
```

### mapValue

This is an alias for [mapValues](#mapvalues).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: mapValue
        mapper:
          expression: str(value)   # convert value from source type to String
    to: output_stream
```

### mapValues

This operation takes a message and transforms its value to a new value, which may have different value type
than the source.

| Stream Type      | Returns           | Parameter | Value Type          | Required | Description                                                                                                                             |
|:-----------------|:------------------|:----------|:--------------------|:---------|:----------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return type should be a value of type `VR`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: mapValues
        mapper:
          expression: str(value)   # convert value from source type to String
    to: output_stream
```

### merge

Merge this stream and the given stream into one larger stream. There is no ordering guarantee between records from this
stream and records from the provided stream in the merged stream. Relative order is preserved within each input stream
though (ie, records within one input stream are processed in order).

| Stream Type      | Returns          | Parameter | Value Type | Description                           |
|:-----------------|:-----------------|:----------|:-----------|:--------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,V>` | `stream`  | `string`   | The name of the stream to merge with. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: merge
        stream: second_stream
    to: output_stream
```

### outerJoin

Join records of this stream with another stream's records using outer join. The join is computed on the
records' key with join predicate `thisStream.key == otherStream.key`. If both streams are not tables, then
their timestamps need to be close enough as defined by timeDifference.

| Stream Type      | Returns           | Parameter        | Value Type          | Required | Description                                                                                                                                                                            |
|:-----------------|:------------------|:-----------------|:--------------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `store`          | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                         |
|                  |                   | `stream`         | `string`            | Yes      | The name of the stream to join with. The stream should be of key type `K` and value type `VR`.                                                                                         |
|                  |                   | `valueJoiner`    | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `key` of type `K`, and two values `value1` and `value2` of type `V`. The return value is the joined value of type `VR`.                        |
|                  |                   | `timeDifference` | `duration`          | Yes      | The maximum allowed between two joined records.                                                                                                                                        |
|                  |                   | `grace`          | `duration`          | No       | A grace period during with out-of-order to-be-joined records may still arrive.                                                                                                         |
| [KTable]`<K,V>`  | [KStream]`<K,VR>` | `store`          | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                       |
|                  |                   | `table`          | `string`            | Yes      | The name of the table to join with. The table should be of key type `K` and value type `VO`.                                                                                           |                                                                    |
|                  |                   | `valueJoiner`    | Inline or reference | Yes      | A [ValueJoiner] function, which takes a `value1` of type `V` from the source table and a `value2` of type `VO` from the join table. The return value is the joined value of type `VR`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: outerJoin
        stream: second_stream
        valueJoiner: my_key_value_mapper
        timeDifference: 1m
    to: output_stream
```

### peek

Perform an action on each record of a stream. This is a stateless record-by-record operation. Peek is a non-terminal
operation that triggers a side effect (such as logging or statistics collection) and returns an unchanged stream.

| Stream Type      | Returns          | Parameter | Value Type          | Description                                                                                                                  |
|:-----------------|:-----------------|:----------|:--------------------|:-----------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,V>` | `forEach` | Inline or reference | The [ForEach] function that will be called for every message, receiving arguments `key` of type `K` and `value` of type `V`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: peek
        forEach: print_key_and_value
    to: output_stream
```

### reduce

Combine the values of records in this stream by the grouped key. Records with null key or value are ignored. Combining
implies that the type of the aggregate result is the same as the type of the input value, similar
to [aggregate(Initializer, Aggregator)](#aggregate).

| Stream Type                        | Returns                   | Parameter    | Value Type          | Required | Description                                                                                                                                                                                                                                  |
|:-----------------------------------|:--------------------------|:-------------|:--------------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream]`<K,V>`            | [KTable]`<K,V>`           | `store`      | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                                                                             |
|                                    |                           | `reducer`    | Inline or reference | Yes      | A [Reducer] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `V`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `V`.      |
| [KGroupedTable]`<K,V>`             | [KTable]`<K,V>`           | `store`      | Store configuration | No       | An optional [Store] configuration, should be of type `keyValue`.                                                                                                                                                                             |
|                                    |                           | `adder`      | Inline or reference | Yes      | A [Reducer] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `V`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `V`.      |
|                                    |                           | `subtractor` | Inline or reference | Yes      | A [Reducer] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `V`. It should remove the key/value from the previously calculated `aggregateValue` and return a new aggregate value of type `V`. |
| [SessionWindowedKStream]`<K,V>`    | [KTable]`<Windowed<K>,V>` | `store`      | Store configuration | No       | An optional [Store] configuration, should be of type `session`.                                                                                                                                                                              |
|                                    |                           | `reducer`    | Inline or reference | Yes      | A [Reducer] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `V`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `V`.      |
| [TimeWindowedKStreamObject]`<K,V>` | [KTable]`<Windowed<K>,V>` | `store`      | Store configuration | No       | An optional [Store] configuration, should be of type `window`.                                                                                                                                                                               |
|                                    |                           | `reducer`    | Inline or reference | Yes      | A [Reducer] function, which takes a `key` of type `K`, a `value` of type `V` and `aggregatedValue` of type `V`. It should add the key/value to the previously calculated `aggregateValue` and return a new aggregate value of type `V`.      |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: aggregate
        initializer:
          expression: 0
        aggregator:
          expression: aggregatedValue + value
      - type: toStream
    to: output_stream
```

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: reduce
        reducer:
          expression: value1+value2
      - type: toStream
    to: output_stream
```

### repartition

Materialize this stream to an auto-generated repartition topic with a given number of partitions, using a custom
partitioner. Similar to auto-repartitioning, the topic will be created with infinite retention time and data will be
automatically purged. The topic will be named as "${applicationId}-<name>-repartition".

| Stream Type      | Returns          | Parameter            | Value Type          | Required | Description                                           |
|:-----------------|:-----------------|:---------------------|:--------------------|:---------|:------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,V>` | `numberOfPartitions` | integer             | No       | The number of partitions of the repartitioned topic.  |
|                  |                  | `partitioner`        | Inline or reference | No       | A custom [Partitioner] function to partition records. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: repartition
        name: my_partitioner
        numberOfPartitions: 3
        partitioner: my_own_partitioner
      - type: peek
        forEach: print_key_and_value
      - type: toStream
    to: output_stream
```

### selectKey

This operation takes a message and transforms the key into a new key, which may have a different type.

| Stream Type      | Returns           | Parameter | Value Type          | Required | Description                                                                                                                                                     |
|:-----------------|:------------------|:----------|:--------------------|:---------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<KR,V>` | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return value is the key of resulting stream, which is of type `KR`. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: selectKey
        mapper:
          expression: str(key)   # convert key from source type to string
    to: output_stream
```

### suppress

Suppress some updates from this changelog stream, determined by the supplied Suppressed configuration. When
_windowCloses_ is selected and no further restrictions are provided, then this is interpreted as
_Suppressed.untilWindowCloses(unbounded())_.

| Stream Type     | Returns         | Parameter            | Value Type | Required | Description                                                                                                                                                                                                                                |
|:----------------|:----------------|:---------------------|:-----------|:---------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KTable]`<K,V>` | [KTable]`<K,V>` | `until`              | `string`   | Yes      | This value can either be `timeLimit` or `windowCloses`. Note that _timeLimit_ suppression works on any stream, while _windowCloses_ suppression works only on _Windowed_ streams. For the latter, see [windowByTime] or [windowBySession]. |
|                 |                 | `duration`           | `string`   | No       | The [Duration] to suppress updates (only when `until`==`timeLimit`)                                                                                                                                                                        |
|                 |                 | `maxBytes`           | `int`      | No       | The maximum number of bytes to suppress updates                                                                                                                                                                                            |
|                 |                 | `maxRecords`         | `int`      | No       | The maximum number of records to suppress updates                                                                                                                                                                                          |
|                 |                 | `bufferFullStrategy` | `string`   | No       | Can be one of `emitEarlyWhenFull`, `shutdownWhenFull`                                                                                                                                                                                      |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_table
    via:
      - type: suppress
        until: timeLimit
        duration: 30s
        maxBytes: 128000
        maxRecords: 10000
        bufferFullStrategy: emitEarlyWhenFull
      - type: peek
        forEach: print_key_and_value
      - type: toStream
    to: output_stream
```

### toStream

Convert a KTable into a KStream object.

| Stream Type     | Returns           | Parameter | Value Type          | Required | Description                                                                                                                                                                                                           |
|:----------------|:------------------|:----------|:--------------------|:---------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KTable]`<K,V>` | [KStream]`<KR,V>` | `mapper`  | Inline or reference | No       | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return value is the key of resulting stream, which is of type `KR`. If no mapper is provided, then keys remain unchanged. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_table
    via:
      - type: toStream
    to: output_stream
```

### transformKey

This is an alias for [selectKey](#selectkey).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformKey
        mapper:
          expression: str(key)   # convert key from source type to string
    to: output_stream
```

### transformKeyValue

This is an alias for [map](#map).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformKeyValue
        mapper:
          expression: (str(key), str(value))   # convert key and value from source type to string
    to: output_stream
```

### transformKeyValueToKeyValueList

This is an alias for [flatMap](#flatmap).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformKeyValueToKeyValueList
        mapper:
          expression: [ (key,value), (key,value) ]   # duplicate all incoming messages
    to: output_stream
```

### transformKeyValueToValueList

This is an alias for [flapMapValues](#flatmapvalues).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformKeyValueToValueList
        mapper:
          expression: [ value+1, value+2, value+3 ]   # creates 3 new messages [key,VR] for every input message
    to: output_stream
```

### transformMetadata

This operation takes a message and transforms its value to a new value, which may have different value type
than the source.

| Stream Type      | Returns           | Parameter | Value Type          | Required | Description                                                                                                                                                                                                                     |
|:-----------------|:------------------|:----------|:--------------------|:---------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | [KStream]`<K,VR>` | `mapper`  | Inline or reference | Yes      | A [MetadataTransformer] function that converts the metadata (Kafka headers, timestamp) of every record in the stream. It gets a metadata object as input and should return the same type, but potentially with modified fields. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformValue
        mapper:
          expression: str(value)   # convert value from source type to String
    to: output_stream
```

### transformValue

This is an alias for [mapValues](#mapvalues).

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: transformValue
        mapper:
          expression: str(value)   # convert value from source type to String
    to: output_stream
```

### windowBySession

Create a new windowed KStream instance that can be used to perform windowed aggregations. For more details on the
different types of windows, please refer to [WindowTypes]|[this page].

| Stream Type               | Returns                                  | Parameter     | Value Type | Required | Description                                                                                          |
|:--------------------------|:-----------------------------------------|:--------------|:-----------|:---------|:-----------------------------------------------------------------------------------------------------|
| [KGroupedStream]`<K,V>`   | [SessionWindowedKStream]`<K,V>`          | inactivityGap | [Duration] | Yes      | The maximum inactivity gap with which keys are grouped.                                              |
|                           |                                          | grace         | [Duration] | No       | The grace duration allowing for out-of-order messages to still be associated with the right session. |
| [CogroupedKStream]`<K,V>` | [SessionWindowedCogroupedKStream]`<K,V>` | inactivityGap | [Duration] | Yes      | The maximum inactivity gap with which keys are grouped.                                              |
|                           |                                          | grace         | [Duration] | No       | The grace duration allowing for out-of-order messages to still be associated with the right session. |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: windowBySession
        inactivityGap: 1h
        grace: 5m
      - type: reduce
        reducer: my_reducer_function
      - type: toStream
    to: output_stream
```

### windowByTime

Create a new windowed KStream instance that can be used to perform windowed aggregations. For more details on the
different types of windows, please refer to [WindowTypes]|[this page].

| Stream Type               | Returns                               | Parameter      | Value Type | Description                                                                                                                                                                                                                  |
|:--------------------------|:--------------------------------------|:---------------|:-----------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream]`<K,V>`   | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `sliding`.                                                                                                                                                                                                       |
|                           |                                       | timeDifference | [Duration] | The time difference parameter for the [SlidingWindows] object.                                                                                                                                                               |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [SlidingWindows] object.                                                                                                                                                              |
| [KGroupedStream]`<K,V>`   | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `hopping`.                                                                                                                                                                                                       |
|                           |                                       | advanceBy      | [Duration] | The amount by which each window is advanced. If this value is not specified, then it will be equal to _duration_, which gives tumbling windows. If you make this value smaller than _duration_ you will get hopping windows. |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [KGroupedStream]`<K,V>`   | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `tumbling`.                                                                                                                                                                                                      |
|                           |                                       | duration       | [Duration] | The duration parameter for the [TimeWindows] object.                                                                                                                                                                         |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [CogroupedKStream]`<K,V>` | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `sliding`.                                                                                                                                                                                                       |
|                           |                                       | timeDifference | [Duration] | The time difference parameter for the [SlidingWindows] object.                                                                                                                                                               |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [SlidingWindows] object.                                                                                                                                                              |
| [CogroupedKStream]`<K,V>` | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `hopping`.                                                                                                                                                                                                       |
|                           |                                       | advanceBy      | [Duration] | The amount by which each window is advanced. If this value is not specified, then it will be equal to _duration_, which gives tumbling windows. If you make this value smaller than _duration_ you will get hopping windows. |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [CogroupedKStream]`<K,V>` | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `tumbling`.                                                                                                                                                                                                      |
|                           |                                       | duration       | [Duration] | The duration parameter for the [TimeWindows] object.                                                                                                                                                                         |
|                           |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |

Example:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: groupBy
        mapper: my_mapper_function
      - type: windowedByTime
        windowType: tumbling
        duration: 1h
        advanceBy: 15m
        grace: 5m
      - type: reduce
        reducer: my_reducer_function
      - type: toStream
    to: output_stream
```

## Sink Operations

### as

Pipelines closed of with `as` can be referred by other pipelines as their starting reference. This allows for a common
part of processing logic to be placed in its own pipeline in KSML, serving as an intermediate result.

| Applies to          | Value Type | Required | Description                                                                    |
|:--------------------|:-----------|:---------|:-------------------------------------------------------------------------------|
| Any pipeline`<K,V>` | string     | Yes      | The name under which the pipeline result can be referenced by other pipelines. |

Example:

```yaml
pipelines:
  first:
    from: some_source_topic
    via:
      - type: ...
    as: first_pipeline

  second:
    from: first_pipeline
    via:
      - type: ...
    to: ...
```

Here, the first pipeline ends by sending its output to a stream internally called `first_pipeline`. This stream is used
as input for the `second` pipeline.

### branch

Branches out messages from the input stream into several branches based on predicates. Each branch is defined as a list
item below the branch operation. Branch predicates are defined using the `if` keyword. Messages are only processed by
one of the branches, namely the first one for which the predicate returns `true`.

| Applies to       | Value Type                 | Required | Description                                      |
|:-----------------|:---------------------------|:---------|:-------------------------------------------------|
| [KStream]`<K,V>` | List of branch definitions | Yes      | See for description of branch definitions below. |

Branches in KSML are nested pipelines, which are parsed without the requirement of a source attribute. Each branch
accepts the following parameters:

| Branch element | Value Type                              | Required | Description                                                                                                                   |
|:---------------|:----------------------------------------|:---------|:------------------------------------------------------------------------------------------------------------------------------|
| `if`           | Inline [Predicate] or reference         | No       | The [Predicate] function that determines if the message is sent down this branch, or is passed on to the next branch in line. |
| _Inline_       | All pipeline parameters, see [Pipeline] | Yes      | The inlined pipeline describes the topology of the specific branch.                                                           |

Example:

```yaml
pipelines:
  example_pipeline:
    from: some_source_topic
    branch:
      - if:
          expression: value['color'] == 'blue'
        to: ksml_sensordata_blue
      - if:
          expression: value['color'] == 'red'
        to: ksml_sensordata_red
      - forEach:
          code: |
            log.warn('Unknown color sensor: {}', value["color"])
```

In this example, the first two branches are entered if the respective predicate matches (the color attribute of value
matches a certain color).
If the predicate returns `false`, then the next predicate/branch is tried. Only the last branch in the list can be a
sink operation.

### forEach

This sends each message to a custom defined function. This function is expected to handle each message as its final
step. The function does not (need to) return anything.

| Applies to       | Value Type          | Description                                                                                                                               |
|:-----------------|:--------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | Inline or reference | The [ForEach] function that is called for every record on the source stream. Its arguments are `key` of type `K` and `value` of type `V`. |

Examples:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    forEach: my_foreach_function
```

```yaml
pipelines:
  example_pipeline:
    forEach:
      code: |
        log.info("Processing message: key={}, value={}", key, value)
```

### print

This sends each message to a custom defined print function. This function is expected to handle each message as the
final in the pipeline. The function does not (need to) return anything.

As target, you can specify a filename. If none is specified, then all messages are printed to stdout.

| Applies to       | Parameter | Value Type          | Required | Description                                                                                                                                                                      |
|:-----------------|:----------|:--------------------|:---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | filename  | string              | No       | The filename to output records to. If nothing is specified, then messages will be printed on stdout.                                                                             |
|                  | label     | string              | No       | A label to attach to every output record.                                                                                                                                        |
|                  | `mapper`  | Inline or reference | Yes      | A [KeyValueMapper] function, which takes a `key` of type `K` and a `value` of type `V`. The return value should be of type `string` and is sent to the specified file or stdout. |

Examples:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: ...
    print:
      filename: file.txt
      mapper:
        expression: "record value: " + str(value)
```

### to

Messages are sent directly to a named `Stream`.

| Applies to       | Value Type                                                     | Required | Description                                 |
|:-----------------|:---------------------------------------------------------------|:---------|:--------------------------------------------|
| [KStream]`<K,V>` | Inline [Topic] or reference to a stream, table or global table | Yes      | The name of a defined [stream](streams.md). |

Examples:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    to: my_target_stream
```

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    via:
      - type: ...
    to:
      topic: my_target_topic
      keyType: someType
      valueType: someOtherType
      partitioner:
        expression: hash_of(key)
```

### toTopicNameExtractor

Messages are passed onto a user function, which returns the name of the topic that message needs to be sent to. This
operation acts as a Sink and is always the last operation in a [pipeline](pipelines.md).

| Applies to       | Value Type          | Required | Description                                                                                                                          |
|:-----------------|:--------------------|:---------|:-------------------------------------------------------------------------------------------------------------------------------------|
| [KStream]`<K,V>` | Inline or reference | Yes      | The [TopicNameExtractor] function that is called for every message and returns the topic name to which the message shall be written. |

Examples:

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    toTopicNameExtractor: my_extractor_function
```

```yaml
pipelines:
  example_pipeline:
    from: input_stream
    toTopicNameExtractor:
      code: |
        if key == 'sensor1':
          return 'ksml_sensordata_sensor1'
        elif key == 'sensor2':
          return 'ksml_sensordata_sensor2'
        else:
          return 'ksml_sensordata_sensor0'
```
