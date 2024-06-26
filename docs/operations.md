# Operations

### Table of Contents

1. [Introduction](#introduction)
1. [Operations](#transform-operations)
    * [aggregate](#aggregate)
    * [convertKey](#convertkey)
    * [convertKeyValue](#convertkeyvalue)
    * [convertValue](#convertvalue)
    * [count](#count)
    * [filter](#filter)
    * [filterNot](#filternot)
    * [groupBy](#groupby)
    * [groupByKey](#groupbykey)
    * [join](#join)
    * [leftJoin](#leftjoin)
    * [map](#map)
    * [mapKey](#mapkey)
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
1. [Sink Operations](#sink-operations)
    * [branch](#branch)
    * [forEach](#foreach)
    * [to](#to)
    * [toExtractor](#toextractor)

[Duration]: types.md#duration
[Store]: stores.md
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

Pipelines in KSML have a beginning, a middle and (optionally) an end. Operations form the middle part of pipelines. They are modeled as separate YAML entities, where each operation takes input from the previous operation and applies its own logic. The returned stream then serves as input for the next operation.

## Transform Operations

Transformations are operations that take an input stream and convert it to an output stream. This section lists all supported transformations. Each one states the type of stream it returns.

| Parameter | Value Type | Description                               |
|:----------|:-----------|:------------------------------------------|
| `name`    | string     | The name of the transformation operation. |

Note that not all combinations of output/input streams are supported by Kafka Streams. The user that writes the KSML definition needs to make sure that streams that result from one operations can actually serve as input to the next. KSML does type checking and will exit with an error when operations that can not be chained together are listed after another in the KSML definition.

### aggregate

[KGroupedStream::aggregate]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#aggregate-org.apache.kafka.streams.kstream.Initializer-org.apache.kafka.streams.kstream.Aggregator-org.apache.kafka.streams.kstream.Materialized-
[KGroupedTable::aggregate]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html#aggregate-org.apache.kafka.streams.kstream.Initializer-org.apache.kafka.streams.kstream.Aggregator-org.apache.kafka.streams.kstream.Aggregator-org.apache.kafka.streams.kstream.Materialized-
[SessionWindowedKStream::aggregate]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html#aggregate-org.apache.kafka.streams.kstream.Initializer-org.apache.kafka.streams.kstream.Aggregator-org.apache.kafka.streams.kstream.Merger-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[TimeWindowedKStreamObject:aggregate]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html#aggregate-org.apache.kafka.streams.kstream.Initializer-org.apache.kafka.streams.kstream.Aggregator-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

This operations aggregates multiple values into a single one by repeatedly calling an aggregator function. It can operate on a range of stream types.

| Stream Type                                                             | Returns                    | Parameter     | Value Type          | Required | Description                                       |
|:------------------------------------------------------------------------|:---------------------------|:--------------|:--------------------|:---------|:--------------------------------------------------|
| [KGroupedStream][KGroupedStream::aggregate]`<K,V>`                      | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                         |                            | `initializer` | Inline or reference | Yes      | The [Initializer] function.                       |
|                                                                         |                            | `aggregator`  | Inline or reference | Yes      | The [Aggregator] function.                        |
| [KGroupedTable][KGroupedTable::aggregate]`<K,V>`                        | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                         |                            | `initializer` | Inline or reference | Yes      | The [Initializer] function.                       |
|                                                                         |                            | `adder`       | Inline or reference | Yes      | The [Reducer] function that adds two values.      |
|                                                                         |                            | `subtractor`  | Inline or reference | Yes      | The [Reducer] function that subtracts two values. |
| [SessionWindowedKStream][SessionWindowedKStream::aggregate]`<K,V>`      | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                         |                            | `initializer` | Inline or reference | Yes      | The [Initializer] function.                       |
|                                                                         |                            | `aggregator`  | Inline or reference | Yes      | The [Aggregator] function.                        |
|                                                                         |                            | `merger`      | Inline or reference | Yes      | The [Merger] function.                            |
| [TimeWindowedKStreamObject][TimeWindowedKStreamObject:aggregate]`<K,V>` | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                         |                            | `initializer` | Inline or reference | Yes      | The [Initializer] function.                       |
|                                                                         |                            | `aggregator`  | Inline or reference | Yes      | The [Aggregator] function.                        |

Example:
```yaml
from: input_stream
via:
  - type: groupBy
    mapper: my_mapper_function
  - type: aggregate
    initializer:
      expression: 0
    aggregator:
      expression: value1+value2
  - type: toStream
to: output_stream
```

### convertKey

This built-in operation takes a message and converts the key into a given type.

| Stream Type    | Returns         | Parameter | Value Type | Description            |
|:---------------|:----------------|:----------|:-----------|:-----------------------|
| KStream`<K,V>` | KStream`<KR,V>` | `into`    | string     | The type to convert to |

Example:
```yaml
from: input_stream
via:
  - type: convertKey
    into: string
to: output_stream
```

### convertKeyValue

This built-in operation takes a message and converts the key and value into a given type.

| Stream Type    | Returns          | Parameter | Value Type | Description            |
|:---------------|:-----------------|:----------|:-----------|:-----------------------|
| KStream`<K,V>` | KStream`<KR,VR>` | `into`    | string     | The type to convert to |

Example:
```yaml
from: input_stream
via:
  - type: convertKeyValue
    into: (string,avro:SensorData)
to: output_stream
```

### convertValue

This built-in operation takes a message and converts the value into a given type.

| Stream Type    | Returns         | Parameter | Value Type | Description            |
|:---------------|:----------------|:----------|:-----------|:-----------------------|
| KStream`<K,V>` | KStream`<KR,V>` | `into`    | string     | The type to convert to |

Example:
```yaml
from: input_stream
via:
  - type: convertValue
    into: avro:SensorData
to: output_stream
```

### count

[KGroupedStream::count]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#count-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[KGroupedTable::count]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html#count-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[SessionWindowedKStream::count]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html#count-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[TimeWindowedKStreamObject:count]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html#count-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

This operations counts the number of messages and returns a table  multiple values into a single one by repeatedly calling an aggregator function. It can operate on a range of stream types.

| Stream Type                                                         | Returns                      | Parameter | Value Type          | Required | Description                |
|:--------------------------------------------------------------------|:-----------------------------|:----------|:--------------------|:---------|:---------------------------|
| [KGroupedStream][KGroupedStream::count]`<K,V>`                      | [KTable]`<K,Long>`           | `store`   | Store configuration | No       | The [Store] configuration. |
| [KGroupedTable][KGroupedTable::count]`<K,V>`                        | [KTable]`<K,Long>`           | `store`   | Store configuration | No       | The [Store] configuration. |
| [SessionWindowedKStream][SessionWindowedKStream::count]`<K,V>`      | [KTable]`<Windowed<K>,Long>` | `store`   | Store configuration | No       | The [Store] configuration. |
| [TimeWindowedKStreamObject][TimeWindowedKStreamObject:count]`<K,V>` | [KTable]`<Windowed<K>,Long>` | `store`   | Store configuration | No       | The [Store] configuration. |

Example:
```yaml
from: input_stream
via:
  - type: groupBy
    mapper: my_mapper_function
  - type: count
  - type: toStream
to: output_stream
```

### filter

[KStream::filter]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#filter-org.apache.kafka.streams.kstream.Predicate-org.apache.kafka.streams.kstream.Named-
[KTable::filter]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#filter-org.apache.kafka.streams.kstream.Predicate-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

Filter all incoming messages according to some predicate. The predicate function is called for every message. Only when the predicate returns `true`, then the message will be sent to the output stream.

| Stream Type                       | Returns          | Parameter   | Value Type | Required            | Description               |
|:----------------------------------|:-----------------|:------------|:-----------|:--------------------|:--------------------------|
| [KStream][KStream::filter]`<K,V>` | [KStream]`<K,V>` | `predicate` | Yes        | Inline or reference | The [Predicate] function. |
| [KTable][KTable::filter]`<K,V>`   | [KTable]`<K,V>`  | `predicate` | Yes        | Inline or reference | The [Predicate] function. |

Example:
```yaml
from: input_stream
via:
  - type: filter
    predicate: my_filter_function
  - type: filter
    predicate:
      expression: key.startswith('a')
to: output_stream
```

### filterNot

This transformation works exactly like [filter](#filter), but negates all predicates before applying them. See [filter](#filter) for details on how to implement.

### groupBy

[KStream::groupBy]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#groupBy-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.Grouped-
[KTable::groupBy]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#groupBy-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.Grouped-

Group the records of a stream on a new key that is selected using the provided KeyValueMapper.

| Stream Type                        | Returns                 | Parameter | Value Type          | Required | Description                    |
|:-----------------------------------|:------------------------|:----------|:--------------------|:---------|:-------------------------------|
| [KStream][KStream::groupBy]`<K,V>` | [KGroupedStream]`<K,V>` | `store`   | Store configuration | No       | The [Store] configuration.     |
|                                    |                         | `mapper`  | Inline or reference | Yes      | The [KeyValueMapper] function. |
| [KTable][KTable::groupBy]`<K,V>`   | [KGroupedTable]`<K,V>`  | `store`   | Store configuration | No       | The [Store] configuration.     |
|                                    |                         | `mapper`  | Inline or reference | Yes      | The [KeyValueMapper] function. |

Example:
```yaml
from: input_stream
via:
  - type: groupBy
    mapper: my_mapper_function
  - type: aggregate
    initializer:
      expression: 0
    aggregator:
      expression: value1+value2
  - type: toStream
to: output_stream
```

### groupByKey

[KStream::groupByKey]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#groupByKey--

Group the records of a stream on the stream's key.

| Stream Type                           | Returns                 | Parameter | Value Type          | Required | Description                    |
|:--------------------------------------|:------------------------|:----------|:--------------------|:---------|:-------------------------------|
| [KStream][KStream::groupByKey]`<K,V>` | [KGroupedStream]`<K,V>` | `store`   | Store configuration | No       | The [Store] configuration.     |
|                                       |                         | `mapper`  | Inline or reference | Yes      | The [KeyValueMapper] function. |

Example:
```yaml
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

[KStream::joinStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-org.apache.kafka.streams.kstream.StreamJoined-
[KStream::joinTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Joined-
[KStream::joinGlobalTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.GlobalKTable-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Named-
[KTable::joinTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#join-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

Join records of this stream with another stream's records using windowed inner equi join. The join is computed on the records' key with join predicate `thisKStream.key == otherKStream.key`. Furthermore, two records are only joined if their timestamps are close to each other as defined by the given JoinWindows, i.e., the window defines an additional join predicate on the record timestamps.

| Stream Type                                | Returns          | Parameter     | Value Type          | Required | Description                                |
|:-------------------------------------------|:-----------------|:--------------|:--------------------|:---------|:-------------------------------------------|
| [KStream][KStream::joinStream]`<K,V>`      | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                            |                  | `stream`      | `string`            | Yes      | The name of the stream to join with.       |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                            |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KStream][KStream::joinTable]`<K,V>`       | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                            |                  | `table`       | `string`            | Yes      | The name of the table to join with.        |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                            |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KStream][KStream::joinGlobalTable]`<K,V>` | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                            |                  | `globalTable` | `string`            | Yes      | The name of the global table to join with. |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                            |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KTable][KTable::joinTable]`<K,V>`         | [KTable]`<K,V>`  | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                            |                  | `table`       | `string`            | Yes      | The name of the table to join with.        |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |

Example:
```yaml
from: input_stream
via:
  - type: join
    stream: second_stream
    valueJoiner: my_key_value_mapper
    duration: 1m
to: output_stream
```

### leftJoin

[KStream::leftJoinStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#leftJoin-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-org.apache.kafka.streams.kstream.StreamJoined-
[KStream::leftJoinTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#leftJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Joined-
[KStream::leftJoinGlobalTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.GlobalKTable-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Named-
[KTable::leftJoinTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#leftJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

Join records of this stream with another stream's records using windowed left equi join. In contrast to inner-join, all records from this stream will produce at least one output record. The join is computed on the records' key with join attribute thisKStream.key == otherKStream.key. Furthermore, two records are only joined if their timestamps are close to each other as defined by the given JoinWindows, i.e., the window defines an additional join predicate on the record timestamps.

| Stream Type                                    | Returns          | Parameter     | Value Type          | Required | Description                                |
|:-----------------------------------------------|:-----------------|:--------------|:--------------------|:---------|:-------------------------------------------|
| [KStream][KStream::leftJoinStream]`<K,V>`      | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                                |                  | `stream`      | `string`            | Yes      | The name of the stream to join with.       |
|                                                |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                                |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KStream][KStream::leftJoinTable]`<K,V>`       | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                                |                  | `table`       | `string`            | Yes      | The name of the table to join with.        |
|                                                |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                                |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KStream][KStream::leftJoinGlobalTable]`<K,V>` | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                                |                  | `globalTable` | `string`            | Yes      | The name of the global table to join with. |
|                                                |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |
|                                                |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join.     |
| [KTable][KTable::leftJoinTable]`<K,V>`         | [KTable]`<K,V>`  | `store`       | Store configuration | No       | The [Store] configuration.                 |
|                                                |                  | `table`       | `string`            | Yes      | The name of the table to join with.        |
|                                                |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.             |

Example:
```yaml
[yaml]
----
from: input_stream
via:
  - type: leftJoin
    stream: second_stream
    valueJoiner: my_join_function
    duration: 1m
to: output_stream
```

### map

This is an alias for [transformKeyValue](#transformkeyvalue).

### mapKey

This is an alias for [transformKey](#transformkey).

### mapValues

This is an alias for [transformValue](#transformvalue).

### merge

[KStream::merge]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#merge-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.Named-

Merge this stream and the given stream into one larger stream. There is no ordering guarantee between records from this stream and records from the provided stream in the merged stream. Relative order is preserved within each input stream though (ie, records within one input stream are processed in order).

| Stream Type                      | Returns          | Parameter | Value Type | Description                           |
|:---------------------------------|:-----------------|:----------|:-----------|:--------------------------------------|
| [KStream][KStream::merge]`<K,V>` | [KStream]`<K,V>` | `stream`  | `string`   | The name of the stream to merge with. |

Example:
```yaml
from: input_stream
via:
  - type: merge
    stream: second_stream
to: output_stream
```

### outerJoin

[KStream::outerJoinStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#outerJoin-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-org.apache.kafka.streams.kstream.StreamJoined-
[KTable::outerJoinTable]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#outerJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

Join records of this stream with another stream's records using windowed outer equi join. In contrast to inner-join or left-join, all records from both streams will produce at least one output record. The join is computed on the records' key with join attribute thisKStream.key == otherKStream.key. Furthermore, two records are only joined if their timestamps are close to each other as defined by the given JoinWindows, i.e., the window defines an additional join predicate on the record timestamps.

| Stream Type                                | Returns          | Parameter     | Value Type          | Required | Description                            |
|:-------------------------------------------|:-----------------|:--------------|:--------------------|:---------|:---------------------------------------|
| [KStream][KStream::outerJoinStream]`<K,V>` | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.             |
|                                            |                  | `stream`      | `string`            | Yes      | The name of the stream to join with.   |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.         |
|                                            |                  | `duration`    | `string`            | Yes      | The [Duration] of the windows to join. |
| [KTable][KTable::outerJoinTable]`<K,V>`    | [KTable]`<K,V>`  | `store`       | Store configuration | No       | The [Store] configuration.             |
|                                            |                  | `table`       | `string`            | Yes      | The name of the table to join with.    |
|                                            |                  | `valueJoiner` | Inline or reference | Yes      | The [KeyValueMapper] function.         |

Example:
```yaml
[yaml]
----
from: input_stream
via:
  - type: outerJoin
    stream: second_stream
    valueJoiner: my_join_function
    duration: 1m
to: output_stream
```

### peek

[KStream::peek]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#peek-org.apache.kafka.streams.kstream.ForeachAction-org.apache.kafka.streams.kstream.Named-

Perform an action on each record of a stream. This is a stateless record-by-record operation. Peek is a non-terminal operation that triggers a side effect (such as logging or statistics collection) and returns an unchanged stream.

| Stream Type                     | Returns          | Parameter | Value Type          | Description                                                   |
|:--------------------------------|:-----------------|:----------|:--------------------|:--------------------------------------------------------------|
| [KStream][KStream::peek]`<K,V>` | [KStream]`<K,V>` | `forEach` | Inline or reference | The [ForEach] function that will be called for every message. |

Example:
```yaml
from: input_stream
via:
  - type: peek
    forEach: print_key_and_value
to: output_stream
```

### reduce

[KGroupedStream::reduce]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#reduce-org.apache.kafka.streams.kstream.Reducer-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[KGroupedTable::reduce]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html#reduce-org.apache.kafka.streams.kstream.Reducer-org.apache.kafka.streams.kstream.Reducer-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[SessionWindowedKStream::reduce]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html#reduce-org.apache.kafka.streams.kstream.Reducer-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-
[TimeWindowedKStreamObject:reduce]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html#reduce-org.apache.kafka.streams.kstream.Reducer-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Materialized-

Combine the values of records in this stream by the grouped key. Records with null key or value are ignored. Combining implies that the type of the aggregate result is the same as the type of the input value, similar to [aggregate(Initializer, Aggregator)](#aggregate).

| Stream Type                                                          | Returns                    | Parameter     | Value Type          | Required | Description                                       |
|:---------------------------------------------------------------------|:---------------------------|:--------------|:--------------------|:---------|:--------------------------------------------------|
| [KGroupedStream][KGroupedStream::reduce]`<K,V>`                      | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                      |                            | `reducer`     | Inline or reference | Yes      | The [Reducer] function.                           |
| [KGroupedTable][KGroupedTable::reduce]`<K,V>`                        | [KTable]`<K,VR>`           | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                      |                            | `adder`       | Inline or reference | Yes      | The [Reducer] function that adds two values.      |
|                                                                      |                            | `subtractor`  | Inline or reference | Yes      | The [Reducer] function that subtracts two values. |
| [SessionWindowedKStream][SessionWindowedKStream::reduce]`<K,V>`      | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                      |                            | `reducer`     | Inline or reference | Yes      | The [Reducer] function.                           |
| [TimeWindowedKStreamObject][TimeWindowedKStreamObject:reduce]`<K,V>` | [KTable]`<Windowed<K>,VR>` | `store`       | Store configuration | No       | The [Store] configuration.                        |
|                                                                      |                            | `initializer` | Inline or reference | Yes      | The [Reducer] function.                           |

Example:
```yaml
[yaml]
----
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

[KStream::repartition]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#repartition-org.apache.kafka.streams.kstream.Repartitioned-

Materialize this stream to an auto-generated repartition topic and create a new KStream from the auto-generated topic using key serde, value serde, StreamPartitioner, number of partitions, and topic name part.
The created topic is considered as an internal topic and is meant to be used only by the current Kafka Streams instance. Similar to auto-repartitioning, the topic will be created with infinite retention time and data will be automatically purged by Kafka Streams. The topic will be named as "${applicationId}-<name>-repartition", where "applicationId" is user-specified in StreamsConfig via parameter APPLICATION_ID_CONFIG, "<name>" is either provided via Repartitioned.as(String) or an internally generated name, and "-repartition" is a fixed suffix.

| Stream Type                            | Returns          | Parameter     | Value Type          | Required | Description                                                    |
|:---------------------------------------|:-----------------|:--------------|:--------------------|:---------|:---------------------------------------------------------------|
| [KStream][KStream::repartition]`<K,V>` | [KStream]`<K,V>` | `store`       | Store configuration | No       | The [Store] configuration.                                     |
|                                        |                  | `name`        | `string`            | Yes      | The name used as part of repartition topic and processor name. |
|                                        |                  | `partitioner` | Inline or reference | Yes      | The [StreamPartitioner] function.                              |

Example:
```yaml
from: input_stream
via:
  - type: repartition
    name: my_repartitioner
    partitioner: my_own_partitioner
  - type: peek
    forEach: print_key_and_value
  - type: toStream
to: output_stream
```

### selectKey

This is an alias for [transformKey](#transformkey).

### suppress

[KTable::suppress]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#suppress-org.apache.kafka.streams.kstream.Suppressed-

Suppress some updates from this changelog stream, determined by the supplied Suppressed configuration. When
_windowCloses_ is selected and no further restrictions are provided, then this is interpreted as _Suppressed.untilWindowCloses(unbounded())_.

| Stream Type                       | Returns         | Parameter            | Value Type | Description                                                                                                                                                                                                                      |
|:----------------------------------|:----------------|:---------------------|:-----------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KTable][KTable::suppress]`<K,V>` | [KTable]`<K,V>` | `until`              | `string`   | This value can either be `timeLimit` or `windowCloses`. Note that _timeLimit_ suppression works on any stream, while _windowCloses_ suppression works only on _Windowed_ streams. For the latter, see [windowedBy](#windowedby). |
|                                   |                 | `duration`           | `string`   | The [Duration] to suppress updates (only when `until`==`timeLimit`)                                                                                                                                                              |
|                                   |                 | `maxBytes`           | `int`      | (Optional) The maximum number of bytes to suppress updates                                                                                                                                                                       |
|                                   |                 | `maxRecords`         | `int`      | (Optional) The maximum number of records to suppress updates                                                                                                                                                                     |
|                                   |                 | `bufferFullStrategy` | `string`   | (Optional) Can be one of `emitEarlyWhenFull`, `shutdownWhenFull`                                                                                                                                                                 |

Example:
```yaml
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

[KTable::toStream]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#toStream--

Convert a KTable into a KStream object.

| Stream Type                       | Returns          | Parameter | Value Type          | Description                                                                           |
|:----------------------------------|:-----------------|:----------|:--------------------|:--------------------------------------------------------------------------------------|
| [KTable][KTable::toStream]`<K,V>` | [KStream]`<K,V>` | `mapper`  | Inline or reference | (Optional)The [KeyValueMapper] function. If no mapper is specified, `K` will be used. |

Example:
```yaml
from: input_table
via:
  - type: toStream
to: output_stream
```

### transformKey

[KStream::transformKey]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#selectKey-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.Named-

This operation takes a message and transforms the key into a new key, which can be potentially of different type.

| Stream Type                             | Returns           | Parameter | Value Type          | Description                                |
|:----------------------------------------|:------------------|:----------|:--------------------|:-------------------------------------------|
| [KStream][KStream::transformKey]`<K,V>` | [KStream]`<KR,V>` | `mapper`  | Inline or reference | The [KeyValueMapper] function.             |
|                                         |                   | `name`    | `string`            | (Optional) The name of the processor node. |

Example:
```yaml
from: input_stream
via:
  - type: transformKey
    mapper:
      expression: key=str(key)   # convert key from Integer to String
to: output_stream
```

### transformKeyValue

[KStream::transformKeyValue]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#map-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.Named-

This operation takes a message and transforms the key and value into a new key and value, which can each be potentially of different type.

| Stream Type                                  | Returns            | Parameter | Value Type          | Description                                |
|:---------------------------------------------|:-------------------|:----------|:--------------------|:-------------------------------------------|
| [KStream][KStream::transformKeyValue]`<K,V>` | [KStream]`<KR,VR>` | `mapper`  | Inline or reference | The [KeyValueMapper] function.             |
|                                              |                    | `name`    | `string`            | (Optional) The name of the processor node. |

Example:
```yaml
from: input_stream
via:
  - type: transformKeyValue
    mapper:
      expression: (str(key), str(value))   # convert key and value from Integer to String
to: output_stream
```

### transformKeyValueToKeyValueList

[KStream::transformKeyValueToKeyValueList]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMap-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.Named-

This operation takes a message and transforms it into zero, one or more new messages, which can be potentially of different type.

| Stream Type                                                | Returns            | Parameter | Value Type          | Description                                       |
|:-----------------------------------------------------------|:-------------------|:----------|:--------------------|:--------------------------------------------------|
| [KStream][KStream::transformKeyValueToKeyValueList]`<K,V>` | [KStream]`<KR,VR>` | `mapper`  | Inline or reference | The [KeyValueToKeyValueListTransformer] function. |
|                                                            |                    | `name`    | `string`            | (Optional) The name of the processor node.        |

Example:
```yaml
from: input_stream
via:
  - type: transformKeyValueToKeyValueList
    mapper:
    expression: [(key,value),(key,value)]   # duplicate all incoming messages
to: output_stream
```

### transformKeyValueToValueList

[KStream::transformKeyValueToValueList]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMapValues-org.apache.kafka.streams.kstream.ValueMapperWithKey-org.apache.kafka.streams.kstream.Named-

This operation takes a message and generates a new list of values for the key, which can be potentially of different type.

| Stream Type                                             | Returns           | Parameter | Value Type          | Description                                    |
|:--------------------------------------------------------|:------------------|:----------|:--------------------|:-----------------------------------------------|
| [KStream][KStream::transformKeyValueToValueList]`<K,V>` | [KStream]`<K,VR>` | `mapper`  | Inline or reference | The [KeyValueToValueListTransformer] function. |
|                                                         |                   | `name`    | `string`            | (Optional) The name of the processor node.     |

Example:
```yaml
from: input_stream
via:
  - type: transformKeyValueToValueList
    mapper:
      expression: [value+1,value+2,value+3]   # creates 3 new messages [key,VR] for every input message
to: output_stream
```

### transformValue

[KStream::transformValue]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMapValues-org.apache.kafka.streams.kstream.ValueMapperWithKey-org.apache.kafka.streams.kstream.Named-

This operation takes a message and transforms the value into a new value, which can be potentially of different type.

| Stream Type                               | Returns           | Parameter | Value Type          | Required | Description                                       |
|:------------------------------------------|:------------------|:----------|:--------------------|:---------|:--------------------------------------------------|
| [KStream][KStream::transformValue]`<K,V>` | [KStream]`<K,VR>` | `store`   | Store configuration | No       | The [Store] configuration.                        |
|                                           |                   | `mapper`  | Inline or reference | Yes      | The [KeyValueToKeyValueListTransformer] function. |
|                                           |                   | `name`    | `string`            | No       | The name of the processor node.                   |

Example:
```yaml
from: input_stream
via:
  - type: transformValue
    mapper:
      expression: value=str(key)   # convert value from Integer to String
to: output_stream
```

### windowBySession

[KGroupedStream::windowedBy]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#windowedBy-org.apache.kafka.streams.kstream.SessionWindows-
[CogroupedKStream::windowedBySession]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/CogroupedKStream.html#windowedBy-org.apache.kafka.streams.kstream.SessionWindows-
[SessionWindows]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SessionWindows.html
[WindowTypes]: https://kafka.apache.org/37/documentation/streams/developer-guide/dsl-api.html#windowing

Create a new windowed KStream instance that can be used to perform windowed aggregations. For more details on the different types of windows, please refer to [WindowTypes]|[this page].

| Stream Type                                           | Returns                                  | Parameter      | Value Type | Description                                                                                                                                                                                                                             |
|:------------------------------------------------------|:-----------------------------------------|:---------------|:-----------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream][KGroupedStream::windowedBySession]   | [SessionWindowedKStream]`<K,V>`          | inactivityGap  | [Duration] | The inactivity gap parameter for the [SessionWindows] object.                                                                                                                                                                           |
|                                                       |                                          | grace          | [Duration] | (Optional) The grace parameter for the [SessionWindows] object.                                                                                                                                                                         |
| [CogroupedKStream][CogroupedKStream::windowedBySession] | [SessionWindowedCogroupedKStream]`<K,V>` | inactivityGap  | [Duration] | The inactivity gap parameter for the [SessionWindows] object.                                                                                                                                                                           |
|                                                       |                                          | grace          | [Duration] | (Optional) The grace parameter for the [SessionWindows] object.                                                                                                                                                                         |

Example:
```yaml
from: input_stream
via:
  - type: groupBy
    mapper: my_mapper_function
  - type: windowedBy
    windowType: time
    duration: 1h
    advanceBy: 15m
    grace: 5m
  - type: reduce
    reducer: my_reducer_function
  - type: toStream
to: output_stream
```
### windowByTime

[KGroupedStream::windowedBySliding]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#windowedBy-org.apache.kafka.streams.kstream.SlidingWindows-
[KGroupedStream::windowedByDuration]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#windowedBy-org.apache.kafka.streams.kstream.Windows-
[SlidingWindows]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/SlidingWindows.html
[TimeWindows]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindows.html
[WindowTypes]: https://kafka.apache.org/37/documentation/streams/developer-guide/dsl-api.html#windowing

Create a new windowed KStream instance that can be used to perform windowed aggregations. For more details on the different types of windows, please refer to [WindowTypes]|[this page].

| Stream Type                                              | Returns                               | Parameter      | Value Type | Description                                                                                                                                                                                                                  |
|:---------------------------------------------------------|:--------------------------------------|:---------------|:-----------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KGroupedStream][KGroupedStream::windowedBySliding]      | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `sliding`.                                                                                                                                                                                                       |
|                                                          |                                       | timeDifference | [Duration] | The time difference parameter for the [SlidingWindows] object.                                                                                                                                                               |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [SlidingWindows] object.                                                                                                                                                              |
| [KGroupedStream][KGroupedStream::windowedByDuration]     | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `hopping`.                                                                                                                                                                                                       |
|                                                          |                                       | advanceBy      | [Duration] | The amount by which each window is advanced. If this value is not specified, then it will be equal to _duration_, which gives tumbling windows. If you make this value smaller than _duration_ you will get hopping windows. |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [KGroupedStream][KGroupedStream::windowedByDuration]     | [TimeWindowedKStream]`<K,V>`          | `windowType`   | `string`   | Fixed value `tumbling`.                                                                                                                                                                                                      |
|                                                          |                                       | duration       | [Duration] | The duration parameter for the [TimeWindows] object.                                                                                                                                                                         |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [CogroupedKStream][CogroupedKStream::windowedBySliding]  | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `sliding`.                                                                                                                                                                                                       |
|                                                          |                                       | timeDifference | [Duration] | The time difference parameter for the [SlidingWindows] object.                                                                                                                                                               |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [SlidingWindows] object.                                                                                                                                                              |
| [CogroupedKStream][CogroupedKStream::windowedByDuration] | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `hopping`.                                                                                                                                                                                                       |
|                                                          |                                       | advanceBy      | [Duration] | The amount by which each window is advanced. If this value is not specified, then it will be equal to _duration_, which gives tumbling windows. If you make this value smaller than _duration_ you will get hopping windows. |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |
| [CogroupedKStream][CogroupedKStream::windowedByDuration] | [TimeWindowedCogroupedKStream]`<K,V>` | `windowType`   | `string`   | Fixed value `tumbling`.                                                                                                                                                                                                      |
|                                                          |                                       | duration       | [Duration] | The duration parameter for the [TimeWindows] object.                                                                                                                                                                         |
|                                                          |                                       | grace          | [Duration] | (Optional) The grace parameter for the [TimeWindows] object.                                                                                                                                                                 |

Example:
```yaml
from: input_stream
via:
  - type: groupBy
    mapper: my_mapper_function
  - type: windowedBy
    windowType: time
    duration: 1h
    advanceBy: 15m
    grace: 5m
  - type: reduce
    reducer: my_reducer_function
  - type: toStream
to: output_stream
```


## Sink Operations

### branch

[KStream::branch]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#branch-org.apache.kafka.streams.kstream.Named-org.apache.kafka.streams.kstream.Predicate...-

Branches out messages from the input stream into several branches based on predicates. Each branch is defined as a list item below the branch operation. Branch predicates are defined using the `if` keyword. Messages are only processed by one of the branches, namely the first one for which the predicate returns `true`.

| Applies to                        | Value Type                 | Description                                      |
|:----------------------------------|:---------------------------|:-------------------------------------------------|
| [KStream][KStream::branch]`<K,V>` | List of branch definitions | See for description of branch definitions below. |

Branches in KSML are nested pipelines, which are parsed without the requirement of a source attribute. Each branch accepts the following parameters:

| Branch element | Value Type                              | Description                                                                                                                   |
|:---------------|:----------------------------------------|:------------------------------------------------------------------------------------------------------------------------------|
| `if`           | Inline [Predicate] or reference         | The [Predicate] function that determines if the message is sent down this branch, or is passed on to the next branch in line. |
| _Inline_       | All pipeline parameters, see [Pipeline] | The inlined pipeline describes the topology of the specific branch.                                                           |

Example:
```yaml
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
        print('Unknown color sensor: '+str(value))
```

In this example, the first two branches are entered if the respective predicate matches (the color attribute of value matches a certain color).
If the predicate returns `false`, then the next predicate/branch is tried. Only the last branch in the list can be a sink operation.

### forEach

[KStream::forEach]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#foreach-org.apache.kafka.streams.kstream.ForeachAction-org.apache.kafka.streams.kstream.Named-

This sends each message to a custom defined function. This function is expected to handle each message as its final step. The function does not (need to) return anything.

| Applies to                         | Value Type          | Description                                                                  |
|:-----------------------------------|:--------------------|:-----------------------------------------------------------------------------|
| [KStream][KStream::forEach]`<K,V>` | Inline or reference | The [ForEach] function that is called for every record on the source stream. |

Examples:
```yaml
forEach: my_print_function
```

```yaml
forEach:
  code: print(value)
```

### to

[KStream::to]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#to-java.lang.String-org.apache.kafka.streams.kstream.Produced-

Messages are sent directly to a named `Stream`.

| Applies to                    | Value Type | Description                                 |
|:------------------------------|:-----------|:--------------------------------------------|
| [KStream][KStream::to]`<K,V>` | `string`   | The name of a defined [Stream](streams.md). |

Example:
```yaml
to: my_target_topic
```

### toExtractor

[KStream::toExtractor]: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#to-org.apache.kafka.streams.processor.TopicNameExtractor-org.apache.kafka.streams.kstream.Produced-

Messages are passed onto a user function, which returns the name of the topic that message needs to be sent to. This operation acts as a Sink and is always the last operation in a [pipeline](pipelines.md).

| Applies to                             | Value Type          | Description                                                                                                                          |
|:---------------------------------------|:--------------------|:-------------------------------------------------------------------------------------------------------------------------------------|
| [KStream][KStream::toExtractor]`<K,V>` | Inline or reference | The [TopicNameExtractor] function that is called for every message and returns the topic name to which the message shall be written. |

Examples:
```yaml
toExtractor: my_extractor_function
```

```yaml
toExtractor:
  code: |
    if key == 'sensor1':
      return 'ksml_sensordata_sensor1'
    elif key == 'sensor2':
      return 'ksml_sensordata_sensor2'
    else:
      return 'ksml_sensordata_sensor0'
```
