[<< Back to index](index.md)

# State Stores

[Duration]: types.md#duration

## Introduction

Several stream operations use state stores to retain (intermediate) results of their calculations.
State stores are typically configured with some additional parameters to limit their data storage
(retention time) and/or determine when data is emitted from them to the next stream
operation.

```yaml
stores:
  owner_count_store:
    name: owner_count
    retention: 3m
    caching: false
```

## Configuration

State store configurations are defined by the following tags:

|Parameter|Value Type|Default|Description
|:---|:---|:---|:---
|`name`|`string`|_none_|The name of the state store. This field is not mandatory, but operations that use the state store configuration will require a name for their store. If the store configuration does not specify an explicit name, then the operation will default back to the operation's name, specified with its `name` attribute. If that name is unspecified, then an exception will be thrown. In general, it is considered good practice to always specify the store name explicitly with its definition.
|`retention`|[Duration]|_infinite_|The retention time of entries kept in the state store. By default all entries are stored infinitely, but you may want to limit this to keep storage requirements under control.
|`caching`|`boolean`|`true`|This parameter controls whether the state store caches entries. If you disable caching, then all changes to the state store will be emitted immediately. When cache is enabled then only some changes lead to updated aggregations/calculations, based on technical Kafka Streams configuration such as cache size and topic commit frequency.

Example:
```yaml
stores:
  owner_count_store:
    name: owner_count
    retention: 3m
    caching: false

pipelines:
  main:
    from: sensor_source
    via:
      - type: groupBy
        name: ksml_sensordata_grouped
        mapper:
          expression: value["owner"]
          resultType: string
      - type: windowedBy
        windowType: time
        duration: 20s
        grace: 40s
      - type: aggregate
        store: owner_count_store     # refer to predefined store configuration above
        initializer:
          expression: 0
          resultType: long
        aggregator:
          expression: aggregatedValue+1
          resultType: long
      ...
```

Instead of referring to predefined state store configurations, you may also use an inline definition for the store:
```yaml
      - type: aggregate
        store:
          name: owner_count
          retention: 3m
          caching: false
        initializer:
          expression: 0
          resultType: long
        aggregator:
          expression: aggregatedValue+1
          resultType: long
      ...
```
