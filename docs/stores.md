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

| Parameter     | Value Type | Default | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|:--------------|:-----------|:--------|:---------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`        | `string`   | _none_  | Optional | The name of the state store. This field is not mandatory, but operations that use the state store configuration will require a name for their store. If the store configuration does not specify an explicit name, then the operation will default back to the operation's name, specified with its `name` attribute. If that name is unspecified, then an exception will be thrown. In general, it is considered good practice to always specify the store name explicitly with its definition. |
| `type`        | `string`   | _none_  | Required | The type of the state store. Possible types are `keyValue`, `session` and `window`.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `persistent`  | `boolean`  | `false` | Optional | `true` if the state store should be retained on disk. See [link] for more information on how Kafka Streams maintains state store state in a state directory. When this parameter is false or undefined, the state store is (re)built up in memory during upon KSML start.                                                                                                                                                                                                                        |
| `timestamped` | `boolean`  | `false` | Optional | (Only relevant for keyValue and window stores) `true` if all messages in the state store need to be timestamped. This effectively changes the state store from type <key, value> to <key, timestamp+value>. The timestamp contains the last timestamp that updated the aggregated value in the window.                                                                                                                                                                                           |
| `versioned`   | `boolean`  | `false` | Optional | (Only relevant for keyValue stores) `true` if elements in the store are versioned, `false` otherwise                                                                                                                                                                                                                                                                                                                                                                                             |
| `keyType`     | `string`   | _none_  | Required | The key type of the state store. See [Types](types.md) for more information.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `valueType`   | `string`   | _none_  | Required | The value type of the state store. See [Types](types.md) for more information.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `caching`     | `boolean`  | `false` | Optional | This parameter controls the internal state store caching. When _true_, the state store caches entries and does not emit every state change but only. When _false_ all changes to the state store will be emitted immediately.                                                                                                                                                                                                                                                                    |
| `logging`     | `boolean`  | `false` | Optional | This parameter determines whether state changes are written out to a changelog topic, or not. When _true_ all state store changes are produced to a changelog topic. When _false_ no changelog topic is written to.                                                                                                                                                                                                                                                                              |

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
```
