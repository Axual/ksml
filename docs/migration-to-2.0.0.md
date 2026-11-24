# Upgrading to KSML 2.0.0

KSML 2.0.0 is a major release. It upgrades the core libraries that KSML is built on: Jackson (2 to 3), Apicurio Registry (2 to 3), Protobuf (4.x), Wire (6.x) and the Confluent serdes (8.x). It also upgrades Kafka Streams and the Kafka clients from 4.3 to 4.4.0. And it upgrades GraalVM from 25.0.2 to 25.5.5.0.3, which moves your Python code from Python 3.12 to 3.13.

Most of these upgrades are internal, but a few of them change how you configure KSML. Please read this page before you upgrade a running deployment.

This guide is written as "what changed" followed by "what you need to do".

## Quick checklist

Before you upgrade, check each item below:

* Update every Apicurio registry URL to the `/apis/registry/v3` endpoint.
* Rename the Apicurio basic-auth keys from `apicurio.auth.*` to `apicurio.registry.auth.*`.
* Replace any `apicurio.registry.auto-register.if-exists: RETURN` with a valid v3 value such as `FIND_OR_CREATE_VERSION`.
* Remove `apicurio.registry.as-confluent`, and replace `Legacy4ByteIdHandler` with `Default4ByteIdHandler` if you set `apicurio.registry.id-handler` yourself.
* Fix any typo'd keys in your runner config, because unknown keys now fail at startup again.
* Fix any unknown or misspelled fields in your KSML definitions, because they now fail at startup too.
* If you read an Avro `decimal` field in Python, change the code to expect a string instead of bytes.
* If a `streamPartitioner` or `valueJoiner` function uses a global variable called `headers` or `mappedKey`, rename it.
* If Python code calls a `streamPartitioner` function by position, pass `headers` before `numPartitions`, or call it by name.
* Rename any `as` name that is also a name under `streams`, `tables` or `globalTables`.
* Add `toTable` before any `join` or `leftJoin` with `foreignKeyExtractor` that has a stream as input.
* Do not keep a store iterator in a global variable for a later function call.
* If Python code uses the `stores` variable, read each store by its own name, or pass it to helper functions as an argument.
* If Python code reads `get(key)["value"]` or `["timestamp"]` on a store with `timestamped: true`, or on the store of a `count`, `aggregate`, `reduce` or non-versioned table, use `getWithTimestamp(key)` instead. The same applies to the results of `delete` and `putIfAbsent`, and to `fetch(key, time)` on the store of a windowed aggregation (use `fetchWithTimestamp`).
* Check every store you declare under `stores` or in an operation: it is now persistent, timestamped and has a changelog topic unless it sets `persistent`, `timestamped` or `logging` to `false`. Make sure the application can create the changelog topics (or create them yourself) and that `ksml.storageDirectory` is on a volume that has room for RocksDB. On the first start, KSML stops if a persistent store with local state leaves out `logging`, or if the persistent `thisStore` of a `leftJoin` or `outerJoin` between two streams doesn't set `logging: true`, until you set it. See [Declared stores use the Kafka Streams defaults](#declared-stores-use-the-kafka-streams-defaults).
* Stop all KSML 1.x instances of an application before you start 2.0.0, and don't do a rolling upgrade. A 1.x instance does not create the new changelog topics, so a 2.0.0 instance in the same group can't start its tasks. Before you start 2.0.0, wait until the consumer group of the application has no members (`kafka-consumer-groups.sh --describe --group <application.id> --members`). This takes up to `session.timeout.ms` (45 seconds by default), because Kafka Streams doesn't leave the group when it stops. If the application sets `group.instance.id` (static membership, which the KSML Helm chart does), an instance that comes back sooner gets its 1.x assignment again and is stuck. With the KSML Helm chart, scale the StatefulSet to 0 replicas, wait until the group is empty, and then run `helm upgrade`.
* If a store sets `versioned: true` without `persistent: true`, check the Python code that uses it, or set `persistent: false`.
* If you rely on every update of an `aggregate`, `count`, `reduce` or table being emitted, set `caching: false` on its store.
* If Python code calls `put` or `remove` on a session store, check that each session key is a dict with `key`, `start` and `end`, with `int` times. A session key that is not valid now raises an error instead of being ignored.
* Check `reduce` and `aggregate` functions that handle `None`, and functions that return `None`: tombstones are now handled like in Kafka Streams.
* Check consumers of topics that get empty `json` or union keys or values, and `valueJoiner`s that read such a table: an empty key or value is no longer a `null` key or a tombstone.
* Check your Python code and `modulePath` modules for modules that Python 3.13 removed, such as `cgi`, `telnetlib`, `imghdr` and `pipes`.
* If you set `allowNativeAccess: true`, check that only one definition file imports C extension modules such as `ctypes`, and that this file does not have both pipelines and producers.
* If you build your own code on top of the KSML libraries, update the Jackson package names.
* If you use `KeyValueIteratorProxy` or `WindowStoreIteratorProxy`, use `StoreIteratorProxy` instead, and create store proxies with `ProxyUtil.wrapStateStore`.
* If you extend `OperationProcessor` or `FixedKeyOperationProcessor`, implement `processRecord` instead of `process`.
* If you create a `TimestampedKeyValueStoreProxy` or `TimestampedWindowStoreProxy` yourself, pass a `LongSupplier` that gives the timestamp of the record that is being processed.
* If you create a `TopologyBuildContext` yourself, or call `StoreUtil.getStoreBuilder`, `StoreUtil.materialize` or `StoreUtil.validatedWindowStore`, pass `headersStoreFormat`: `true` when `dsl.store.format` is `HEADERS`, and the same value for the whole topology. If you call `BaseOperation.streamJoinedOf` in a subclass, pass the `TopologyBuildContext` first, and last whether the join has a shared store: `true` for a `leftJoin` or `outerJoin`, `false` for a `join`.
* If you call `StateStoreDefinition.caching()`, handle `null`: it means the definition does not set `caching`.
* If you call `TimestampedWindowStoreProxy.put(key, ValueAndTimestamp, windowStart)`, call `put(key, value, windowStart, timestamp)` instead. A `ValueAndTimestamp` still compiles as the value, but is then stored inside another `ValueAndTimestamp`.

## Apicurio Registry 3

KSML 2.0.0 upgrades to the Apicurio Registry 3 client. Point `apicurio.registry.url` at the `/apis/registry/v3` endpoint of an Apicurio 3.x server. The client can still fall back to the v2 API when the URL contains `/apis/registry/v2`, or when you set `apicurio.registry.url.version` to `2`, but KSML 2.0.0 is only tested against Apicurio 3.x and we do not support that path. If you cannot upgrade your registry yet, stay on KSML 1.x.

### Registry URL must use the v3 endpoint

Use the v3 endpoint. The v2 endpoint is not tested or supported by KSML 2.0.0.

Update the `apicurio.registry.url` in your schema registry config:

```yaml
# Before (KSML 1.x)
apicurio.registry.url: http://schema-registry:8081

# After (KSML 2.0.0)
apicurio.registry.url: http://schema-registry:8081/apis/registry/v3
```

If you use the Confluent-compatible endpoint of Apicurio, that path stays `/apis/ccompat/v7`.

### Basic-auth config keys were renamed

Apicurio v3 renamed the basic-auth keys. KSML no longer maps these keys itself, it passes them straight to Apicurio.

Update the keys:

```yaml
# Before (KSML 1.x)
apicurio.auth.username: my-user
apicurio.auth.password: my-secret

# After (KSML 2.0.0)
apicurio.registry.auth.username: my-user
apicurio.registry.auth.password: my-secret
```

To protect you from a silent failure, KSML now stops at startup with a clear error if it still finds the old `apicurio.auth.username` or `apicurio.auth.password` keys. This is on purpose: if the old keys were simply ignored, KSML would connect without a login and you would only find out later with a `401` error.

### The `auto-register.if-exists` values changed

Apicurio v3 changed the accepted values for `apicurio.registry.auto-register.if-exists`. The old `RETURN` value is gone. The valid values are now `FAIL`, `CREATE_VERSION` and `FIND_OR_CREATE_VERSION`. Apicurio validates this value at startup even when auto-register is off, so a stale value fails the serde immediately.

```yaml
# Before (KSML 1.x)
apicurio.registry.auto-register.if-exists: RETURN

# After (KSML 2.0.0)
apicurio.registry.auto-register.if-exists: FIND_OR_CREATE_VERSION
```

### The id-handler settings changed

Apicurio v3 dropped the `apicurio.registry.as-confluent` key, and removed the `Legacy4ByteIdHandler` class. The payload id format is now set by `apicurio.registry.id-handler` and `apicurio.registry.use-id` alone.

KSML sets both for you, so most users have nothing to do. But KSML never overwrites a value you set yourself, so a leftover v2 setting would quietly change your wire format or fail when the serde tries to load a class that is gone. KSML therefore stops at startup if it still finds either of them.

```yaml
# Before (KSML 1.x)
apicurio.registry.as-confluent: true
apicurio.registry.id-handler: io.apicurio.registry.serde.Legacy4ByteIdHandler

# After (KSML 2.0.0): drop as-confluent, and use the new handler if you set one at all
apicurio.registry.id-handler: io.apicurio.registry.serde.Default4ByteIdHandler
```

### The Apicurio on-wire format is unchanged

Good news: nothing to do here. The Apicurio notations (`apicurio_avro`, `apicurio_jsonschema`, `apicurio_protobuf`) keep the same on-wire format as KSML 1.x.

KSML still sets the Apicurio serde options explicitly (now using the Apicurio v3 config keys) so the format is pinned by KSML rather than left to Apicurio's defaults: the schema id is a 4-byte content id in the message payload, headers are off. We verified this on KSML 2.0.0 for all three notations: the message value starts with a `0x00` magic byte, then a 4-byte schema id, then the payload, and no schema id is written into Kafka headers. So existing topics keep working and you do not need to reprocess them.

If you deliberately want the header-based id format instead, set `apicurio.registry.headers.enabled: true` in the notation config; KSML only applies the payload-id defaults when headers are off, and your own settings always win.

### Resolving pre-registered schemas (issue #290)

KSML keeps defaulting `apicurio.registry.find-latest` to `true`, so that with auto-register disabled and a pre-registered schema, the serializer resolves the artifact by its coordinates instead of by its content. This is the same default that was introduced in KSML 1.3.0, and it keeps working on Apicurio v3. You do not need to change anything for this. If you had set `find-latest` yourself, KSML still respects your value.

### TLS and mutual TLS settings

Apicurio v3 has its own TLS settings. The Apicurio v2 `apicurio.registry.request.ssl.*` keys are ignored, so move the trust store to the v3 keys:

```yaml
apicurio.registry.tls.truststore.location: /path/to/truststore.jks
apicurio.registry.tls.truststore.password: my-secret
apicurio.registry.tls.truststore.type: JKS   # JKS, PKCS12 or PEM
```

For a registry that requires mutual TLS, set the client certificate with the Apicurio v3 key store keys:

```yaml
apicurio.registry.tls.keystore.location: /path/to/keystore.jks
apicurio.registry.tls.keystore.password: my-secret   # also unlocks the private key
apicurio.registry.tls.keystore.type: JKS             # JKS (default), PKCS12 or PEM
# For a PEM certificate and key, instead of the key store:
# apicurio.registry.tls.client-certificate: /path/to/client.crt   # file path or PEM content
# apicurio.registry.tls.client-key: /path/to/client.key           # file path or PEM content
```

KSML creates the registry client when it starts, so incomplete TLS settings stop KSML at startup.

## Stricter runner config validation
## Stricter validation of runner config and definitions

### Runner config

KSML validates your runner config again like it did on the 1.x line: an unknown or misspelled key now makes KSML stop at startup with an error, instead of being silently ignored.

Jackson 3 changed its default so that unknown keys are ignored. KSML re-enables the strict check on purpose, so a typo such as `schemaRegsitry` fails fast instead of quietly disabling a setting.

What you need to do: make sure your `ksml-runner.yaml` has no leftover or misspelled keys before you upgrade.

### KSML definitions

KSML definitions are now checked the same way. On the 1.x line a field that KSML did not know was silently ignored, so a typo quietly changed the behavior: `loging: true` on a store gave no changelog topic, and `grase: 5s` on `windowByTime` gave no grace period. KSML now stops at startup and names the field, where it is, and the fields that are allowed there:

```
KSML parse error: Error in input node processor->stores->count_store: Unknown field "loging", known fields: caching, historyRetention, keyType, logging, name, persistent, segmentInterval, timestamped, type, valueType, versioned
```

For an operation, function or store with a `type`, only the fields of that type are allowed. For example, `duration` on a `filter` step is now an error, because only the window operations have that field. The same goes for a join: only the fields for its `stream`, `table` or `globalTable` are allowed, so `timeDifference` on a join with a `table` is now an error.

An example case is a `type` line on an inline function, such as a function written directly under `if:` or `valueJoiner:`. Only functions under `functions:` have a `type` field; for an inline function, the operation already sets it. On the 1.x line the line was ignored. Now KSML stops with an error like `Unknown field "type", known fields: code, expression, globalCode, name, parameters, resultType`. Remove the line:

```yaml
      - type: join
        table: customer_profiles
        valueJoiner:
          type: valueJoiner    # remove this line
          expression: '{"order": value1, "customer": value2}'
          resultType: json
```

The published language schema (`ksml --schema`) already marked these fields as not allowed, so a definition that validates against the schema is not affected. The schema now also lists the optional top-level fields `name`, `version` and `description`.

What you need to do: start KSML once with each definition, or validate it against the language schema, and fix or remove the fields it reports.

## Duplicate keys in KSML definitions are now an error

KSML now rejects a definition file that uses the same key twice, instead of silently keeping the last value. This catches copy-and-paste mistakes early.

On the 1.x line a repeated key was accepted and the last one quietly won, which could hide a real mistake in a pipeline or function definition.

KSML stops at startup and names the file and the position:

```
Configuration Key   : 'definitionFile'
Configuration Value : '/ksml/processor.yaml'
Could not read the KSML definition: Duplicate field 'topic'
 at [Source: (File); line: 4, column: 12]
```

This also changed for any other unreadable definition file. On the 1.x line a definition KSML could not parse was skipped with one log line, and the runner started without that pipeline. It now stops, because a runner that quietly omits a pipeline looks like an idle topic rather than a broken config.

What you need to do: make sure each KSML definition file has no duplicate keys before you upgrade.

## An `as` name can not be a defined stream or table name

KSML now stops with a parse error when a pipeline saves its result with `as` under a name that is also defined under `streams`, `tables` or `globalTables`:

```
"as: orders" is already defined under "tables". Use another name
```

On the 1.x line this was accepted. A later `from` with that name then silently got the saved result, not the defined stream or table, because `from` looks for a saved result first.

A pipeline without a sink saves its result under the pipeline name. If that name is also defined under `streams`, `tables` or `globalTables`, KSML only logs a warning, so these definitions still start.

What you need to do: rename the `as` name, and every `from` or `cogroup` that uses it. Do not change a `join` or `merge` with that name: it already uses the defined stream or table, and would get the saved result instead.

For a pipeline without a sink, add `as` with another name, and use that name in the later `from` or `cogroup`. Adding `as` leaves the Kafka Streams topology unchanged, so a running application keeps its topics and state.

## A foreign-key join needs a table as input

KSML now stops at startup when a `join` or `leftJoin` with `foreignKeyExtractor` has a stream as input:

```
A join with "foreignKeyExtractor" needs a table as input, but found a stream. Add "toTable" before the join, or remove "foreignKeyExtractor"
```

Kafka Streams only has foreign-key joins between two tables. On the 1.x line, KSML accepted a `foreignKeyExtractor` on a stream, but never called it: the stream was joined with the table by the stream's own key. So the join silently found the wrong records, or none.

What you need to do: add `toTable` before the join, and `toStream` after it if the next steps need a stream:

```yaml
# Before (KSML 1.x)
via:
  - type: join
    table: customers
    foreignKeyExtractor: extract_customer_id
    valueJoiner: join_order_customer

# After (KSML 2.0.0)
via:
  - type: toTable
  - type: join
    table: customers
    foreignKeyExtractor: extract_customer_id
    valueJoiner: join_order_customer
  - type: toStream
```

The result of the join is then a table that is updated when an order or its customer changes. If you meant to join the stream by its own key, remove `foreignKeyExtractor` instead.

After `toStream`, a record can have a `null` value when the customer is deleted or the order gets another customer. Make the next steps accept `None`, or add a `filter` that drops these records.

The new steps add Kafka topics to your application: `toTable` adds a state store with a changelog topic, and the foreign-key join adds a subscription store with a changelog topic and two internal topics for subscription registrations and responses. If topics are created by hand or need ACLs in your cluster, set them up before the upgrade.

## Store iterators are closed when the function returns

KSML now closes a store iterator when all entries are read, or when the KSML function (the function in your definition) that created it returns. An iterator that is kept, for example in a global variable, raises an error when a later call uses it:

```
Store iterator was closed when the function that created it returned
```

On the 1.x line, KSML never closed such an iterator, so a later call could still read from it, and the store kept its resources open.

What you need to do: create a new iterator in each call.

```python
# Before (KSML 1.x): the iterator is kept in a global and read over several calls
if it is None:
  it = store.all()
while it.hasNext():
  ...

# After (KSML 2.0.0)
for entry in store.all():
  ...
```

## The `stores` variable is gone outside the function

On the 1.x line, every function also had a global variable `stores`, a dict with its state stores. Helper functions in `globalCode` could read it too. This was never documented.

In KSML 2.0.0, a function only has the stores it declares in `stores`, each as a variable with the store's name. `stores` is no longer a global, so a helper function in `globalCode` that reads it raises a `NameError`.

What you need to do: pass the store to the helper function as an argument.

```python
# Before (KSML 1.x): globalCode
def get_total(key):
  return stores["totals"].get(key)

# After (KSML 2.0.0): globalCode, called as get_total(totals, key)
def get_total(store, key):
  return store.get(key)
```

## Timestamped stores return the value

In KSML 1.x, Python code that used a timestamped key-value store got a dict with `value` and `timestamp` from `get`, `delete` and `putIfAbsent`. In KSML 2.0.0 these methods return only the value, as on a plain key-value store. A timestamped store now has the same methods as a plain key-value store, so the same Python code works on both.

This affects:

* stores with `timestamped: true`
* the stores of `count`, `aggregate`, `reduce` and tables (not versioned tables), also without `timestamped: true`, because Kafka Streams always makes these stores timestamped

Search your Python code for `["value"]` and `["timestamp"]` on store results. Old code does not always fail: if the stored value has its own `value` field, `store.get(key)["value"]` now reads that field, without an error.

To read the timestamp, use `getWithTimestamp`:

```python
# Before (KSML 1.x)
last = store.get(key)
color = last["value"]["color"]

# After (KSML 2.0.0)
last = store.getWithTimestamp(key)
color = last["value"]["color"]
```

There is no `deleteWithTimestamp`. If you need the old timestamp, call `getWithTimestamp` before `delete`.

The same applies to the store of a windowed aggregation that a Python function uses: `fetch(key, time)` returns only the value, as on a plain window store. Use `fetchWithTimestamp(key, time)` to read the timestamp. See [Timestamped Window Stores](reference/state-store-reference.md#timestamped-window-stores).

`put(key, value)` without a timestamp now works on a timestamped store, and uses the timestamp of the record that is being processed. `put(key, value, timestamp)` still works. See [Timestamped Key-Value Stores](reference/state-store-reference.md#timestamped-key-value-stores).

## Session store keys are checked

In KSML 1.x, session store `put` and `remove` silently ignored a session key that they could not read: a key that was not a dict, missed a field, or had `start` and `end` times in the int range. Nothing was written, and no error was raised. A session with one record (`start` equal to `end`) failed with a Java error that Python could not catch.

In KSML 2.0.0, any `int` time from 0 works and `start` may equal `end`. A session key that is not valid raises `TypeError` or `ValueError`, which `except Exception` catches. See [Session Store Methods](reference/state-store-reference.md#session-store-methods).

What you need to do: if a Python function writes or removes sessions, check that each session key is a dict with `key`, `start` and `end`, with `int` times, `start` not negative and not after `end`. Code that passed other keys wrote nothing before, so fixing it can change what your application stores.

Keys and values of stores are now also converted to the store's `keyType` and `valueType`. Code that worked before keeps working; see [Keys and Values](reference/state-store-reference.md#keys-and-values).

## Declared stores use the Kafka Streams defaults

In KSML 1.x, a store you declared was kept in memory, had no changelog topic, was not timestamped and was not cached, unless you switched these options on. Its state was lost when the application restarted. In KSML 2.0.0, declared stores get the same defaults as in Kafka Streams:

| Option | KSML 1.x default | KSML 2.0.0 default |
|---|---|---|
| `persistent` | `false` | `true` |
| `logging` | `false` | `true` |
| `timestamped` | `false` | `true` |
| `caching` | `false` | `true` when an operation such as `aggregate`, `count`, `reduce` or a table materializes the store, `false` when Python functions use it |

This applies to stores under `stores` and to stores declared inside an operation or on a table. Tables and globalTables without a `store`, and operations without a `store`, do not change. An option you set explicitly keeps its value.

What changes for a running application:

* Each declared store gets a changelog topic named `<application.id>-<store name>-changelog`. If topics are created by hand or need ACLs in your cluster, create them or let the application create topics before the upgrade. A table with a `store` uses its input topic as changelog when `topology.optimization` is `all`, the runner's default.
* Persistent stores keep their state in RocksDB under `storageDirectory` in the `ksml` section of the runner config (see [Directory Configuration](reference/configuration-reference.md#directory-configuration)). KSML uses it as the Kafka Streams `state.dir` and ignores a `state.dir` in the `kafka` section, with a warning. Put it on a volume with enough room that is kept between restarts, or the stores are restored from their changelog at every start.
* With the KSML Helm chart, set `store.spec` to put `storageDirectory` on a persistent volume. Without it, the stores are in an `emptyDir`, which is empty after every pod restart and limited to `store.emptyDir.sizeLimit` (`1Gi`, before `10Mi`). Kubernetes evicts the pod when the stores use more.
* With caching, an `aggregate`, `count`, `reduce` or table emits the latest result for a key when Kafka Streams commits (`commit.interval.ms`) or when the cache is full (`statestore.cache.max.bytes`), instead of every update. Set `caching: false` on the store if you need every update.
* RocksDB and changelog topics cost time and broker traffic: each write goes to local disk, and each write that isn't cached also to the changelog. Pipelines with many stores or many distinct keys can become noticeably slower, so test the throughput before you upgrade production. A store that doesn't need to survive a restart can set `persistent: false` and `logging: false`, as in 1.x. See [Performance Considerations](reference/state-store-reference.md#performance-considerations).
* A store with `versioned: true` but without `persistent: true` was an in-memory key-value store in 1.x, and is now a versioned store: in Python, `get` returns a dict with `value` and `timestamp`, and `put` and `delete` take a timestamp (see [Versioned Key-Value Stores](reference/state-store-reference.md#versioned-key-value-stores)). A table with this store is a versioned table, which changes stream-table join lookups. Set `persistent: false` and `timestamped: false` to keep the plain in-memory store of 1.x (KSML logs a warning).
* A window store under `stores` is timestamped unless it sets `timestamped: false`, so Python functions get the `*WithTimestamp` methods. See [Timestamped Window Stores](reference/state-store-reference.md#timestamped-window-stores).
* A RocksDB store of a `count`, `aggregate`, `reduce` or table keeps timestamps, so `getWithTimestamp` and `fetchWithTimestamp` return the record timestamp instead of `-1`.
* `timestamped: true` on a session store logs a warning, because session stores are not timestamped.
* The shared store of a `leftJoin` or `outerJoin` between two streams has a changelog topic unless both `thisStore` and `otherStore` set `logging: false`. In 1.x it had one only when `thisStore` set `logging: true`. The two window stores of the join always have a changelog topic, so `logging: false` on them logs a warning when it has no effect. See [Changelog topics of stream-stream joins](reference/state-store-reference.md#for-stream-stream-joins).

On the first start, KSML checks the local state that 1.x left, before Kafka Streams starts:

* A persistent store that leaves out `logging` had no changelog in 1.x, and a new, empty changelog can't restore its state. So KSML stops when such a store has local state, and changes nothing. Set `logging: false` to keep the 1.x behavior, or `logging: true` to start a changelog with only the new updates. With `exactly_once_v2`, `logging: true` makes Kafka Streams delete the old state of all stores in the same task, until the store has written to its new changelog there: run with `processing.guarantee: at_least_once` until then. The error names these tasks and stores. To keep the old state in the changelog, set `logging: true` while still on 1.x, and upgrade once the changelog has the state you need. For the shared store of a stream-stream `leftJoin` or `outerJoin`, set `logging: false` on both join stores, or `logging: true` on `thisStore`.
* KSML 2.0.0 can't read the 1.x state of a persistent window store for Python with `timestamped: true`. With `logging: true`, KSML deletes this state once, and Kafka Streams restores it from the changelog. Without it, KSML stops: delete the files that the error names, or rename the store. With `exactly_once_v2`, KSML also stops when this delete would take a store with `logging: false` along: start once with `processing.guarantee: at_least_once`.
* The check runs once. It reads `logging` and `timestamped` in your YAML as the 1.x options, so do the first start with the 1.x options. Then KSML writes `.ksml-store-format` in `<storageDirectory>/<application.id>`. If you go back to 1.x and upgrade again, delete this file first.

KSML now also follows the Kafka Streams setting `dsl.store.format`. With `dsl.store.format: HEADERS` in the `kafka`
section of the runner config, declared key-value and window stores that are persistent and timestamped, and persistent
session stores, can keep headers with each value; writes from Python store none. The default (`DEFAULT`) changes nothing, so you only need to act if you set it: switch the format only on a new
application or after a reset, because existing state keeps its old format. See
[Store Format](reference/state-store-reference.md#store-format).

To keep the KSML 1.x behavior for a store, set the options explicitly:

```yaml
stores:
  my_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: false
    logging: false
    timestamped: false
    caching: false
```

A store that was in memory without a changelog in 1.x started empty after every restart. After the upgrade, it starts empty once more, because there is no earlier state to restore, and from then on it keeps its state.

## Tombstones are handled like in Kafka Streams

A tombstone is a record with a key but no value. It means "delete this key". On the 1.x line, KSML did not give a tombstone to Kafka Streams as a `null`, for example on a `json` or `avro` topic, nor a `None` returned by a function. So Kafka Streams did not see a delete:

* `count` counted tombstones.
* `reduce` and `aggregate` called the function with `None`, which often stopped the application with a `TypeError`.
* A join between tables wrote data where it should write a delete.
* A join with a global table failed with a `NullPointerException` when the `mapper` returned `None`.

KSML 2.0.0 handles tombstones like Kafka Streams. This changes the result of some pipelines, without an error:

* `count`, `reduce` and `aggregate` skip a tombstone, so a `reducer` or `aggregator` is not called for it, and code in it that handles `None` does not run for a tombstone any more.
* On a table, `filter` and `filterNot` do not call the `predicate` for a tombstone. The delete is passed on.
* A function that returns `None` gives a tombstone. From a `reducer` or `aggregator`, it deletes the key from the store. As a value, a later `count`, `reduce` or `aggregate` skips it.
* `groupBy`, `groupByKey`, `count`, `reduce` and `aggregate` skip a record with a `null` key, like Kafka Streams. That is a `null` key read from a topic, of any key type, or from a `groupBy` mapper that returns `None`. A table, `toTable` and a `join` with a table skip it too, and a `leftJoin` with a table calls the `valueJoiner` with `None` for `value2`. Before, such a record could stop the application with a `NullPointerException`.
* A join between tables writes a tombstone when the joined row is deleted. Consumers of the output topic get these tombstones.

An empty key or value (zero bytes) is not a `null` key or a tombstone. With `json` (without a schema) or a union type, your functions get it as `None`, also after a step that uses an internal topic. But `groupByKey`, `count`, `reduce` and `aggregate` do not skip it. What KSML does with it:

* Passed on to a topic without a change: it stays empty. On 1.x it was written as a `null` key or a tombstone.
* On a table: the row stays, with the value `None`. On 1.x the row was removed. So a `join` with the table now calls the `valueJoiner` with `None` for `value2`.
* After a `convertValue` to another type, like `string` or `avro`: it is a tombstone.
* Returned by a function, like a `groupBy` mapper: it is `None`, so a `null` key or a tombstone.

[Tombstones and `None`](reference/function-reference.md#tombstones-and-none) and [Tombstones in joins](reference/operation-reference.md#tombstones-in-joins) list the details.

What you need to do:

* If a `reducer` or `aggregator` handles `None` to process deletes, handle the delete before the grouping instead. For example, a `mapValues` that returns `{"deleted": True}` for `None` turns the tombstone into a value that the `aggregator` gets.
* If a function returns `None` and a later step should still see the record, return a value instead.
* If a `groupBy` mapper returns `None` for some records, return a real key instead, or the records are skipped.
* If a pipeline writes a join between tables to a topic, check that the consumers of that topic accept tombstones.
* If a pipeline passes on empty `json` or union keys or values to a topic, check that the consumers accept them. They got `null` keys or tombstones before.
* If a `json` or union table can get empty values, check that functions that read the table, like a `valueJoiner`, accept `None`.
* State built by 1.x is not recomputed: counts that include tombstones and joined rows that were not deleted stay as they are. If they must be correct, reset the application and process the input again.

## Avro logical types

KSML now understands Avro logical types (`uuid`, `decimal`, `date`, `time-millis`, `time-micros`,
`timestamp-millis`, `timestamp-micros`, `local-timestamp-millis`, `local-timestamp-micros`). It keeps
the logical type when it reads and writes a record. One of these changes what your Python code sees.

### A decimal is now a string

An Avro `decimal` used to arrive in Python as raw bytes, because KSML treated it as its base `bytes`
type. It is now a string holding the exact number.

```python
# Before (KSML 1.x): amount was bytes, and you had to decode it yourself
amount = value["amount"]          # b'\x30\x39'

# After (KSML 2.0.0): amount is the number as text
amount = value["amount"]          # "123.45"
```

What you need to do: if a Python function reads a `decimal` field, treat it as a string. Use
`float(value["amount"])` or Python's `decimal.Decimal` when you need to calculate with it. When you
write the field, write a string such as `"123.45"`.

### Values are checked against their logical type

KSML checks that a value fits its logical type, for example that a `uuid` is a real UUID and that a
`time-millis` is between `0` and `86399999`.

Writing an invalid value fails, which points at the mistake in your own pipeline. Reading an invalid
value only writes a warning to the log and passes the value on. That is on purpose: a bad record in a
topic comes from another system, and the default consume error handler is `stopOnFail`, so failing
there would stop your application because of someone else's data.

What you need to do: nothing to upgrade, but watch the log for these warnings after you upgrade.

### Decimal precision may grow, scale may not change

Two versions of a schema whose `decimal` differs only in precision are compatible, as long as the
scale is the same and the precision grows. Narrowing the precision or changing the scale is rejected,
because both can lose digits.

### Limits

* Only a `bytes`-backed decimal is supported. A `fixed`-backed decimal is treated as a plain `fixed`
  and reaches Python as bytes.
* Logical types come from the schema, so they work when the schema is loaded from a `.avsc` file or
  from the schema registry. An inline KSML schema cannot express one.

## Kafka Streams error handlers

For pipeline authors who write KSML YAML, there is nothing to change here.

KSML did move off the Kafka Streams handler methods that 4.x deprecated. If you implement your own Kafka Streams error handler against the KSML libraries, `handle` became `handleError`, and the enum results were replaced by `Response.resume()`, `Response.fail()` and `Response.retry()`.

## Kafka 4.4

KSML 2.0.0 upgrades Kafka Streams and the Kafka clients from 4.3 (used by KSML 1.3.x) to 4.4.0. For pipeline authors, there is nothing to change.

Two function types get a new parameter:

* A `streamPartitioner` gets the record headers in `headers`. Its parameters are now `topic`, `key`, `value`, `headers` and `numPartitions`, in the order that Kafka Streams uses.
* A `valueJoiner` in a join with a globalTable gets the key that the `mapper` returned in `mappedKey`. This parameter is optional and comes last, so `key`, `value1` and `value2` keep their position.

Existing functions work without changes, with two exceptions:

* If such a function uses a global variable called `headers` or `mappedKey`, for example from `globalCode`, rename that variable. The new parameter hides it, and `global headers` in the function code fails at startup.
* If Python code calls a `streamPartitioner` function by position, for example `my_partitioner(topic, key, value, 4)`, pass `headers` before `numPartitions`, or call the function by name. A `generic` function with only `topic`, `key`, `value` and `numPartitions` still works as a partitioner.

## Python 3.13 and C extension modules

KSML 2.0.0 runs on GraalVM 25.5.5.0.3. Your Python code now runs on Python 3.13 instead of 3.12. Python 3.13 removed some old modules, for example `cgi`, `telnetlib`, `imghdr` and `pipes`. Check your Python code and your `modulePath` modules for them.

With `allowNativeAccess: true`, only one Python context in a KSML process can load C extension modules, such as `ctypes`, `sqlite3` and `termios`. Before, `ctypes` worked in every context. KSML creates one context for the pipelines and one for the producers of each definition file, so even one file with both can hit this limit. See [Python Context Configuration](reference/configuration-reference.md#python-context-configuration).

With the default `allowNativeAccess: false`, nothing changes: C extension modules stay blocked.

## Protobuf and Wire

Protobuf was upgraded to the 4.x line and Wire to the 6.x line. There is no configuration change for pipeline authors. If you use Protobuf schemas, test them after upgrading.

## Jackson 3 (only if you extend KSML in Java)

If you only write KSML definitions and runner config, you can skip this section.

If you have Java code that depends on the KSML data libraries, KSML moved from Jackson 2 to Jackson 3. The main package root changed:

* `com.fasterxml.jackson.core`, `com.fasterxml.jackson.databind` and the dataformat packages moved to `tools.jackson.*`.
* Jackson annotations stay under `com.fasterxml.jackson.annotation`, so your annotated model classes do not need to change.

Update your imports and rebuild against the KSML 2.0.0 libraries.
