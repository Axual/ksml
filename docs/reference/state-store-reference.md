# State Store Reference

State stores enable stateful stream processing in KSML by maintaining data across multiple messages. This reference covers state store configuration, usage patterns, and best practices.

For hands-on tutorials, please check out:

- [State Stores Intermediate Tutorial](../tutorials/intermediate/state-stores.md)
- [Custom State Stores Advanced Tutorial](../tutorials/advanced/custom-state-stores.md) 

## What are State Stores?

State stores allow KSML applications to:

- Maintain state between messages for aggregations, counts, and joins
- Track historical information for context-aware processing  
- Build complex business logic that depends on previous events

## State Store Types

KSML supports three types of state stores, each optimized for specific use cases:

| Type | Description | Use Cases | Examples                                                                                                          |
|------|-------------|-----------|-------------------------------------------------------------------------------------------------------------------|
| `keyValue` | Simple key-value storage | General lookups, non-windowed aggregations, manual state management | [`keyValue` type state store](../tutorials/intermediate/state-stores.md#1-predefined-store-configuration) |
| `window` | Time-windowed storage with automatic expiry | Time-based aggregations, windowed joins, temporal analytics | [`window` type state store](../tutorials/intermediate/aggregations.md#windowed-aggregation-example)               |
| `session` | Session-based storage with activity gaps | User session tracking, activity-based grouping | [`session` type state store](../tutorials/intermediate/windowing.md#session-window-user-activity-analysis)                                             |

## Configuration Methods

### 1. Pre-defined State Stores

Define once in the `stores` section, reuse across operations:

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml:17:26"
```

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml:17:26"
```

**Full example:**

- [Pre-defined State Stores Tutorial](../tutorials/intermediate/state-stores.md#1-predefined-store-configuration)

### 2. Inline State Stores

Define directly within operations for custom configurations:

```yaml
--8<-- "docs-examples/intermediate-tutorial/state-stores/processor-inline-store.yaml:58:67"
```

**Full example:**

- [Inline State Stores Tutorial](../tutorials/intermediate/state-stores.md#2-inline-store-configuration)

## Configuration Parameters

### Common Parameters (All Store Types)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | String | No | Operation name | Unique identifier for the state store |
| `type` | String | Yes | - | Store type: `keyValue`, `window`, or `session` |
| `keyType` | String | Yes | - | Data type for keys (see [Data Types Reference](data-and-formats-reference.md)) |
| `valueType` | String | Yes | - | Data type for values |
| `persistent` | Boolean | No | `true` | If `true`, uses RocksDB (disk); if `false`, uses in-memory storage |
| `caching` | Boolean | No | See below | If `true`, improves read performance but delays updates |
| `logging` | Boolean | No | `true` | If `true`, creates changelog topic for fault tolerance (in addition to local storage) |
| `timestamped` | Boolean | No | `true` | If `true`, stores timestamp with each entry. Has no effect on session stores: setting it there logs a warning |

These defaults are the same as in Kafka Streams. Set an option to `false` to turn it off. Without `caching`, the
default depends on how the store is used, again as in Kafka Streams:

- When an operation such as `aggregate`, `count`, `reduce` or a table materializes the store, it is cached.
- When Python functions use the store (it is listed in their `stores`), it is not cached.

Tables and globalTables without a `store` are not affected: their stores stay in memory without a changelog topic.

### Store Format

With `dsl.store.format: HEADERS` in the `kafka` section of the runner config, KSML builds declared stores in the
headers-aware format of Kafka Streams, which keeps the record headers with each value:

```yaml
kafka:
  dsl.store.format: HEADERS
```

This applies to stores that are persistent and timestamped (both are the default), whether an operation materializes
them or Python functions use them, and to the window stores of stream-stream joins. It does not apply to in-memory
stores (`persistent: false`), stores with `timestamped: false` and versioned stores: Kafka Streams has no
headers-aware format for them. Session stores are headers-aware when they are persistent. With the default value
`DEFAULT`, no store is headers-aware. Python functions use headers-aware stores with the same methods as before; see
[Headers-Aware Stores](#headers-aware-stores).

Operations without a `store` are not declared stores: Kafka Streams creates their stores and picks the format itself,
also from `dsl.store.format`. Tables and globalTables without a `store` keep an in-memory store, which is not
headers-aware.

**Changing the format of a running application:** a store keeps its data on disk and in its changelog topic in the
format it was created with. Before you switch `dsl.store.format` on an application that already has state, reset the
application or give it a new `application.id`. Switching back from `HEADERS` to `DEFAULT` is not supported.

### Window Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `windowSize` | Duration | Yes | - | Size of the time window (must match operation's window duration) |
| `retention` | Duration | No | - | How long to retain window data (should be > windowSize + grace period) |
| `retainDuplicates` | Boolean | No | `false` | Whether to keep duplicate entries in windows |

### KeyValue Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `versioned` | Boolean | No | `false` | If `true`, maintains version history of values |
| `historyRetention` | Duration | No (Yes if versioned) | - | How long to keep old versions |
| `segmentInterval` | Duration | No | - | Segment size for versioned stores |

**Important:** Versioned stores (`versioned: true`) do not support caching. When an operation materializes a
versioned store, Kafka Streams ignores caching. For a versioned store that Python functions use, do not set
`caching: true`. Like other stores, a versioned store is persistent unless it sets `persistent: false`, in which case
it becomes a plain in-memory store without versions.

### Session Store Specific Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `retention` | Duration | No | - | How long to retain session data |

## Usage in Operations

### With Aggregations

State stores are automatically used by aggregation operations:

```yaml
pipelines:
  calculate_statistics:
    from: payment_stream
    via:
      - type: groupByKey
      - type: aggregate
        store:
          type: keyValue
          retention: 1h
          caching: false
```

**Full example:**

- [Aggregations with State Stores](../tutorials/intermediate/aggregations.md#complex-aggregate-json-format)

### With Manual State Access

Access state stores directly in functions for custom caching, enrichment, and state management:

```yaml
stores:
  user_profile_cache:
    name: user_profile_cache
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    logging: true
    retention: 1h
```


```yaml
functions:
  enrich_and_cache:
    type: valueTransformer
    code: |
      # Get cached data for this user
      cached_data = user_profile_cache.get(key)
```

**Full example**

??? info "State Store with Manual State Access (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/intermediate-tutorial/state-stores/producer-user-events.yaml" %}
    ```

??? info "State Store with Manual State Access (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/intermediate-tutorial/state-stores/processor-manual-store.yaml" %}
    ```

This example demonstrates:

- **Store declaration**: List store names in the function's `stores` parameter
- **State retrieval**: Use `store_name.get(key)` to read cached data
- **State storage**: Use `store_name.put(key, value)` to update cache
- **Null handling**: Check if `cached_data is not None` for first-time detection

**Key functions:**

- `store.get(key)` - Retrieve value from state store (returns None if not found)
- `store.put(key, value)` - Store key-value pair in state store

See [Key-Value Store Methods](#key-value-store-methods) for all methods.

### With Session Operations

Session stores track user activity with inactivity gaps and automatic timeout:
```yaml
pipelines:
  user_session_analysis:
    from: user_clicks
    via:
      - type: groupByKey
      - type: windowBySession
        inactivityGap: 2m  # Close session after 2 minutes of inactivity
        grace: 30s
      - type: count
        store:
          name: user_sessions
          type: session
          retention: 1h
          caching: false
```

**Full example:**

- [Session store type example](../tutorials/intermediate/windowing.md#session-window-user-activity-analysis)

**Session store patterns:**

- **Activity tracking**: Monitor user engagement and session duration
- **Timeout detection**: Identify inactive sessions for cleanup
- **State aggregation**: Accumulate metrics within session boundaries

### With Windowed Operations

Window stores require specific configuration that varies by operation type:

#### For Aggregations (Count, Aggregate, etc.)

```yaml
pipelines:
  count_clicks_5min:
    from: user_clicks
    via:
      - type: groupByKey
      - type: windowByTime
        windowType: tumbling
        duration: 5m
        grace: 30s
      - type: count
        store:
          name: click_counts_5min
          type: window
          windowSize: 5m          # Must match window duration
          retention: 35m          # windowSize + grace + buffer
          caching: false
```

#### For Stream-Stream Joins

```yaml
pipelines:
  join_streams:
    from: stream1
    via:
      - type: join
        stream: stream2
        timeDifference: 15m
        grace: 5m
        thisStore:
          type: window
          windowSize: 30m         # Must be 2 × timeDifference
          retention: 35m          # 2 × timeDifference + grace
          retainDuplicates: true
```

**Changelog topics of stream-stream joins:** A stream-stream join has two window stores, `thisStore` and
`otherStore`. A `leftJoin` or `outerJoin` also has a shared store for records that have not found a match yet.
Kafka Streams always gives the two window stores a changelog topic, also with `logging: false`. The `logging` setting
of the two window stores decides whether the shared store gets a changelog topic:

- Both without `logging`, or both `logging: true`: the shared store has a changelog topic. This is the default.
- Both `logging: false`: the shared store has no changelog topic. KSML logs a warning for each window store, because
  their own changelog topics stay.
- Different values: KSML stops with an error that names both stores. Set `logging` to the same value on both.

### Retention Guidelines

**For Window Aggregations:**
```
retention = windowSize + gracePeriod + processingBuffer
```

**For Stream-Stream Joins:**
```
windowSize = 2 × timeDifference
retention = 2 × timeDifference + gracePeriod
```

**Full examples:**

- [Windowed Aggregations](../tutorials/intermediate/windowing.md#tumbling-window-click-counting)
- [Stream-Stream Joins](../tutorials/intermediate/joins.md#stream-stream-join)

## Python Store Methods

Python functions can call these methods on the stores listed in their `stores`.

### Keys and Values

Keys and values are converted to the `keyType` and `valueType` of the store. This matters for numbers: Python has one
`int` type, while a store with `valueType: long` needs a long and a store with `valueType: int` an int. So
`store.put(key, count + 1)` works on both. The keys of all methods are converted too, so `store.get(5)` finds the value
that `store.put(5, ...)` wrote.

A value is only converted to a type of the same kind:

| Store type                     | Accepted Python values                               |
|--------------------------------|------------------------------------------------------|
| `byte`, `short`, `int`, `long` | `int`, within the range of the type                  |
| `float`, `double`              | `int` or `float`                                     |
| `string`                       | `str`                                                |
| `boolean`                      | `bool`                                               |
| `bytes`                        | `bytes`, or a list or tuple of ints from -128 to 255 |

Reads return the value of a `bytes` store as a list of ints.

This is stricter than the conversion of a function result to its `resultType`, on purpose: a function with
`resultType: long` may return `5.0`, but `store.put(key, 5.0)` on a `long` store raises `TypeError`, so a value of the
wrong kind is never stored by accident.

Other values raise a Python error that you can catch with `except Exception`, and nothing is written:

- `TypeError` - a value of another kind, for example a `float` on a `long` store (also a whole number like `2.0`), a
  `str` on a `long` store, or an `int` on a `string` store. Convert the value first, for example with `int(...)` or
  `str(...)`. A `bool` on a number store is also a `TypeError`, although in Python `True` is an `int` too
- `ValueError` - an `int` out of range, for example `2 ** 40` on an `int` store, `10 ** 400` on a `double` store, or
  `300` in a list for a `bytes` store

The message names the store and the types, not the value, for example
`TypeError: Store counts: value must be long, got float`.

`None` is not converted, so `store.put(key, None)` still deletes the key. A `None` key in a method that takes one key,
such as `get`, `put`, `putIfAbsent`, `putAll`, `delete` or `fetch`, raises `ValueError`, because Kafka does not accept
it. `None` as a bound of a range, for example in `range`, means no bound. For session keys, see
[Session Store Methods](#session-store-methods). Stores with other types (`json`, `avro:...`
and other schema types, unions, enums, or no `keyType` / `valueType`) get keys and values as they are. Stores in the
`assert` blocks of the [KSML test runner](../getting-started/testing-your-pipeline.md) convert the same way.

### Key-Value Store Methods

- `store.get(key)` - The value of the key, or `None`
- `store.put(key, value)` - Store the value. A value `None` deletes the key
- `store.delete(key)` - Remove a key, and return its old value
- `store.putIfAbsent(key, value)` - Store the value only if the key has none, and return the existing value
- `store.putAll(entries)` - Store many entries in one call. `entries` is a list or an iterator of `(key, value)` pairs or dicts with `key` and `value`, so `store.putAll(other_store.all())` copies a store. All entries are kept in memory first, so use this for small stores only. A value `None` deletes the key
- `store.range(keyFrom, keyTo)` and `store.reverseRange(keyFrom, keyTo)` - Entries from `keyFrom` to `keyTo`, both included, in key order or in reverse. `None` means no bound
- `store.all()` and `store.reverseAll()` - All entries, in key order or in reverse
- `store.prefixScan(prefix)` - Entries whose key starts with `prefix`. The prefix is compared with the stored keys as UTF-8 bytes, so it only works for stores with `keyType: string`; on a store with another declared key type it raises `TypeError`. An empty prefix returns all entries
- `store.approximateNumEntries()` - Estimated number of entries

Keys are ordered by their serialized bytes, not by their value. For string keys this is UTF-8 byte order: `"B"` comes before `"a"`, and `"a"` before `"ab"` before `"b"`. For number keys this is not numeric order: negative numbers come after positive numbers, so `range(-10, 10)` on a `long` key store returns nothing.

Besides the [conversion of keys and values](#keys-and-values), `putAll` and `prefixScan` check the form of their
arguments and raise a Python error that you can catch with `except Exception`:

- `TypeError` - the argument has the wrong kind: `putAll` gets something that is not a list or an iterator, or an entry that is not a pair or a dict; `prefixScan` gets a prefix that is not a string, or runs on a store whose `keyType` is not `string`
- `ValueError` - the argument has the right kind, but wrong content: a pair that does not have 2 elements, or a dict without `key` or `value`

The message names the position of the bad entry, for example `putAll entry 1 must be a (key, value) pair ...`. A `putAll` call with a bad entry writes nothing.

### Window Store Methods

Times are in milliseconds. `timeFrom` and `timeTo` select windows by their start time, both included.

- `store.put(key, value, windowStartTimestamp)` - Store the value in the window that starts at `windowStartTimestamp`
- `store.fetch(key, time)` - The value of the window that starts at `time`, or `None`
- `store.fetch(key, timeFrom, timeTo)` and `store.backwardFetch(key, timeFrom, timeTo)` - The windows of one key,
  oldest first or newest first. The key of each entry is the window start time
- `store.fetch(keyFrom, keyTo, timeFrom, timeTo)` and `store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo)` - The
  windows of a range of keys
- `store.fetchAll(timeFrom, timeTo)` and `store.backwardFetchAll(timeFrom, timeTo)` - The windows of all keys
- `store.all()` and `store.backwardAll()` - All windows in the store

### Session Store Methods

A session is identified by a dict with `key`, `start` and `end` (times in milliseconds, as `int`). A session with one
record has `start` equal to `end`. The session keys that `fetch` and `findSessions` return can be passed back to `put`
and `remove`; their `startTime` and `endTime` fields are ignored. For a session key that is not valid, `put` and
`remove` raise a Python error and write nothing:

- `TypeError` - the session key is not a dict, or `start` or `end` is not an `int`
- `ValueError` - `key`, `start` or `end` is missing, `key` is `None`, a time is out of the range of a long, `start` is
  negative, or `start` is after `end`

- `store.put(sessionKey, value)` - Store the value of a session
- `store.remove(sessionKey)` - Remove a session
- `store.fetchSession(key, sessionStartTime, sessionEndTime)` - The value of one session, or `None`
- `store.fetch(key)` and `store.backwardFetch(key)` - All sessions of one key, oldest first or newest first
- `store.fetch(keyFrom, keyTo)` and `store.backwardFetch(keyFrom, keyTo)` - All sessions of a range of keys
- `store.findSessions(key, earliestSessionEndTime, latestSessionStartTime)` and
  `store.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime)` - The sessions of one key that end
  at or after `earliestSessionEndTime` and start at or before `latestSessionStartTime`
- `store.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime)` and
  `store.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime)` - The same for a range
  of keys

### Order of Window and Session Entries

For window and session stores, when the entries have more than one key (`all()`, `fetchAll()`, key ranges and their
`backward` versions), the order depends on the store. An in-memory store sorts them by time first, then by key. A
RocksDB store keeps its data in time segments: it sorts the entries by segment first, then by key, then by time. The
time is the window start for window stores and the session end for session stores.

### Timestamped Key-Value Stores

A key-value store with `timestamped: true` keeps a timestamp with each value. The store of a `count`, `aggregate`,
`reduce` or table (not a versioned table) is also a timestamped store in Python, with or without `timestamped: true`. In Python it has the
same methods as a plain key-value store, and they return the same results, so the same code works on both kinds of
store:

- `store.get(key)`, `store.delete(key)` and `store.putIfAbsent(key, value)` return only the value
- `store.all()`, `store.range(...)`, `store.reverseRange(...)`, `store.reverseAll()` and `store.prefixScan(prefix)`
  give entries with `key` and `value`
- `store.put(key, value)`, `store.putIfAbsent(key, value)` and `store.putAll(entries)` store the values with the
  timestamp of the record that is being processed. In `put` and `putAll`, a value `None` deletes the key

To work with the timestamps, use these methods. They only exist on timestamped stores:

- `store.getWithTimestamp(key)` - A dict with `value` and `timestamp`, or `None` if the key is not found
- `store.put(key, value, timestamp)` and `store.putIfAbsent(key, value, timestamp)` - Store the value with the given
  timestamp in milliseconds
- `store.allWithTimestamp()`, `store.rangeWithTimestamp(keyFrom, keyTo)`,
  `store.reverseRangeWithTimestamp(keyFrom, keyTo)`, `store.reverseAllWithTimestamp()` and
  `store.prefixScanWithTimestamp(prefix)` - Entries with `key`, `value` and `timestamp`

The store of a `count`, `aggregate`, `reduce` or table keeps timestamps unless it is persistent and sets
`timestamped: false`. In that case, the `*WithTimestamp` methods can return `-1` as the timestamp: always without
caching, and with `caching: true` once the entry has left the cache (for example after a restart). Do not pass this
timestamp to `put`.

In `putAll`, a dict entry can have a `timestamp` field. That entry is stored with this timestamp, so
`store.putAll(other_store.allWithTimestamp())` copies a store with its timestamps. A `(key, value, timestamp)` tuple
is not accepted. A plain key-value store ignores the `timestamp` field.

A negative timestamp raises a `ValueError` and writes nothing, because Kafka cannot write it to a changelog topic.
To copy a store that returns `-1` as the timestamp, use `store.putAll(other_store.all())`: the entries then get the
timestamp of the current record.

```python
last = store.getWithTimestamp(key)
if last is not None and last["timestamp"] > timestamp_limit:
  log.info("recent value: {}", last["value"])
store.put(key, value)  # stored with the timestamp of the current record
```

### Timestamped Window Stores

A windowed aggregation, such as `count` or `aggregate`, keeps its results in a timestamped window store. When a
Python function lists this store in its `stores`, it gets the same methods as a
[plain window store](#window-store-methods), with the same results:

- `store.fetch(key, time)` returns only the value
- `store.fetch(...)`, `store.backwardFetch(...)`, `store.fetchAll(...)`, `store.backwardFetchAll(...)`, `store.all()`
  and `store.backwardAll()` give entries with `key` and `value`
- `store.put(key, value, windowStartTimestamp)` stores the value with the timestamp of the record that is being processed

To work with the timestamps, use these methods:

- `store.fetchWithTimestamp(key, time)` - A dict with `value` and `timestamp`, or `None` if there is no value
- `store.put(key, value, windowStartTimestamp, timestamp)` - Store the value with the given timestamp
- `fetchWithTimestamp`, `backwardFetchWithTimestamp`, `fetchAllWithTimestamp`, `backwardFetchAllWithTimestamp`,
  `allWithTimestamp` and `backwardAllWithTimestamp` - The same arguments as the methods without `WithTimestamp`, and
  entries with `key`, `value` and `timestamp`

The store of a windowed aggregation keeps timestamps unless it is persistent and sets `timestamped: false`. In that
case, the `*WithTimestamp` methods can return `-1` as the timestamp: always without caching, and with `caching: true`
once the entry has left the cache (for example after a restart). Do not pass this timestamp to `put`.
A negative timestamp in `put` raises a `ValueError`, as on a timestamped key-value store.

A window store under `stores:` that only Python functions use is a plain window store, also with `timestamped: true`.
It has no `*WithTimestamp` methods.

### Headers-Aware Stores

With [`dsl.store.format: HEADERS`](#store-format), persistent stores keep the record headers with each value. Python
uses them with the same methods, and gets the same results, as without headers:

- A key-value store has the methods of a [timestamped key-value store](#timestamped-key-value-stores).
- A window store has the methods of a [timestamped window store](#timestamped-window-stores), including the
  `*WithTimestamp` methods. Without `HEADERS`, a window store under `stores` has the methods of a
  [plain window store](#window-store-methods); the methods that both have return the same results.
- A session store has the methods of a [session store](#session-store-methods).

Reads never return headers. A write from Python stores the value with the headers of the record that is being
processed, so the headers stay with the value in the store and its changelog topic, as Kafka Streams does for its own
operations. A write while no record is being processed stores no headers.

### Versioned Key-Value Stores

A key-value store with `versioned: true` keeps the old values of a key for the `historyRetention` time. Unlike on the
other stores, `get` returns a dict, not the value:

- `store.get(key)` - The newest value, as a dict with `value` and `timestamp`, or `None`
- `store.get(key, as_of_timestamp)` - The value that was valid at `as_of_timestamp`, as a dict with `value`,
  `timestamp` and, if a newer value replaced it, `validTo`. `None` if there was no value
- `store.put(key, value, timestamp)` - Store the value from `timestamp` on. Returns the time until which the value is
  valid, `-1` if it is the newest value, or `-9223372036854775808` if it was not written
- `store.delete(key, timestamp)` - Delete the key from `timestamp` on, and return the value that was valid at that
  time as a dict, or `None`

`put` and `delete` do nothing when `timestamp` is older than the newest time the store has seen minus the
`historyRetention`.

### Reading Many Entries with Iterators

Store methods that return many entries return an iterator. Each entry is a dict with `key` and `value`.
The `key` depends on the method:

| Method | `key` of each entry |
|---|---|
| Key-value store: `all()`, `reverseAll()`, `range()`, `reverseRange()`, `prefixScan()` | the key |
| Window store: `fetch(key, timeFrom, timeTo)`, `backwardFetch(key, timeFrom, timeTo)` | the window start time |
| Window store: `all()`, `fetchAll()`, `fetch(keyFrom, keyTo, timeFrom, timeTo)` and their `backward` variants | a dict with `key`, `start`, `end`, `startTime` and `endTime` |
| Session store: `fetch()`, `findSessions()` and their `backward` variants | a dict with `key`, `start`, `end`, `startTime` and `endTime` |

`fetch(key, time)` on a window store returns one value, not an iterator.

Use the iterator like any Python iterator:

```python
for entry in store.all():
  log.info("key={}, value={}", entry["key"], entry["value"])

entries = list(store.all())
```

A store iterator holds resources (for RocksDB stores, native memory) until it is closed. KSML closes it when all
entries are read, and when the KSML function (the function in your definition) returns. So a loop can end early with
`break`, `return` or an exception:

```python
found = None
for entry in store.all():
  if entry["value"] == value:
    found = entry["key"]
    break
```

Do not keep an iterator in a global variable for a later call: it is closed by then, and using it raises an error.
A helper function that the KSML function calls, for example a `def` in `globalCode` or in a module, does not close
its iterators when it returns. They stay open until the KSML function returns. So in a helper that runs a loop, or
to close an iterator before the KSML function returns, use `contextlib.closing()`. A plain
`with store.all() as entries:` does not work.

Iterators also have the methods `hasNext()`, `next()` (returns `None` after the last entry), `peekNextKey()`
(returns the next key without moving forward, or `None`) and `close()`.

## Storage Architecture

### Local Storage vs Changelog Topics

It's important to understand that `persistent` and `logging` control different aspects of state storage:

1. **`persistent: true`** → Uses RocksDB for local storage (survives process restarts)
2. **`persistent: false`** → Uses in-memory storage (lost on process restart)
3. **`logging: true`** → ADDITIONALLY replicates state to a Kafka changelog topic
4. **`logging: false`** → No changelog topic (only local storage)

**Key Point**: When `logging: true`, state is stored **BOTH** locally (RocksDB or memory) **AND** in the changelog topic. The changelog is for fault tolerance and recovery, not primary storage.

### Common Configuration Patterns

| Configuration | Description | Use Case |
|--------------|-------------|----------|
| `persistent: true, logging: false` | RocksDB only (local durability, no fault tolerance) | Development, non-critical state |
| `persistent: true, logging: true` | RocksDB + changelog (local durability + fault tolerance). The default | **Production (Recommended)** ✅ |
| `persistent: false, logging: true` | In-memory + changelog (fast access, fault tolerant) | High-performance with recovery |
| `persistent: false, logging: false` | In-memory only (fastest, data lost on restart) | Temporary caches, testing |

### Changelog Topics

When `logging: true` is configured:

- **Topic Name**: `<application.id>-<store-name>-changelog`
- **Compaction**: Topics are log-compacted to keep only the latest value per key
- **Recovery**: On restart/rebalance, missing local state is rebuilt from the changelog
- **Performance**: Normal reads use local storage (fast), changelog is only for recovery
- **Requirements**: Required for exactly-once semantics

## Performance Considerations

### Caching Impact

| Setting | Behavior | Use When |
|---------|----------|----------|
| `caching: true` | Batches updates, reduces downstream load | High-throughput with acceptable latency |
| `caching: false` | Immediate emission of all updates | Real-time requirements, debugging |

With caching, an operation emits the latest result for a key when Kafka Streams commits (`commit.interval.ms`) or when
the cache is full (`statestore.cache.max.bytes`), instead of emitting every update. Stores that operations materialize
are cached by default; see [Common Parameters](#common-parameters-all-store-types).

### Persistence Trade-offs

| Setting | Pros | Cons | Use When |
|---------|------|------|----------|
| `persistent: true` | Survives restarts, enables recovery | Slower, uses disk space | Production, critical state |
| `persistent: false` | Fast, memory-only | Lost on restart | Temporary state, caching |

### Logging Impact

| Setting | Pros | Cons | Use When |
|---------|------|------|----------|
| `logging: true` | Fault tolerance, fast recovery, exactly-once support | Additional Kafka topics, network/storage overhead | Production, fault tolerance needed |
| `logging: false` | Lower overhead, simpler setup | No recovery on failure | Development, non-critical state |

**Important**: Changelog logging supplements local storage - it does NOT replace it. State is always stored locally (RocksDB or memory) for fast access, with the changelog used only for recovery purposes.