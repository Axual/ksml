# punctuation Specification

## Purpose
Lets a KSML pipeline run a Python function on a wall-clock or stream-time schedule, with access to state stores and a selection of the Kafka Streams processor context, like a Kafka Streams `Punctuator`.

## Requirements

### Requirement: Punctuate operation
A pipeline SHALL accept an operation with `type: punctuate` in its `via` list when the input is a stream. The operation SHALL require `interval`, `punctuationType` and `punctuator`, and SHALL accept an optional `name`. Records that reach the operation SHALL be forwarded unchanged, with the same key, value, timestamp and headers.

#### Scenario: Records pass through
- **WHEN** a stream pipeline contains a `punctuate` operation and a record arrives
- **THEN** the same record SHALL be forwarded to the next operation, unchanged

#### Scenario: Missing required field
- **WHEN** a `punctuate` operation omits `interval`, `punctuationType` or `punctuator`
- **THEN** KSML SHALL reject the definition at startup with an error naming the missing field

#### Scenario: Punctuate on a table
- **WHEN** a `punctuate` operation follows a table, a grouped stream or a windowed stream
- **THEN** KSML SHALL reject the definition at startup with an error stating that `punctuate` needs a stream

### Requirement: Schedule interval
The `interval` SHALL be a KSML duration of at least 1 millisecond. The punctuator SHALL be scheduled once for each stream task that runs the operation.

#### Scenario: Valid interval
- **WHEN** `interval` is `30s`
- **THEN** the punctuator SHALL be scheduled every 30 seconds

#### Scenario: Interval too small
- **WHEN** `interval` is `0` or negative
- **THEN** KSML SHALL reject the definition at startup with an error about the interval

#### Scenario: One schedule per task
- **WHEN** the input topic has 3 partitions and the application runs 3 stream tasks for the operation
- **THEN** the punctuator SHALL run once per interval in each of the 3 tasks, with access to that task's stores

### Requirement: Wall-clock punctuation
With `punctuationType: wallClockTime`, the punctuator SHALL be called each time the interval has passed in system time, also when no records arrive.

#### Scenario: No input records
- **WHEN** a `wallClockTime` punctuator has an interval of `1m` and no records arrive for 3 minutes
- **THEN** the punctuator SHALL be called about 3 times

### Requirement: Stream-time punctuation
With `punctuationType: streamTime`, the punctuator SHALL be called with the first record the task processes, and after that when the stream time of the task, derived from the timestamps of the records it processes, has advanced by at least the interval since the previous call, as Kafka Streams does.

#### Scenario: Stream time advances
- **WHEN** a `streamTime` punctuator has an interval of `1m` and records with timestamps 0 s, 30 s and 61 s arrive
- **THEN** the punctuator SHALL be called after the record at 0 s and after the record at 61 s, and not after the record at 30 s

#### Scenario: No input records
- **WHEN** a `streamTime` punctuator is scheduled and no records arrive
- **THEN** the punctuator SHALL NOT be called

#### Scenario: Unknown punctuation type
- **WHEN** `punctuationType` is a value other than `wallClockTime` or `streamTime`
- **THEN** KSML SHALL reject the definition at startup with an error listing the valid values

### Requirement: Punctuator function type
KSML SHALL provide a function type `punctuator`, usable inline or by name from `functions:`, with `globalCode` and custom parameters like other function types. It SHALL be called with one parameter, `timestamp`, the punctuation time in milliseconds since the epoch, and SHALL have the punctuation context available as the variable `context`, in the same way as the state stores are available by their names. It SHALL NOT return a result; records are emitted with `context.forward`.

#### Scenario: Named punctuator
- **WHEN** `functions:` defines `emit_totals` with `type: punctuator` and a `punctuate` operation sets `punctuator: emit_totals`
- **THEN** KSML SHALL call `emit_totals` with the punctuation `timestamp` and a `context` on each punctuation

#### Scenario: Wrong function type
- **WHEN** a `punctuate` operation refers to a function of another type, such as a `predicate`
- **THEN** KSML SHALL reject the definition at startup with an error about the function type

#### Scenario: Result type declared
- **WHEN** a `punctuator` function declares a `resultType`
- **THEN** KSML SHALL reject the definition at startup with an error stating that a punctuator returns no result

#### Scenario: Returned value is ignored
- **WHEN** the punctuator code returns a value
- **THEN** the value SHALL NOT be forwarded, and KSML SHALL log a warning once per operation that punctuators emit records with `context.forward`

### Requirement: Punctuation context methods
The `context` SHALL offer exactly these methods: `forward`, `commit`, `currentSystemTimeMs`, `currentStreamTimeMs`, `applicationId` and `taskId`. Other `ProcessorContext` methods, such as scheduling, looking up stores by name, the application config, serdes and metrics, SHALL NOT be reachable from Python.

#### Scenario: Current times
- **WHEN** the punctuator calls `context.currentSystemTimeMs()` and `context.currentStreamTimeMs()`
- **THEN** it SHALL get the task's current system time and stream time in milliseconds, as Kafka Streams reports them

#### Scenario: Commit request
- **WHEN** the punctuator calls `context.commit()`
- **THEN** Kafka Streams SHALL commit the task as soon as possible, as with `ProcessorContext.commit()`

#### Scenario: Application and task id
- **WHEN** the punctuator calls `context.applicationId()` and `context.taskId()`
- **THEN** it SHALL get the application id and the task id as strings, for example `"0_2"`

#### Scenario: Hidden method
- **WHEN** the punctuator calls `context.schedule(...)` or `context.appConfigs()`
- **THEN** Python SHALL raise an error that the attribute does not exist

### Requirement: Context is only valid during the call
The `context` SHALL only be usable during the punctuator call that received it.

#### Scenario: Context kept for later
- **WHEN** a punctuator stores `context` in a global variable and another function calls `forward` on it later
- **THEN** the call SHALL raise a Python error stating that the context is no longer valid, and no record SHALL be forwarded

### Requirement: Forwarding records from a punctuator
`context.forward(key, value, timestamp=None, headers=None, to=None)` SHALL send a record to the next operation in the pipeline, or to the operation named by `to`. Without `timestamp`, the record SHALL get the punctuation timestamp. `headers` SHALL use the format of `metadataTransformer` (a list of dicts with `key` and `value`); without it the record SHALL have no headers. A `None` value SHALL be forwarded as a tombstone. Records SHALL be forwarded in call order.

#### Scenario: Periodic emission
- **WHEN** a punctuator calls `context.forward("a", 10)` and then `context.forward("b", 20)`, and the pipeline ends with `to: totals`
- **THEN** the records `("a", 10)` and `("b", 20)` SHALL be written to topic `totals` in that order, with the punctuation timestamp

#### Scenario: Explicit timestamp and headers
- **WHEN** a punctuator calls `context.forward("a", 10, timestamp=1700000000000, headers=[{"key": "source", "value": "flush"}])`
- **THEN** the record SHALL have timestamp `1700000000000` and one header `source` with value `flush`

#### Scenario: Punctuate is the last operation
- **WHEN** `punctuate` is the last operation in `via` and the punctuator calls `context.forward("a", 10)` without `to`
- **THEN** the record SHALL go directly to the pipeline's end result, such as the topic in `to`, a `forEach` or an `as`

#### Scenario: Emitted records are processed by later operations
- **WHEN** a `filter` follows the `punctuate` operation
- **THEN** records forwarded by the punctuator SHALL pass through the `filter` like records from the input

### Requirement: Forwarding to a named operation
`context.forward` SHALL accept an optional `to` argument with the name of an operation. The record SHALL then go directly to that operation, skipping the operations in between. Every operation that comes after the `punctuate` operation in the same pipeline's `via` list, has a `name`, takes a stream as input, and does not come after an operation that repartitions (`repartition`, `groupBy`, `groupByKey`, `toTable`, or a join with a stream or table) SHALL be a valid target. Without `to`, the record SHALL go to the next operation.

#### Scenario: Skip operations
- **WHEN** the `via` list is `punctuate`, `transformValue` named `compute`, `transformValue` named `enrich`, and the punctuator calls `context.forward("a", 10, to="enrich")`
- **THEN** the record SHALL be processed by `enrich` and the operations after it, and SHALL NOT be processed by `compute`

#### Scenario: Input records are not affected
- **WHEN** a pipeline has a `punctuate` operation and a named operation further down
- **THEN** records from the input SHALL still pass through every operation in order, and SHALL reach the named operation exactly once

#### Scenario: Unknown or unreachable name
- **WHEN** `to` names an operation that does not exist in the pipeline, comes before the `punctuate` operation, is in another pipeline, or takes a table or grouped stream as input
- **THEN** `forward` SHALL raise a Python error that lists the valid target names, and no record SHALL be forwarded

#### Scenario: Unnamed operation
- **WHEN** an operation further down the pipeline has no `name`
- **THEN** it SHALL NOT be a target of `forward(..., to=...)`; it SHALL only be reached as the next operation or through the operations before it

### Requirement: Types of forwarded records
The key and value passed to `context.forward` SHALL match the input key and value types of the operation that receives the record: the stream at the `punctuate` operation without `to`, or the input of the named operation with `to`. The same conversion rules as for the result of other transformer functions SHALL apply. A key or value that does not match SHALL make `forward` raise a Python error, and the record SHALL NOT be forwarded.

#### Scenario: Types of a named target
- **WHEN** the stream at `punctuate` has value type `long`, the operation before `enrich` turns it into `string`, and the punctuator calls `context.forward("a", "x", to="enrich")`
- **THEN** the record SHALL be forwarded, because `enrich` takes `string` values

#### Scenario: Matching types
- **WHEN** the stream has key type `string` and value type `long`, and the punctuator calls `context.forward("a", 10)`
- **THEN** the record SHALL be forwarded

#### Scenario: Value of the wrong type
- **WHEN** the stream has value type `long` and the punctuator calls `context.forward("a", "ten")`
- **THEN** `forward` SHALL raise a Python error naming the expected and actual type, and the record SHALL NOT be forwarded

#### Scenario: Malformed headers
- **WHEN** `headers` is not a list of dicts with `key` and `value`
- **THEN** `forward` SHALL raise a Python error, and the record SHALL NOT be forwarded

### Requirement: State store access during a punctuation
A punctuator SHALL have access to the stores listed in the function's `stores`, with the same Python store methods as other functions. Writes to a timestamped store without an explicit timestamp SHALL use the punctuation timestamp.

#### Scenario: Read and clear a store
- **WHEN** an earlier operation adds to key-value store `totals`, and the punctuator iterates over `totals`, forwards each entry and deletes it
- **THEN** the entries SHALL be forwarded and SHALL no longer be in `totals` after the punctuation

#### Scenario: Write to a timestamped store
- **WHEN** a punctuator calls `put` on a store with `timestamped: true` without a timestamp
- **THEN** the entry SHALL be stored with the punctuation timestamp

#### Scenario: Undeclared store
- **WHEN** a punctuator uses a store that is not listed in the function's `stores`
- **THEN** the store SHALL NOT be available to the function

### Requirement: Errors in a punctuator
An exception raised by a punctuator SHALL be handled the same way as an exception raised by a function in another pipeline operation, under the same error handling configuration. The log message SHALL name the `punctuate` operation and SHALL state that the error happened during a punctuation, not while processing a record. Records forwarded before the exception SHALL stay forwarded.

#### Scenario: Same outcome as other functions
- **WHEN** a punctuator raises an exception, and a `transformValue` mapper raising an exception under the same configuration would stop Kafka Streams
- **THEN** Kafka Streams SHALL stop in the same way

#### Scenario: Error is logged without a record
- **WHEN** a punctuator raises an exception
- **THEN** the error SHALL be logged with the operation name and the punctuation timestamp

#### Scenario: Error after a forward
- **WHEN** a punctuator forwards one record and then raises an exception
- **THEN** the forwarded record SHALL reach the next operation, as with a Kafka Streams `Punctuator` that throws after `forward`
