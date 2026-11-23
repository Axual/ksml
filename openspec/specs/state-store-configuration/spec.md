# state-store-configuration Specification

## Purpose
Defines how KSML turns declared state store definitions and the runner's Kafka configuration into Kafka Streams state stores, so stores behave like Kafka Streams stores unless the user says otherwise.

## Requirements

### Requirement: Declared stores default to persistent, logged and timestamped
A declared keyValue, window or session store that leaves out `persistent`, `logging` or `timestamped` SHALL be built as if that option were `true`. An option set explicitly SHALL keep its value.

#### Scenario: Store without options
- **WHEN** a keyValue store is declared with only a name, key type and value type
- **THEN** the store is persistent, timestamped and logs to a changelog topic

#### Scenario: Explicit false is respected
- **WHEN** a window store is declared with `persistent: false` and `logging: false`
- **THEN** the store is kept in memory and has no changelog topic

#### Scenario: Window store timestamped by default
- **WHEN** a window store is declared without `timestamped`
- **THEN** the store is a timestamped window store

### Requirement: Caching default depends on how the store is used
A declared store that leaves out `caching` SHALL be cached when a DSL operation materializes it, and SHALL NOT be cached when it is connected to Python functions. An explicit `caching` value SHALL apply in both cases.

#### Scenario: Store materialized by an aggregation
- **WHEN** a keyValue store without `caching` is used as the store of an `aggregate` operation
- **THEN** the store is cached

#### Scenario: Store used by Python functions
- **WHEN** a keyValue store without `caching` is declared under `stores` and used by a Python function
- **THEN** the store is not cached

#### Scenario: Explicit caching
- **WHEN** a store with `caching: false` is used as the store of a `count` operation
- **THEN** the store is not cached

### Requirement: Versioned stores are persistent by default
A keyValue store declared with `versioned: true` that leaves out `persistent` SHALL be built as a persistent versioned store with a changelog topic.

#### Scenario: Versioned store without persistent
- **WHEN** a keyValue store is declared with `versioned: true` and a `historyRetention`, and without `persistent`
- **THEN** the store is a versioned store that logs to a changelog topic

### Requirement: Timestamped on session stores warns
When a session store definition sets `timestamped: true`, KSML SHALL log a warning that names the store and says the setting has no effect, and SHALL still build the store. `timestamped: false` SHALL NOT log a warning.

#### Scenario: Session store with timestamped
- **WHEN** a session store is declared with `timestamped: true`
- **THEN** a warning naming the store is logged and the topology is built

#### Scenario: Session store with timestamped false
- **WHEN** a session store is declared with `timestamped: false`
- **THEN** no warning is logged

### Requirement: Store state is kept in ksml.storageDirectory
The runner SHALL use `ksml.storageDirectory` as the Kafka Streams `state.dir`. When the `kafka` section sets another `state.dir`, the runner SHALL log a warning that names both directories, and SHALL still use `ksml.storageDirectory`.

#### Scenario: state.dir in the kafka section
- **WHEN** the `kafka` section sets `state.dir` to a directory other than `ksml.storageDirectory`
- **THEN** a warning naming both directories is logged and Kafka Streams uses `ksml.storageDirectory`

#### Scenario: No state.dir, or the same one
- **WHEN** the `kafka` section leaves out `state.dir`, or sets it to `ksml.storageDirectory`
- **THEN** no warning is logged

### Requirement: Store format follows dsl.store.format
When the runner's Kafka config sets `dsl.store.format` to `HEADERS`, declared keyValue and window stores that are persistent, timestamped and not versioned, and declared session stores that are persistent, SHALL be built in the headers-aware format. Other stores, and all stores when the setting is absent or `DEFAULT`, SHALL keep the non-headers format.

#### Scenario: Headers format for a default store
- **WHEN** `dsl.store.format` is `HEADERS` and a keyValue, window or session store is declared without options
- **THEN** the store is built in the headers-aware format

#### Scenario: Default format
- **WHEN** `dsl.store.format` is absent or `DEFAULT` and a keyValue store is declared without options
- **THEN** the store is a timestamped store without headers

#### Scenario: Stores excluded from the headers format
- **WHEN** `dsl.store.format` is `HEADERS` and a keyValue or window store is declared with `persistent: false`, with `timestamped: false`, or with `versioned: true`, or a session store is declared with `persistent: false`
- **THEN** the store is built without the headers-aware format

#### Scenario: Session store with timestamped false
- **WHEN** `dsl.store.format` is `HEADERS` and a persistent session store is declared with `timestamped: false`
- **THEN** the store is built in the headers-aware format, because session stores are not timestamped

### Requirement: Shared join store follows the join stores' logging
The shared store of a stream-stream left or outer join SHALL NOT log to a changelog topic when both declared join stores have `logging` false, and SHALL log in all other cases.

#### Scenario: Join stores with default logging
- **WHEN** a pipeline uses an outer join between two streams and declares `thisStore` and `otherStore` without `logging`
- **THEN** the outer join's shared store has a changelog topic

#### Scenario: Join stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** the outer join's shared store has no changelog topic

#### Scenario: Join stores with different logging
- **WHEN** an outer join between two streams declares `thisStore` with `logging: true` and `otherStore` with `logging: false`
- **THEN** the topology is built and the outer join's shared store has a changelog topic

### Requirement: Join window stores always log
The two declared window stores of a stream-stream join SHALL always log to changelog topics, as Kafka Streams does for join stores that the application provides.

#### Scenario: Window stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** both window stores have changelog topics

### Requirement: Join store logging is reported
When `logging: false` on a join store has no effect, KSML SHALL log a warning that names the store: on an inner join always, and on a left or outer join when the other join store does not set `logging: false`, and then the warning also names that other store. When both join stores of a left or outer join set it, KSML SHALL log an info line, and no warning, that says only the shared store has no changelog topic.

#### Scenario: Inner join stores with logging off
- **WHEN** an inner join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** a warning naming each store is logged

#### Scenario: Outer join stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** an info line and no warning is logged

#### Scenario: Outer join store with logging off on one side
- **WHEN** an outer join between two streams declares `thisStore` with `logging: true` and `otherStore` with `logging: false`
- **THEN** a warning naming `otherStore` and `thisStore` is logged

### Requirement: Existing window store state reads correctly after the upgrade
Window store state that KSML 1.x wrote from Python with a changelog SHALL read correctly after the upgrade to this version: the local state of a plain store as it is, the state of a store with `timestamped: true` after KSML deletes its local state once and Kafka Streams restores it from the changelog topic, and every store after a restore without local state. KSML SHALL decide on the store options as declared, because KSML 1.x read options that a definition leaves out as `false`.

#### Scenario: Local state from KSML 1.x
- **WHEN** a KSML 1.x application with persistent window stores used by Python, with `logging: true`, one with `timestamped: true` and one without `timestamped`, wrote values from Python, and this version starts with the same application id and state directory
- **THEN** KSML deletes the local state of the store with `timestamped: true` once and Kafka Streams restores it from the changelog, KSML keeps the local state of the other store, and Python reads the same values from both stores as before the upgrade

#### Scenario: Restore from the changelog
- **WHEN** the same application starts on this version without local state
- **THEN** the store is restored from the changelog and Python reads the same values as before the upgrade

#### Scenario: Cleanup runs once
- **WHEN** this version has already started on a state directory and restarts
- **THEN** it keeps the local state of its window stores

### Requirement: KSML stops on KSML 1.x local state that would be lost
On the first start on an application state directory, before Kafka Streams starts, KSML SHALL stop with an error that names each store at risk, and SHALL change nothing, when a checked declared store (see "The upgrade check leaves out stores that had a changelog in KSML 1.x") has local state that would be lost: it is persistent and leaves out `logging`, or it is a persistent window store for Python with `timestamped: true` and without `logging: true`.

#### Scenario: Persistent store without logging
- **WHEN** a KSML 1.x application left local state for a persistent store that leaves out `logging`, and this version starts on that state directory
- **THEN** KSML stops with an error that names the store and asks to set `logging`, and the local state is unchanged

#### Scenario: Persistent store without logging with exactly-once
- **WHEN** the same happens with `processing.guarantee: exactly_once_v2` and without transactional state stores, and a store with `logging: false` has local state in the same task
- **THEN** the error also says that `logging: true` makes Kafka Streams delete the task, names that store, and asks to run with `processing.guarantee: at_least_once` until the store has written to its new changelog

#### Scenario: Logging set after the stop
- **WHEN** the store then sets `logging: false` and this version starts again
- **THEN** KSML starts and Python reads the values that KSML 1.x left in the local state

#### Scenario: Unreadable window store without changelog
- **WHEN** a KSML 1.x application left local state for a persistent window store for Python with `timestamped: true` and without `logging: true`, and this version starts on that state directory
- **THEN** KSML stops with an error that names the files to delete, and does not delete them

#### Scenario: Stores without data
- **WHEN** a KSML 1.x application left RocksDB files without data (no `.sst` or `.blob` file) for a persistent store that leaves out `logging`, and this version starts on that state directory
- **THEN** KSML starts, as there is nothing to lose

### Requirement: The upgrade check runs once per state directory
After a passed check of local state on the first start, KSML SHALL not check that application state directory again.

#### Scenario: New application
- **WHEN** this version starts on a state directory without local state for its stores
- **THEN** KSML starts, and later starts do not check the state again

### Requirement: KSML stops when exactly-once would delete KSML 1.x local state
With exactly-once processing without transactional state stores, KSML SHALL stop on the first start on an application state directory, before Kafka Streams starts, and SHALL change nothing, when KSML would delete the 1.x local state of a store in a task where a checked store with `logging: false` has local state, because Kafka Streams would then delete the whole task.

#### Scenario: Exactly-once would delete a store that keeps its state
- **WHEN** with `processing.guarantee: exactly_once_v2` and without transactional state stores, KSML would delete the 1.x local state of a window store with `timestamped: true` and `logging: true`, and a store with `logging: false` has local state in the same task
- **THEN** KSML stops with an error that names the task and both stores and asks to start once with `processing.guarantee: at_least_once`, and the local state is unchanged

### Requirement: KSML stops on KSML 1.x local state of a shared join store
On the first start on an application state directory, before Kafka Streams starts, KSML SHALL stop with an error that names the store, and SHALL change nothing, when the shared store of a stream-stream left or outer join has local state, `thisStore` is persistent and does not set `logging: true`, and the join stores do not both set `logging: false`. In KSML 1.x this store had no changelog then, and now it gets one.

#### Scenario: Shared store of a join without logging
- **WHEN** a KSML 1.x application left local state for the shared store of an outer join whose persistent `thisStore` leaves out `logging`, and this version starts on that state directory
- **THEN** KSML stops with an error that names the shared store and asks to set `logging: false` on both join stores or `logging: true` on `thisStore`, and the local state is unchanged

#### Scenario: Shared store of a join that keeps the KSML 1.x behavior
- **WHEN** both join stores then set `logging: false` and this version starts again
- **THEN** KSML starts, and the shared store keeps its local state without a changelog

### Requirement: The upgrade check leaves out stores that had a changelog in KSML 1.x
The check of local state on the first start SHALL leave out the two window stores of stream-stream joins, which Kafka Streams always logged, the stores of global tables, and the stores of tables when `topology.optimization` includes `reuse.ktable.source.topics`, as `all` does. Kafka Streams restores the stores of tables and global tables from their input topic, whatever `logging` says.

#### Scenario: Table store with topology optimization
- **WHEN** a KSML 1.x application left local state for the persistent store of a table that leaves out `logging`, with `topology.optimization` set to `all` (the default) or to a list with `reuse.ktable.source.topics`, and this version starts on that state directory
- **THEN** KSML does not stop for that store, because Kafka Streams restores it from the input topic of the table

#### Scenario: Table store without topology optimization
- **WHEN** a KSML 1.x application ran with `topology.optimization: none` and left local state for the persistent store of a table that leaves out `logging`, and this version starts on that state directory
- **THEN** KSML stops with an error that names the store and asks to set `logging`
