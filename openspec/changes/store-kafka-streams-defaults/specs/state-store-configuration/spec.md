# Spec Delta

## Purpose

Defines how KSML turns declared state store definitions and the runner's Kafka configuration into Kafka Streams state stores, so stores behave like Kafka Streams stores unless the user says otherwise.

## ADDED Requirements

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

### Requirement: Store format follows dsl.store.format
When the runner's Kafka config sets `dsl.store.format` to `HEADERS`, declared stores that are persistent, timestamped and not versioned SHALL be built in the headers-aware format. Other stores, and all stores when the setting is absent or `DEFAULT`, SHALL keep the non-headers format.

#### Scenario: Headers format for a default store
- **WHEN** `dsl.store.format` is `HEADERS` and a keyValue, window or session store is declared without options
- **THEN** the store is built in the headers-aware format

#### Scenario: Default format
- **WHEN** `dsl.store.format` is absent or `DEFAULT` and a keyValue store is declared without options
- **THEN** the store is a timestamped store without headers

#### Scenario: Stores excluded from the headers format
- **WHEN** `dsl.store.format` is `HEADERS` and a store is declared with `persistent: false`, with `timestamped: false`, or with `versioned: true`
- **THEN** the store is built without the headers-aware format

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
The two declared window stores of a stream-stream join SHALL always log to changelog topics, as Kafka Streams does for join stores that the application provides. When a join store sets `logging: false`, KSML SHALL log a warning that names the store and says that only the shared store's changelog is turned off.

#### Scenario: Window stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** both window stores have changelog topics and a warning naming each store is logged

### Requirement: Existing window store state reads correctly after the upgrade
Window store state written by KSML 1.x SHALL read correctly after the upgrade to this version, both from local state and after a restore from the changelog topic.

#### Scenario: Local state from KSML 1.x
- **WHEN** a KSML 1.x application with a persistent window store wrote values from Python, and this version starts with the same application id and state directory
- **THEN** Python reads the same values from the store as before the upgrade

#### Scenario: Restore from the changelog
- **WHEN** the same application starts on this version without local state
- **THEN** the store is restored from the changelog and Python reads the same values as before the upgrade

#### Scenario: Cleanup runs once
- **WHEN** this version has already started on a state directory and restarts
- **THEN** it keeps the local state of its window stores
