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
When a session store definition sets `timestamped`, KSML SHALL log a warning that names the store and says the setting has no effect, and SHALL still build the store.

#### Scenario: Session store with timestamped
- **WHEN** a session store is declared with `timestamped: true`
- **THEN** a warning naming the store is logged and the topology is built

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
The shared store of a stream-stream left or outer join SHALL log to a changelog topic when both declared join stores have `logging` true (the default), and SHALL NOT log when both have `logging` false. If the two join stores of any stream-stream join have different `logging` values, building the topology SHALL fail with an error that names both stores.

#### Scenario: Join stores with default logging
- **WHEN** a pipeline uses an outer join between two streams and declares `thisStore` and `otherStore` without `logging`
- **THEN** the outer join's shared store has a changelog topic

#### Scenario: Join stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** the outer join's shared store has no changelog topic

#### Scenario: Conflicting join store logging
- **WHEN** a stream-stream join declares `thisStore` with `logging: true` and `otherStore` with `logging: false`
- **THEN** building the topology fails with an error that names both stores

### Requirement: Join window stores always log
The two declared window stores of a stream-stream join SHALL always log to changelog topics, as Kafka Streams does for join stores that the application provides. When a join store sets `logging: false`, KSML SHALL log a warning that names the store and says that only the shared store's changelog is turned off.

#### Scenario: Window stores with logging off
- **WHEN** an outer join between two streams declares `thisStore` and `otherStore` with `logging: false`
- **THEN** both window stores have changelog topics and a warning naming each store is logged
