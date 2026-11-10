# Spec Delta

## Purpose

Defines how Python functions read and write the state stores they are connected to, so the same Python code works whichever store format the application is configured with.

## ADDED Requirements

### Requirement: Headers-aware stores offer the timestamped store API
A Python function connected to a headers-aware keyValue or window store SHALL get the same methods, arguments and results as for a timestamped store of that type. A function connected to a headers-aware session store SHALL get the same as for a session store.

#### Scenario: Same calls for both formats
- **WHEN** a Python function calls `get`, `getWithTimestamp`, `all`, `range` and `put` on a keyValue store, once with `dsl.store.format: DEFAULT` and once with `HEADERS`
- **THEN** each call returns the same result in both runs

#### Scenario: Session store in headers format
- **WHEN** a Python function puts an aggregate into a headers-aware session store and then calls `fetchSession` for that session
- **THEN** the call returns the aggregate

### Requirement: Reads never expose headers
Values that Python functions read from a headers-aware store SHALL be the stored values (with their timestamps where the timestamped API returns them), and SHALL NOT include record headers or Kafka Streams wrapper objects.

#### Scenario: Get on a headers-aware store
- **WHEN** a Python function stores the value `{"count": 1}` in a headers-aware keyValue store and then calls `get` for that key
- **THEN** the call returns `{"count": 1}`

### Requirement: Python writes store the current record's headers
When a Python function writes to a headers-aware store while processing a record, the store SHALL keep that record's headers with the value. A write made while no record is being processed SHALL store empty headers.

#### Scenario: Write while processing a record
- **WHEN** a Python function processes a record with header `trace=abc` and puts a value into a headers-aware keyValue store
- **THEN** the stored entry keeps header `trace=abc`, as seen through Kafka Streams interactive queries for the headers-aware store type

#### Scenario: Write without a record
- **WHEN** a Python function writes to a headers-aware store outside of record processing
- **THEN** the stored entry has no headers

### Requirement: Default-option stores keep their Python behavior
For a keyValue store declared without options, the `get` and `all` calls from Python SHALL return the same results as before this change, when such a store was an in-memory store without timestamps.

#### Scenario: Get and all on a default store
- **WHEN** a Python function puts two entries into a keyValue store declared without options, then calls `get` for one key and `all`
- **THEN** `get` returns that entry's value and `all` returns both entries with their keys and values

### Requirement: Window stores used by Python functions keep timestamps
A Python function connected to a window store that is timestamped (the default) SHALL get the timestamped window store API, in both store formats. A window store with `timestamped: false` SHALL keep the plain window store API.

#### Scenario: Timestamped window store without headers
- **WHEN** `dsl.store.format` is absent and a Python function puts a value into a window store declared without options, then calls `fetchWithTimestamp` for that key and window
- **THEN** the call returns the value and the timestamp of the record that was processed

#### Scenario: Window store with timestamped false
- **WHEN** a Python function is connected to a window store declared with `timestamped: false`
- **THEN** the function gets the plain window store methods, without the `*WithTimestamp` methods
