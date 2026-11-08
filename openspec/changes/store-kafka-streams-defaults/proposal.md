# Proposal

## Why

KSML builds declared state stores with its own defaults: in memory, no changelog, not timestamped and not cached. Kafka Streams defaults to persistent, logged and timestamped stores, and caches DSL stores. As a result, a KSML application silently loses state on restart unless the user knows to switch every option on. KSML also ignores the Kafka Streams `dsl.store.format` setting, and stream-stream joins never get a changelog. KSML 2.0 is the release where these defaults can change (AXPD-12445, findings F06, F13 and F26).

## What Changes

- **BREAKING** A declared store (keyValue, window or session) that leaves out `persistent`, `logging` or `timestamped` gets `true`, the Kafka Streams default. Today it gets `false`. An explicit `false` is still respected.
- **BREAKING** A declared store that leaves out `caching` follows Kafka Streams. Caching is on when a DSL operation (for example `aggregate`, `count` or a table) materializes the store, and off when Python functions use the store. Today caching is always off.
- A versioned keyValue store that leaves out `persistent` is now a real versioned store. Today it silently falls back to a plain in-memory store.
- Setting `timestamped` on a session store logs a warning, because Kafka Streams has no timestamped session store and the setting has no effect.
- **BREAKING** Stream-stream joins (inner, left and outer) log their join stores to changelog topics by default, as in Kafka Streams. Declared join stores control this through `logging`. If the two declared join stores ask for different `logging` values, the topology is rejected with a clear error.
- With `dsl.store.format: HEADERS` in the runner's Kafka config, persistent, timestamped, non-versioned declared stores are built in the headers-aware format. With the default setting (`DEFAULT`) nothing changes. In-memory stores, versioned stores and stores with `timestamped: false` are not affected.
- Python functions can use headers-aware stores with the same API and results as timestamped stores. Reads return values, never headers. A write from Python stores the current record's headers with the value, or no headers when no record is being processed.
- Documentation and a migration section explain the new defaults, the extra changelog topics and ACLs, RocksDB state on disk, caching emission timing, and how to opt out.

## Capabilities

### New Capabilities

- `state-store-configuration`: how KSML turns a declared store definition (and the runner's Kafka config) into a store, including its defaults, format, validation and the logging of stream-stream join stores.
- `python-state-store-api`: how Python functions read and write the state stores they are connected to, here specifically stores in the headers-aware format.

### Modified Capabilities

None. No existing spec covers state stores.

## Impact

- Code: `ksml` store definitions and parsers, `StoreUtil`, `BaseOperation` (stream join), `TopologyGenerator` / `TopologyBuildContext` (store format plumbing), the store proxies under `proxy/store` and the operation processors. `ksml-runner` passes `dsl.store.format` on. `ksml-test-runner` keeps the default format.
- Runtime: declared stores now create changelog topics, so applications need topic-create rights or pre-created topics. They also keep RocksDB state under `state.dir`, and DSL results from cached stores are emitted on commit or cache flush instead of on every update.
- Tests: tests that rely on the old defaults need explicit flags or new expectations, and more tests run RocksDB in `TopologyTestDriver`.
- Docs: `docs/reference/state-store-reference.md`, `docs/reference/definition-reference.md`, `docs/tutorials/intermediate/state-stores.md`, `docs/tutorials/advanced/performance-optimization.md`, `docs/migration-to-2.0.0.md`, `docs/release-notes.md`.
- Dependencies: none new. This uses the headers-aware store APIs that Kafka Streams 4.4.0 already provides.
- Depends on AXPD-11068 (timestamped store Python API), which this branch is built on.
