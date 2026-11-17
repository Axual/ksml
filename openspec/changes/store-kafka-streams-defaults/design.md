# Design

## Context

See proposal.md (Why) for motivation, and the two delta specs for the required behavior.

How declared stores are built today (Kafka Streams 4.4.0):

```
 store definition (YAML)
        |
        v
 StateStoreDefinition            null -> false for persistent/timestamped/caching/logging
        |
   +----+-------------------------------+
   |                                    |
 DSL use (aggregate, count,          processor use (functions with `stores:`)
 table with store, ...)                 |
   |                                    v
   v                                 StoreUtil.getStoreBuilder -> Stores.*Builder
 StoreUtil.materialize                  |
 -> Materialized.as(supplier)           v
   |                                 OperationProcessor.init
   v                                 -> ProxyUtil.wrapStateStore -> Python proxy
 KS uses the given supplier and
 ignores dsl.store.format
```

Facts that shape the approach:

- KSML always hands Kafka Streams an explicit store supplier. That makes Kafka Streams skip `dsl.store.format` and `default.dsl.store` for declared stores, so KSML has to choose the format itself.
- Kafka Streams has two caching defaults: `Materialized` is cached, `Stores.*Builder` is not.
- Headers-aware suppliers exist only for persistent stores: `Stores.persistentTimestampedKeyValueStoreWithHeaders`, `persistentTimestampedWindowStoreWithHeaders` and `persistentSessionStoreWithHeaders`. Kafka Streams' own in-memory DSL suppliers ignore the format.
- For DSL use, Kafka Streams' materializers pick the headers builder when the supplier is a `HeadersBytesStoreSupplier`. Picking the supplier is therefore enough for DSL stores.
- For processor use, a headers store appears as `KeyValueStore<K, ValueTimestampHeaders<V>>`, `WindowStore<K, ValueTimestampHeaders<V>>` or `SessionStore<K, AggregationWithHeaders<V>>`. `ProxyUtil.wrapStateStore` would treat these as plain stores and hand Kafka Streams objects to Python.
- Interactive queries (`ksml-query`) already work: Kafka Streams' store providers put headers stores behind facades for the plain `keyValueStore()` and `windowStore()` query types.
- `StreamJoined` has one logging flag for all join stores. KSML calls `withLoggingDisabled()` on every join and only enables logging when `thisStore.logging` is set.
- Operations without a declared store pass `null` materialization, so they already get Kafka Streams defaults and are out of scope.
- Implicit table and globalTable stores (`TopologyBuildContext`, no declared store) are built with explicit `false` values and are not declared stores, so they don't change.

## Goals / Non-Goals

**Goals:**

- Decide every default in one place, so DSL and processor stores can't drift apart.
- One explicit format decision in `StoreUtil`, used by DSL, processor and join stores.
- Python code written for timestamped stores runs unchanged on headers stores.

**Non-Goals:**

- A Python API for reading or writing headers on stores.
- Headers support for in-memory or versioned stores (Kafka Streams has none).
- Supporting `dsl.store.format` in `ksml-test-runner`: it has no Kafka config, so it uses `DEFAULT`. `testing-your-pipeline.md` and the Store Format section of the state store reference say so (PR #767 review 03, item 11). Python gets the same results in both formats, so a new test file field is not worth it.
- Changing implicit table or globalTable stores, or stores of operations without a declared store.
- Migrating existing state when users switch `dsl.store.format`. The upgrade of 1.x window store state is covered by D7.

## Decisions

### D1. Resolve persistent/logging/timestamped in the definition; keep caching nullable

`StateStoreDefinition` turns `null` into `true` for `persistent`, `logging` and `timestamped` in its constructor, so the `boolean` getters and their callers stay as they are. `caching` becomes a `Boolean` that stays `null` when not set. `StoreUtil` resolves it per use: `materialize(...)` turns `null` into `true`, `getStoreBuilder(...)` turns `null` into `false`. Copies (`with(...)`, `BaseOperation.validate*Store`) pass the raw `Boolean` along so an unset value is not frozen early.

- *Alternative: make every flag nullable and resolve all of them in `StoreUtil`.* That touches many more callers for no gain, because only caching depends on the use site.
- *Alternative: one caching default for both uses.* Rejected: it wouldn't match Kafka Streams, which was the decision in explore.

The constructor `KeyValueStateStoreDefinition(name, keyType, valueType)` and the implicit table stores keep passing explicit values, so their behavior doesn't change.

### D2. Warn about session `timestamped` at parse time

`SessionStateStoreDefinitionParser` logs a warning when `timestamped: true` is set. `timestamped: false` gives no warning: users set it to keep the 1.x behavior. The definition keeps the value, but it has no effect. Removing the field would break existing YAML and the JSON schema.

### D3. Versioned stores rely on the persistent default

Because of D1, `versioned: true` without `persistent` reaches the versioned branch in `StoreUtil`. If the user sets `persistent: false` together with `versioned: true`, the behavior stays as it is today (an in-memory store) and is documented. Rejecting that combination is out of scope.

### D4. Plumb the store format as a headers flag

`KafkaStreamsRunner` already builds a `StreamsConfig`, which validates `dsl.store.format` (case-insensitive `DEFAULT` or `HEADERS`). It reads `streamsConfig.getString(DSL_STORE_FORMAT_CONFIG).equalsIgnoreCase(DSL_STORE_FORMAT_HEADERS)`, the same check Kafka Streams makes. The resulting `boolean headersStoreFormat` goes through a new `TopologyGenerator` constructor argument, then `TopologyBuildContext`, then the `StoreUtil` calls. A boolean fits better than Kafka Streams' `DslStoreFormat` enum, which has no value for "default" (it has `PLAIN`, `TIMESTAMPED` and `HEADERS`). The `TopologyGenerator` constructors without the flag, `(applicationId)` and `(applicationId, optimization, pythonContextConfig)`, stay and use `false`, the Kafka Streams `DEFAULT` format, so `ksml-test-runner` and the tests compile unchanged. The lower-level `TopologyBuildContext` constructors and the `StoreUtil` methods lose their old signatures instead, also the `TopologyBuildContext(builder, resources)` constructor without a `PythonContextConfig` (PR #767 review 04, item 7): an old overload that passes `false` would build a non-headers store inside a topology that uses the headers format, and a compile error is safer than mixed formats. The release notes and the migration guide list the changed signatures (PR #767 review 03, item 10). Inside `StoreUtil`, one helper decides `useHeaders(store) = headersStoreFormat && persistent && timestamped && !versioned`, and each supplier/builder method branches on it:

| Store | Supplier (headers) | Processor builder (headers) |
|---|---|---|
| keyValue | `persistentTimestampedKeyValueStoreWithHeaders` | `timestampedKeyValueStoreWithHeadersBuilder` |
| window | `persistentTimestampedWindowStoreWithHeaders` | `timestampedWindowStoreWithHeadersBuilder` |
| session | `persistentSessionStoreWithHeaders` | `sessionStoreWithHeadersBuilder` |

Session stores have no timestamped variant, so for them `useHeaders` checks only `persistent`. Join window stores use the same window supplier helper, and Kafka Streams wraps them in its headers join builder itself.

- *Alternative: `Materialized.as(DslStoreSuppliers)` / `StreamJoined.withDslStoreSuppliers`, letting Kafka Streams pick the format.* Rejected: it can't express versioned stores, `timestamped: false` or validated join suppliers, and processor stores go through `StoreBuilder` anyway. We would end up with two mechanisms instead of one.
- *Alternative: read the config from the `StreamsBuilder`.* Not possible, because the topology config isn't public on `StreamsBuilder`.

### D5. Adapters that show headers stores as the store types Python already knows

Three package-private adapters show a headers-aware store as the store type without headers: `HeadersKeyValueStoreAdapter` as a `TimestampedKeyValueStore`, `HeadersWindowStoreAdapter` as a `TimestampedWindowStore` and `HeadersSessionStoreAdapter` as a `SessionStore`. `ProxyUtil.wrapStateStore` checks for `TimestampedKeyValueStoreWithHeaders`, `TimestampedWindowStoreWithHeaders` and `SessionStoreWithHeaders` **before** the generic store checks, and wraps the adapter in the existing `TimestampedKeyValueStoreProxy`, `TimestampedWindowStoreProxy` or `SessionStoreProxy`.

- On read: the adapters turn `ValueTimestampHeaders` into `ValueAndTimestamp` and `AggregationWithHeaders` into the aggregate, also inside iterators (`ValueMappingIterator`). The existing proxies then give Python exactly what they give for timestamped and session stores.
- On write: the adapters turn `ValueAndTimestamp` into `ValueTimestampHeaders.make(value, timestamp, headers)` and an aggregate into `AggregationWithHeaders.make(aggregate, headers)`.
- Kafka Streams' default methods for `prefixScan`, `reverseRange`, `backwardFetch` and `findSessions` throw, so the adapters override every method that the proxies call.

- *Alternative: new proxy classes for headers stores.* Rejected: they would copy every Python-facing method of the three proxies, and the copies could drift apart. With adapters, Python gets the same methods and results by construction, and the existing proxy tests keep covering them.

Where the headers come from: `OperationProcessor` and `FixedKeyOperationProcessor` already remember `recordTimestamp` while processing. They also remember `rec.headers()` and pass a `Supplier<Headers>` to a new `wrapStateStore` overload, next to the timestamp supplier. Outside record processing the supplier returns empty `RecordHeaders`. The adapters copy the headers before storing them, so later changes to the record don't change stored entries.

- *Alternative: always store empty headers.* Rejected: Kafka Streams' own DSL processors keep record headers, and parity with them is the point of the HEADERS format.

### D6. Stream-stream join logging rule

KSML requires both `thisStore` and `otherStore` on a stream-stream join (the parser rejects a join without either), and passes both to Kafka Streams as store suppliers. Kafka Streams 4.4 (`KStreamImplJoin`) builds join stores from given suppliers with logging always on, and applies the `StreamJoined` logging flag only to stores it generates itself and to the shared store of a left or outer join. So:

- The two join window stores always have changelog topics. This was already the case before this change.
- `logging` on the join stores controls the shared store. `BaseOperation.streamJoinedOf` stops calling `withLoggingDisabled()` unconditionally and stops looking only at `thisStore`:

| thisStore.logging | otherStore.logging | `StreamJoined` logging (shared store) |
|---|---|---|
| true (the default) | true (the default) | on |
| false | false | off |
| different values | | on |

- A join store with `logging: false` logs a warning at build time: Kafka Streams always logs join stores. The warning points to the state store reference for the shared store rule, and doesn't describe it: `streamJoinedOf` doesn't know whether the join is an inner join, which has no shared store, and with mixed values the shared store is logged (PR #767 review 03, item 8).

Values are the ones D1 resolved, so a store without `logging` counts as `true`. Different values are accepted: they worked in 1.x, an inner join has no shared store so its `logging` has no effect, and keeping the changelog is the safe choice. Changelog and store names stay Kafka Streams' own (for example `<thisStore>-outer-shared-join-store` and `<app>-<store>-changelog`). Tests assert on the produced changelog topics, not on hard-coded names that KSML doesn't control.

- *Alternative: make `thisStore` and `otherStore` optional, so Kafka Streams creates the join stores itself.* Rejected for this change: it changes the parser, the JSON schema and the docs, which is beyond F13.
- *Alternative: stop passing suppliers and let Kafka Streams build the join stores through `StreamJoined.withStoreName` and `DslStoreSuppliers`, so `logging: false` applies to all three stores.* Rejected: the stores get generated names instead of the declared ones, which renames the changelog topics of existing applications so they lose their join state on upgrade, and it needs the store validation and format handling reworked.

### D7. Window stores used by Python are timestamped stores

`StoreUtil.getWindowStateStoreBuilder` uses `Stores.windowStoreBuilder` for every window store without the headers format, so Python gets a `WindowStoreProxy` without the `*WithTimestamp` methods, even with `timestamped: true`. With this change, `timestamped && !useHeaders` uses `Stores.timestampedWindowStoreBuilder`, and `timestamped: false` keeps `windowStoreBuilder`. Kafka Streams wraps an in-memory supplier with a timestamped marker and a RocksDB supplier with its adapter, so both in-memory and persistent stores work.

Existing state, as read from the Kafka Streams 4.4 source before task 5.3:

| 1.x declaration | Local RocksDB state after the fix | Restore from the changelog |
|---|---|---|
| in memory (the 1.x default) | none | none (the 1.x default had no changelog) |
| `persistent: true`, `timestamped` unset or false | migrated by Kafka Streams' plain-to-timestamped upgrade path, so KSML leaves it alone | correct, if the store had `logging: true` (otherwise see D8) |
| `persistent: true`, `timestamped: true` | misread: 1.x wrote plain value bytes into the timestamped column family, and the new store can't tell them apart | correct: the changelog holds plain values and record timestamps, if the store had `logging: true` (otherwise see D8) |

Result of task 5.3: KSML 1.1.0 (Kafka Streams 4.0) leaves RocksDB data and a `.checkpoint` file. This version then crashes on the first Python `fetch` from a `timestamped: true` window store, with an `IndexOutOfBoundsException`. KSML 1.3.2 (Kafka Streams 4.3) leaves no data on disk, because its shutdown hook doesn't wait for Kafka Streams to close and RocksDB runs without a write-ahead log. This version then restores from the changelog and reads correctly.

Decision: clean up 1.x local state once, as part of the upgrade check of D8. Before Kafka Streams starts, `KafkaStreamsRunner` calls `StoreUpgrade.prepare(<state.dir>/<application.id>, stores, pythonStores)`. If the application state directory has no `.ksml-store-format` marker, `prepare` looks in every task directory (`<n>_<n>`) for the local state of each persistent window store for Python that **declares** `timestamped: true` (row 3 of the table):

- with a declared `logging: true`, it deletes `<task>/<store>` and `<task>/<store>.position`, logs it at INFO, and deletes the task's `.checkpoint`, because Kafka Streams would otherwise load the old offsets and skip the restore; the other stores of that task are then restored from their changelogs too
- without a declared `logging: true`, the store had no changelog in 1.x, so its state can't be restored: KSML stops, see D8

Then it writes the marker, creating the directory if needed. The marker is one file per application state directory, so it survives Kafka Streams deleting and recreating task directories. Kafka Streams ignores unknown files there; only `KafkaStreams.cleanUp()`, which KSML doesn't call, warns about them.

The cleanup decides on the declared values, not on the resolved defaults of D1, because the state was written with the 1.x defaults (`false`). Not touched: window stores that DSL operations materialize (1.x already built them as timestamped), and stores that leave out `timestamped` or set it to `false` (plain stores in 1.x, row 2, which Kafka Streams converts itself).

An earlier version of this decision filtered on the resolved `timestamped` and logged on the resolved `logging`. It deleted the plain 1.x stores of row 2 too, and for a store that left out `logging` it logged that Kafka Streams restores it from the changelog, while that changelog was new and empty. Reviews 02 and 03 of PR #767 found it.

### D8. Stop on KSML 1.x local state that would be lost

The new defaults of D1 turn `logging` on for stores that had no changelog in 1.x. Their local state is the only copy: a new, empty changelog can't restore it, so it is lost when the task moves, and at once with `exactly_once_v2` (Kafka Streams 4.4 `ProcessorStateManager.initializeStoreOffsets` treats a logged store with local state and no offset as corrupted, and wipes the task). The unreadable window stores of D7 without a changelog lose their state too.

Decision: KSML stops, and changes nothing, rather than lose data without the user deciding. Losing data can't be undone, while a stop costs one deploy, and the user is watching the upgrade anyway. Flink (`--allowNonRestoredState`), PostgreSQL and Kafka Streams' own downgrade error choose the same.

`StoreUpgrade.prepare` runs the check in the same pass as the D7 cleanup, on an unmarked application state directory, before it deletes anything:

| Store with local state in a task | KSML |
|---|---|
| persistent, `logging` left out (not a stream-stream join store or a table store that restores from its input topic) | stops; the user sets `logging: false` (keeps the 1.x behavior and the state) or `logging: true` (accepts a changelog with only new updates) |
| persistent window store for Python, `timestamped: true`, `logging` not `true` | stops; the user deletes the files the error names (the data is lost), renames the store, or sets `logging: true` on 1.x first |
| persistent window store for Python, `timestamped: true`, `logging: true` | deletes the state, Kafka Streams restores it from the changelog (D7) |
| a task where the row above deletes a store, and a store with `logging: false` has local state, with exactly-once processing | stops; the user starts once with `processing.guarantee: at_least_once` |

With exactly-once processing, Kafka Streams 4.4 deletes the **whole task directory** when one logged store in it has local state but no changelog offset (`ProcessorStateManager.initializeStoreOffsets` checks whether the task directory is empty, then `StateManagerUtil.closeStateManager` deletes `baseDir()`). A window store that D7 deleted has no offset, so its task is always wiped, together with the state of a `logging: false` store that the user chose to keep. The runner passes `wipesTasks`: `processing.guarantee` other than `at_least_once`, and `enable.transactional.statestores` off, because Kafka Streams skips the wipe with transactional state stores. With at-least-once, Kafka Streams restores the deleted store and leaves the rest of the task alone; the user can switch back to exactly-once on the next start, when the logged stores have offsets.

The error lists every store at risk with its tasks, so one fix covers all. After a stop the marker is not written, so the check runs again on the next start. Once it passes, KSML writes the marker and never checks again. "Local state" is a `<task>/<store>` or `<task>/rocksdb/<store>` directory (`rocksdb` holds key-value and versioned stores); KSML can't tell an empty RocksDB database from a full one without opening it, so it treats an existing directory as state.

The checked stores are all declared stores that `TopologyGenerator` built, as copies keep the declared values (`StateStoreDefinition.declaredTimestamped()` and `declaredLogging()`). Stream-stream join stores are left out: Kafka Streams always logged them, whatever `logging` said (D6). So are the stores of tables when `topology.optimization` includes `reuse.ktable.source.topics`, as `all` (the KSML default, also in 1.x) does: Kafka Streams then uses the input topic of the table as the changelog of its store, and disables the store's own logging, whatever `logging` says (`TableSourceNode`, Kafka Streams 4.4). The 1.x state of such a store has a changelog with all the data. The stores of global tables are left out too: they always restore from their input topic, and their state is not in a task directory. `TopologyGenerator` parses the optimization with `StreamsConfig.verifyTopologyOptimizationConfigs`, as Kafka Streams does. The marker is now written when the topology has any persistent store, not only window stores for Python: otherwise the second start of a 2.0 application would find its own state and stop.

Known limits:
- KSML reads the YAML as the 1.x options. A user who adds `logging: true` before the first start, to a store that had no changelog in 1.x, skips the stop. For an unreadable window store, KSML then deletes the state. The migration guide says to upgrade with the store options of 1.x.
- A store that sets `logging: true` but had no changelog in 1.x, for example after the stop for `logging` left out, has no offset either. With exactly-once processing, Kafka Streams wipes its task too. KSML can't tell this store from one that was logged in 1.x, so it doesn't stop; the migration guide says to start once with `at_least_once`.
- KSML 1.3.x (Kafka Streams 4.3) leaves RocksDB directories without data at shutdown (task 5.3), so such an application stops although there is little to lose. The fix is the same.

- *Alternative: detect the changelog from the task's `.checkpoint`.* Rejected: since Kafka Streams 4.3, RocksDB stores keep their offsets inside the store (`managesOffsets()`), so state from KSML 1.3.x has no checkpoint entries even with a changelog.
- *Alternative: ask Kafka whether `<application.id>-<store>-changelog` exists.* Rejected: it only helps a user who removed `logging: true` from the YAML, who gets a stop and sets it again. It costs an Admin call and Describe rights at startup.
- *Alternative: log a warning and continue.* Rejected: with `exactly_once_v2` the state is gone a few seconds later, before anyone reads the log.
- *Alternative: copy the local state into the new changelog.* Rejected: it needs the changelog topic created with Kafka Streams' configs, Kafka Streams' internal key formats and RocksDB layout, and care with stale task directories of several instances. Re-writing through the store after start uses public API, but it is too late with `exactly_once_v2`.
- *Alternative for unreadable window stores: a runner setting that allows the delete, or an explicit `logging` value as consent.* Rejected: a setting is a config key that has to stay forever, because unknown keys are rejected; and a `logging: false` already in the 1.x YAML would delete without a stop. Deleting by hand, or renaming the store, is a clear decision by a person, as in Kafka Streams' own downgrade error.

- *Alternative: (a) document a reset in the migration guide and mark the change BREAKING.* Rejected: users who miss the guide get a crash.
- *Alternative: (c) convert the values in place.* Rejected: more code, and the restore from the changelog is enough.

## Risks / Trade-offs

- [Existing applications suddenly create changelog topics and write RocksDB state] → Document it as **BREAKING** in the migration guide, with ACL needs, `ksml.storageDirectory` volume advice and the `persistent/logging: false` opt-out.
- [Caching changes when DSL results are emitted (deduplicated, emitted on commit)] → Document it, and note that `statestore.cache.max.bytes` and `commit.interval.ms` control it. `TopologyTestDriver` commits after every record, so most tests are unaffected. Any that are get explicit `caching: false`.
- [Switching `dsl.store.format` on an application with existing state] → Kafka Streams' headers stores can open plain or timestamped data through their migrating RocksDB stores. Whether that holds for every type is not something KSML guarantees: the migration guide advises a reset or a new application id, and says that switching back from HEADERS is unsupported.
- [More tests run RocksDB, so the build gets slower] → Measure in group 1. Tests that don't care about persistence set `persistent: false`.
- [The adapters must override every store method the proxies call, or Kafka Streams' throwing defaults surface] → Unit tests per adapter, and an end-to-end test that runs the same Python calls against both formats (the spec's "same calls" scenario).
- [The first start after the upgrade restores these window stores from the changelog] → It happens once, it's logged, and the migration guide mentions it.
- [Users put `state.dir` in the `kafka` section on a volume, but the runner always uses `ksml.storageDirectory`, so the persistent stores write to the working directory and are restored after every container restart] → The docs name `ksml.storageDirectory`, and the runner warns when the `kafka` section sets another `state.dir` (PR #767 review 03, item 6). Honoring `state.dir` was rejected: it would move the state directory of existing applications on upgrade, and give one setting two names.
- [The first start stops on 1.x state that would be lost (D8)] → The error names each store and what to set or delete; the migration guide lists the cases. It only happens once per state directory, and only for stores with local state.

## Migration Plan

- Ships in KSML 2.0 with a `docs/migration-to-2.0.0.md` section: new defaults, a table to restore the old behavior (`persistent: false`, `logging: false`, `caching: false`, `timestamped: false`), changelog topics and ACLs, `ksml.storageDirectory`, caching emission timing, join changelogs, and how to opt in to HEADERS (with the caution about existing state).
- Rollback: users can restore the old behavior per store with explicit flags. There is no global switch back to the old defaults.

## Open Questions

- ~~What the RocksDB cost in the test suite turns out to be.~~ Measured in task 1.5 on GraalVM: the `ksml` module tests take 49.8 s before and 54.6 s after (1047 vs 1057 tests). No tests need `persistent: false` for speed.
- ~~Does 1.x window store state read correctly after D7, from local state and after a changelog restore?~~ Answered by task 5.3, see D7.
