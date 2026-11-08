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
- Supporting `dsl.store.format` in `ksml-test-runner`: it has no Kafka config, so it uses `DEFAULT`.
- Changing implicit table or globalTable stores, or stores of operations without a declared store.
- Migrating existing state from one store format to another.

## Decisions

### D1. Resolve persistent/logging/timestamped in the definition; keep caching nullable

`StateStoreDefinition` turns `null` into `true` for `persistent`, `logging` and `timestamped` in its constructor, so the `boolean` getters and their callers stay as they are. `caching` becomes a `Boolean` that stays `null` when not set. `StoreUtil` resolves it per use: `materialize(...)` turns `null` into `true`, `getStoreBuilder(...)` turns `null` into `false`. Copies (`with(...)`, `BaseOperation.validate*Store`) pass the raw `Boolean` along so an unset value is not frozen early.

- *Alternative: make every flag nullable and resolve all of them in `StoreUtil`.* That touches many more callers for no gain, because only caching depends on the use site.
- *Alternative: one caching default for both uses.* Rejected: it wouldn't match Kafka Streams, which was the decision in explore.

The constructor `KeyValueStateStoreDefinition(name, keyType, valueType)` and the implicit table stores keep passing explicit values, so their behavior doesn't change.

### D2. Warn about session `timestamped` at parse time

`SessionStateStoreDefinitionParser` logs a warning when `timestamped` is set, explicitly or to any value. The definition keeps the value, but it has no effect. Removing the field would break existing YAML and the JSON schema.

### D3. Versioned stores rely on the persistent default

Because of D1, `versioned: true` without `persistent` reaches the versioned branch in `StoreUtil`. If the user sets `persistent: false` together with `versioned: true`, the behavior stays as it is today (an in-memory store) and is documented. Rejecting that combination is out of scope.

### D4. Plumb the store format as a headers flag

`KafkaStreamsRunner` already builds a `StreamsConfig`, which validates `dsl.store.format` (case-insensitive `DEFAULT` or `HEADERS`). It reads `streamsConfig.getString(DSL_STORE_FORMAT_CONFIG).equalsIgnoreCase(DSL_STORE_FORMAT_HEADERS)`, the same check Kafka Streams makes. The resulting `boolean headersStoreFormat` goes through a new `TopologyGenerator` constructor argument, then `TopologyBuildContext`, then the `StoreUtil` calls. A boolean fits better than Kafka Streams' `DslStoreFormat` enum, which has no value for "default" (it has `PLAIN`, `TIMESTAMPED` and `HEADERS`). The existing constructors default to `false`, so `ksml-test-runner` and the tests compile unchanged. Inside `StoreUtil`, one helper decides `useHeaders(store) = headersStoreFormat && persistent && timestamped && !versioned`, and each supplier/builder method branches on it:

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
| different values | | `TopologyException` naming both stores |

- A join store with `logging: false` logs a warning at build time: Kafka Streams keeps that store's changelog, and only the shared store's changelog is turned off.

Values are the ones D1 resolved, so a store without `logging` counts as `true`. The conflict check applies to inner joins too, so the rule is the same for every stream-stream join. Changelog and store names stay Kafka Streams' own (for example `<thisStore>-outer-shared-join-store` and `<app>-<store>-changelog`). Tests assert on the produced changelog topics, not on hard-coded names that KSML doesn't control.

- *Alternative: make `thisStore` and `otherStore` optional, so Kafka Streams creates the join stores itself.* Rejected for this change: it changes the parser, the JSON schema and the docs, which is beyond F13.
- *Alternative: stop passing suppliers and let Kafka Streams build the join stores through `StreamJoined.withStoreName` and `DslStoreSuppliers`, so `logging: false` applies to all three stores.* Rejected: the stores get generated names instead of the declared ones, which renames the changelog topics of existing applications so they lose their join state on upgrade, and it needs the store validation and format handling reworked.

## Risks / Trade-offs

- [Existing applications suddenly create changelog topics and write RocksDB state] → Document it as **BREAKING** in the migration guide, with ACL needs, `state.dir` volume advice and the `persistent/logging: false` opt-out.
- [Caching changes when DSL results are emitted (deduplicated, emitted on commit)] → Document it, and note that `statestore.cache.max.bytes` and `commit.interval.ms` control it. `TopologyTestDriver` commits after every record, so most tests are unaffected. Any that are get explicit `caching: false`.
- [Switching `dsl.store.format` on an application with existing state] → Kafka Streams' headers stores can open plain or timestamped data through their migrating RocksDB stores. Whether that holds for every type is not something KSML guarantees: the migration guide advises a reset or a new application id, and says that switching back from HEADERS is unsupported.
- [More tests run RocksDB, so the build gets slower] → Measure in group 1. Tests that don't care about persistence set `persistent: false`.
- [The adapters must override every store method the proxies call, or Kafka Streams' throwing defaults surface] → Unit tests per adapter, and an end-to-end test that runs the same Python calls against both formats (the spec's "same calls" scenario).
- [Join logging conflict error on topologies that used to build] → It only happens when the two join stores have different `logging` values. The error message says how to fix it.

## Migration Plan

- Ships in KSML 2.0 with a `docs/migration-to-2.0.0.md` section: new defaults, a table to restore the old behavior (`persistent: false`, `logging: false`, `caching: false`, `timestamped: false`), changelog topics and ACLs, `state.dir`, caching emission timing, join changelogs, and how to opt in to HEADERS (with the caution about existing state).
- Rollback: users can restore the old behavior per store with explicit flags. There is no global switch back to the old defaults.

## Open Questions

- ~~What the RocksDB cost in the test suite turns out to be.~~ Measured in task 1.5 on GraalVM: the `ksml` module tests take 49.8 s before and 54.6 s after (1047 vs 1057 tests). No tests need `persistent: false` for speed.
