# Tasks

Each implementation task follows TDD: first commit the failing test, then the fix that makes it pass.

## 1. Declared store defaults (design D1-D3)

- [x] 1.1 Add failing tests in `StoreUtilTest` and the definition parser tests: a keyValue, window and session store declared without options is persistent, timestamped and logged; explicit `false` values are kept; a versioned store without `persistent` is a persistent versioned store. Verify the tests fail for the right reason.
- [x] 1.2 Change `StateStoreDefinition` (and subclasses, `with(...)`, `BaseOperation.validate*Store`) so `persistent`/`logging`/`timestamped` turn `null` into `true` and `caching` stays a nullable `Boolean`. Verify the 1.1 tests pass.
- [x] 1.3 Add failing tests for caching: a store without `caching` is cached when materialized by `aggregate`/`count` and not cached when built for Python functions; explicit `caching` wins in both cases. Then resolve caching in `StoreUtil.materialize` (null -> true) and `getStoreBuilder` (null -> false). Verify the tests pass.
- [x] 1.4 Add a failing test that a session store with `timestamped` logs a warning naming the store and still builds. Then add the warning in `SessionStateStoreDefinitionParser`. Verify the test passes.
- [x] 1.5 Run the full `ksml` module tests. Fix tests that relied on the old defaults with explicit flags or new expectations, and record the build time before and after to measure the RocksDB cost (design Open Question). Verify `mvn -pl ksml test` passes.
- [x] 1.6 Check that `TableDefinition`/`GlobalTableDefinition` stores without a declared store still build in memory without logging. Verify with an existing or new test in `TopologyBuildContext` tests.
- [x] 1.7 Update the parser field descriptions (defaults in the descriptions) and regenerate `docs/ksml-language-spec.json`/`.md` if the build generates them. Update the default values and the caching-per-use explanation in `docs/reference/state-store-reference.md`, `docs/reference/definition-reference.md` and `docs/tutorials/intermediate/state-stores.md`. Verify the documented YAML examples still parse (an existing docs-example test, or by running them through the test runner).
- [x] 1.8 Add the "new store defaults" section to `docs/migration-to-2.0.0.md` (changelog topics and ACLs, `state.dir`, caching emission timing, the table of explicit flags that restore the old behavior, the versioned store fix, the session `timestamped` warning) and to `docs/release-notes.md`, and add caching notes (`statestore.cache.max.bytes`, `commit.interval.ms`) to `docs/tutorials/advanced/performance-optimization.md`. Verify the docs build (mkdocs) shows no broken links.

## 2. Stream-stream join store logging (design D6)

- [x] 2.1 Add failing topology tests: an outer stream-stream join whose two declared stores leave out `logging` has changelog topics for its window stores and its shared store; with both stores at `logging: false` the window stores still have changelog topics, the shared store has none, and a warning names each store; with different `logging` values, building fails with an error naming both stores. Verify the tests fail.
- [x] 2.2 Change `BaseOperation.streamJoinedOf` to use the logging rule from D6 instead of the unconditional `withLoggingDisabled()`. Verify the 2.1 tests pass and the existing join tests still pass.
- [x] 2.3 Document the join changelog behavior (window stores always log, `logging` controls the shared store, the warning and the conflict error) in `docs/reference/state-store-reference.md` (or the join operation reference) and in the migration guide section from 1.8. Verify the docs build.

## 3. Headers store format (design D4)

- [x] 3.1 Add failing `StoreUtilTest` cases: with the headers format on, default keyValue, window and session stores get headers-aware suppliers and builders (for both materialized and processor use); in-memory, `timestamped: false` and versioned stores do not; with the headers format off, nothing changes. Verify the tests fail.
- [x] 3.2 Add the `headersStoreFormat` flag to `StoreUtil`, `TopologyBuildContext` and `TopologyGenerator` (existing constructors default to `false`), and pick the suppliers and builders from the D4 table. Verify the 3.1 tests pass.
- [x] 3.3 Add a failing `KafkaStreamsRunner` test that `dsl.store.format: HEADERS` (any case) in the Kafka config builds a topology with headers-aware stores and that an absent value does not. Then pass the flag on from `KafkaStreamsRunner`. Verify the test passes.
- [x] 3.4 Add a test that a stream-stream join with declared stores still builds and joins correctly with the headers format on. Verify it passes.
- [x] 3.5 Document `dsl.store.format: HEADERS` (which stores it affects, which it doesn't, and the caution about switching formats on existing state) in `docs/reference/state-store-reference.md`, the runner configuration docs and the migration guide. Verify the docs build.

## 4. Python access to headers-aware stores (design D5)

- [x] 4.1 Add failing unit tests for Python on headers-aware keyValue, window and session stores (`HeadersStoreProxyTest`, through `ProxyUtil.wrapStateStore`): reads return plain values (or `{value, timestamp}` for the `*WithTimestamp` methods) and never headers; writes store the current record's headers; writes without a record store empty headers; later changes to the record's headers don't change stored entries. Verify the tests fail.
- [x] 4.2 Add a headers supplier next to the timestamp supplier in `OperationProcessor` and `FixedKeyOperationProcessor`, and pass it to `ProxyUtil.wrapStateStore`. Verify the existing proxy and processor tests still pass.
- [x] 4.3 Implement the three adapters (`HeadersKeyValueStoreAdapter`, `HeadersWindowStoreAdapter`, `HeadersSessionStoreAdapter`) with `ValueMappingIterator`, and the headers checks in `wrapStateStore` before the generic store checks. Verify the 4.1 tests pass.
- [x] 4.4 Add a parameterized end-to-end test (KSML definition plus Python in `TopologyTestDriver`) that runs the same keyValue, window and session store calls with the headers format off and on, and asserts the same Python results (spec "Same calls for both formats"). Verify it passes.
- [x] 4.5 Add the test for the ticket's comment criterion: Python `store.get(key)` and `store.all()` on a keyValue store declared without options return the same results as on a `persistent: false, timestamped: false` store. Verify it passes.
- [ ] 4.6 Add an interactive query test (through `ksml-query` or the Kafka Streams IQ types) that a headers-aware keyValue store written from Python keeps the record's headers and can be queried as a plain keyValue store. Verify it passes.
- [ ] 4.7 Document in `docs/reference/state-store-reference.md` (Python store API section) that headers-aware stores have the same Python API, that reads return values only, and that writes keep the current record's headers. Verify the docs build.

## 5. Integration verification

- [ ] 5.1 Run the full Maven build (`mvn clean verify`) for all modules. Verify it passes.
- [ ] 5.2 Run the ticket's Docker reproduction with the KSML runner: confirm that declared stores create changelog topics, survive a restart, and that with `dsl.store.format: HEADERS` the stores are headers-aware. Verify by inspecting topics and the logged topology.
- [ ] 5.3 Run the parity cases C30, C32, C44a, C49, C50 and C50b from AXPD-12447 against this build. Verify they pass, or record each difference on the ticket.
- [ ] 5.4 Run `openspec validate store-kafka-streams-defaults --strict`. Verify it reports no issues.

## Workflow follow-up

- Rebase on `main` once AXPD-11068 is merged, then re-run 5.1.
- Share the change with the ticket reporter for review before or alongside the PR.
- Archive the change (`/opsx:archive`) in the ticket's PR after review.
