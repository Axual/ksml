# Bugs and Dead-Code Candidates

Findings from a systematic review of the production codebase (main sources, not tests)
across all modules, looking for code that is unreachable, will never actually be invoked,
or executes but has no effect on the running pipeline. Each finding was traced against
actual call sites, concrete subtypes, and construction/registration sites — not flagged on
appearance alone. Items marked **(verified directly)** were personally re-checked against
the source by re-reading the file(s) involved, rather than only trusted from a research
pass; other items were reported with grep/trace evidence but not independently re-verified
line-by-line.

The general method, illustrated by the first finding below: for an exhaustive-looking
`instanceof`/switch cascade with a "shouldn't happen" fallback, trace (a) every path that
produces the value being branched on, to see what it can really return without throwing
first, and (b) which concrete subtypes are ever actually constructed/registered elsewhere
in the codebase. A fallback that only covers N-1 of N known cases is *not* dead code — it's
reachable for the missing case. Only flag a fallback as dead once every reachable case is
proven to be covered by a preceding branch.

## Confirmed bugs (not just dead code — actual incorrect behavior in production)

### 1. `ReduceOperationParser` silently drops `adder`/`subtractor` config **(verified directly)**

`ksml/src/main/java/io/axual/ksml/operation/parser/ReduceOperationParser.java:68-78`

```java
public StructsParser<ReduceOperation> parser() {
    return StructsParser.of(
            node -> {
                final var result1 = reducerParser.parse(node);
                if (result1 != null) return result1;
                final var result2 = addedSubtractorParser.parse(node);   // unreachable
                if (result2 != null) return result2;
                throw new TopologyException("Error in reducer operation: " + node);   // unreachable
            },
            schemas);
}
```

`ReduceOperation` (`ksml/src/main/java/io/axual/ksml/operation/ReduceOperation.java`) has two
genuine modes: a `reducer` function (used by `apply(KGroupedStreamWrapper, ...)`) and a
`adder`+`subtractor` pair (used by `apply(KGroupedTableWrapper, ...)`) — confirmed by reading
the class: distinct constructors, distinct `apply()` overloads. But `reducerParser`'s
constructor lambda —

```java
(name, reducer, store, tags) -> new ReduceOperation(storeOperationConfig(name, tags, store), reducer)
```

— never checks whether `reducer` is actually present before constructing. `functionField(...)`
returns `null` (not an exception) when its named child is simply absent from the YAML node
(traced through `TopologyBaseResourceFields.functionField` → `TopologyResourceParser.parse()`,
which falls through to `return new TopologyResource<>(name, null, node.tags())` when neither a
string reference nor an inline child node exists). So for a table-reduce written with
`adder`/`subtractor` (no `reducer` field), `reducerParser.parse(node)` still succeeds, returning
a `ReduceOperation` with `reducer=null, adder=null, subtractor=null`. Since that result is
always non-null, `addedSubtractorParser.parse(node)` (the branch that would actually handle
`adder`/`subtractor`) and the final `throw` can never execute.

**Effect:** a documented table-reduce operation using `adder`/`subtractor` silently produces a
broken `ReduceOperation` instead of a clear parse-time error, likely surfacing later as a
confusing "function not defined" failure at topology-build time (`userFunctionOf` called with a
null `FunctionDefinition`).

No test exercises the `adder`/`subtractor` form; `KSMLReduceTest` only covers the
stream/`reducer` variant.

### 2. `retryOnFail` crashes consume/process error handling **(verified directly)**

`ksml/src/main/java/io/axual/ksml/execution/ErrorHandling.java:104-146`

```java
public DeserializationExceptionHandler.DeserializationHandlerResponse handle(...) {
    ...
    return switch (consumeHandler.handlerType()) {
        case CONTINUE_ON_FAIL -> ...CONTINUE;
        case STOP_ON_FAIL -> ...FAIL;
        default -> throw new UnsupportedOperationException("Unsupported deserialization error handler type. Only CONTINUE_ON_FAIL or STOP_ON_FAIL are allowed.");
    };
}

public ProcessingExceptionHandler.ProcessingHandlerResponse handle(...) {
    ... // same shape, same default -> throw

public ProductionExceptionHandler.ProductionExceptionHandlerResponse handle(...) {
    return switch (produceHandler.handlerType()) {
        case CONTINUE_ON_FAIL -> ...CONTINUE;
        case STOP_ON_FAIL -> ...FAIL;
        case RETRY_ON_FAIL -> ...RETRY;   // only handler role that actually supports retry
    };
}
```

`ErrorHandlingConfig.java:107-116` exposes `retryOnFail` uniformly as a valid value for
`consume`, `process`, and `produce` handler config — nothing rejects it for consume/process at
config-load time. So a user can legally configure `ksml.errorHandling.consume.handler:
retryOnFail`, have it pass validation, and then hit `UnsupportedOperationException` on the
Kafka Streams thread the first time a real deserialization or processing error actually occurs.

### 3. Two modules effectively disabled by empty SPI registration files **(verified directly)**
_Won't fix: this is intentional_

- `ksml-data-jsonschema-confluent/src/main/resources/META-INF/services/io.axual.ksml.data.notation.NotationProvider` — confirmed 0 bytes (`wc -c`)
- `ksml-data-protobuf-confluent/src/main/resources/META-INF/services/io.axual.ksml.data.notation.NotationProvider` — confirmed 0 bytes

~~A working sibling (`ksml-data-avro-confluent`'s equivalent file) correctly contains
`io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider`. Notation providers
are only discovered via `ServiceLoader` (see `NotationFactories` in `ksml-runner`), so
`ConfluentJsonSchemaNotationProvider` and `ConfluentProtobufNotationProvider` — and everything
they construct — are never instantiated in production. There's an acknowledged TODO about this
gap at `ksml-runner/src/main/java/io/axual/ksml/runner/config/NotationConfig.java:70-74`.~~

### 4. `ConfluentProtobufSchemaParser.parse()` is a permanent no-op **(verified directly)**
_Won't fix for now - intentional_

`ksml-data-protobuf-confluent/src/main/java/io/axual/ksml/data/notation/protobuf/confluent/ConfluentProtobufSchemaParser.java:33-35`

```java
@Override
public DataSchema parse(String contextName, String schemaName, String schemaString) {
    return null;
}
```

All three parameters are ignored. Git history shows a real implementation was removed in a
cleanup commit. Combined with finding #3, this module's schema-parsing path is currently
entirely non-functional in production.

### 5. `CsvDataObjectConverter`'s structured-CSV branch is dead due to a type-mismatched `equals`

`ksml-data-csv/src/main/java/io/axual/ksml/data/notation/csv/CsvDataObjectConverter.java:39-46`

```java
if (value instanceof DataList valueList && DEFAULT_TYPE.equals(valueList.type())) {
```

`DEFAULT_TYPE` is a `UnionType`; `valueList.type()` is always a `ListType`. Lombok's generated
`UnionType.equals` starts with an `instanceof UnionType` guard, so it returns `false` for any
`ListType` argument — the branch can never be entered. **Effect:** CSV structured-value-to-string
conversion silently falls back to generic bracket-`toString()` formatting instead of proper
CSV-escaped text. No test exercises this class.

### 6. `JoinTargetDefinitionParser` rejects inline `table:`/`globalTable:` join targets **(verified directly)**

`ksml/src/main/java/io/axual/ksml/definition/parser/JoinTargetDefinitionParser.java:44-51`

```java
if (node.get(Operations.Join.WITH_STREAM) != null) {
    return new TopologyResourceParser<>("stream", Operations.Join.WITH_STREAM, null, ...).parse(node);
}
if (parseString(node, Operations.Join.WITH_TABLE) != null) {
    return new TopologyResourceParser<>("table", Operations.Join.WITH_TABLE, null, ...).parse(node);
}
if (parseString(node, Operations.Join.WITH_GLOBAL_TABLE) != null) {
    return new TopologyResourceParser<>("globalTable", Operations.Join.WITH_GLOBAL_TABLE, null, ...).parse(node);
}
```

The `stream:` field is checked for presence with `node.get(...) != null` (line 44), which is
satisfied whether the field is a plain string reference *or* an inline object — both are valid
per `TopologyResourceParser`'s own two-way handling (string lookup vs. inline definition). But
`table:`/`globalTable:` (lines 47, 50) are instead checked with `parseString(node, ...)`, which
only recognizes a plain string value. Per `BaseParser.isValue` (`BaseParser.java:54-57`),
`parseString` doesn't just fail to match an inline object — it actively **throws**
`ParseException("Expected type string, found object")` the moment the child is present but is an
object rather than a string, before the code can even reach the branch that would parse it as an
inline definition.

**Effect:** an inline (non-reference) `table:` or `globalTable:` join target — e.g.

```yaml
table:
  topic: other
  keyType: string
  valueType: string
```

— fails immediately with a confusing "Expected type string, found object" parse error, even
though the exact same inline-object shape works correctly for `stream:` joins. Reproduced
directly by parsing the snippet above through `JoinOperationParser`; confirmed the same code path
is shared by `LeftJoinOperationParser` and `OuterJoinOperationParser`, so all join-operation
variants are affected for `table`/`globalTable` (only `stream` supports inline definitions in
practice; named references to a previously-declared `table`/`globalTable` still work fine, since
those are plain strings). No test exercises an inline `table:`/`globalTable:` join target —
`JoinOperationParsersTest` only covers named references (`table: theTable`,
`globalTable: theGlobalTable`).

Found while tracing whether `JoinTargetDefinitionParser` could return a null/incomplete result
that would make the `JoinOperationParser`/`LeftJoinOperationParser` `instanceof`-cascade fallback
(their entries in the dead-code table below) reachable after all — it can't (every path either
throws or constructs a concrete `TopicDefinition` subtype first), but this inconsistency turned up
adjacent to that investigation.

## Confirmed unreachable/dead code (no behavioral bug, just code that can't run)

| Location | Why it's dead |
|---|---|
| `ksml/.../operation/parser/JoinOperationParser.java:86-89` **(verified directly)** | All 3 `instanceof` checks (`StreamDefinition`/`TableDefinition`/`GlobalTableDefinition`) cover everything `JoinTargetDefinitionParser` can produce without throwing first — traced through `TopologyResourceParser`'s return paths and confirmed no other `TopicDefinition` subtype is ever registered under a name this lookup can find. Still present: the compiler can't verify the cascade is exhaustive, so the trailing `throw` is a required terminal, not deletable dead code. |
| `ksml/.../operation/parser/LeftJoinOperationParser.java:86-88` **(verified directly)** | Same pattern, same reasoning as above. Still present for the same compiler-mandated reason. |
| ~~`ksml-data/.../mapper/NativeDataObjectMapper.java:129,132` **(verified directly)** — Byte-identical duplicate `instanceof Tuple<?>` check; line 129 always matches first~~ **(Fixed 2026-08-17)** — the duplicate line was removed, and the surrounding method was later rewritten as a Java `switch` pattern-match expression, where a duplicate `case Tuple<?>` would be a compile error; the finding can no longer recur in this form. |
| ~~`ksml-data-avro/.../AvroDataObjectMapper.java:182` — `: null` fallback for a field lookup that's provably always found, since `structSchema` is a 1:1 map of the same `avroSchema`'s fields~~ **(Fixed 2026-08-17)** — replaced with a direct `structSchema.field(name).schema()` call. |
| `ksml-data-avro/.../AvroSchemaMapper.java:304` | Final `throw` in a cascade covering all 6 concrete `DataSchema` subtypes plus fixed singletons — nothing else is ever constructed. Still present: same compiler-mandated-terminal reasoning as the two `JoinOperationParser` entries above. |
| ~~`ksml-data-csv/.../CsvSchemaMapper.java:38-45` — `return null` after a cascade that, traced exhaustively, always returns a `DataList`~~ **(Fixed 2026-08-17)** — replaced with a direct `(DataList)` cast plus a comment explaining the invariant. |
| ~~`ksml-kafka-clients/.../PatternResolver.java:243,252` — `count > 0` checked redundantly, one line after `count` was unconditionally incremented~~ **(Fixed 2026-08-17)** — dropped the redundant `count > 0 &&` from both conditions. |
| ~~`ksml-kafka-clients/.../ResolvingClientConfig.java:84-91` — Catches `ConfigException`, but the called overload only ever throws the broader `ClientException`~~ **(Fixed 2026-08-17)** — this was actually a mislabeled bug, not just dead code: fixed to catch `ClientException`, fold the original message in, and preserve it as `initCause`; added a regression test (`getConfiguredInstanceWrapsFailureAsConfigException`). |
| ~~`ksml-kafka-clients/.../ResolvingSerializer.java` / `ResolvingDeserializer.java` (several lines) — Null-fallback for `topicResolver`, which is always assigned non-null at every real construction site~~ **(Fixed 2026-08-17)** — all 5 call sites simplified to a direct `topicResolver.resolve(topic)`. |
| ~~`ksml-kafka-clients/.../ExtendableCreateTopicsResult.java:32-35` — The one subclass dereferences the same parameter immediately after `super(result)`, so a null would NPE there regardless of the substitution logic~~ **(Fixed 2026-08-17)** — removed the dead null-substitution; `ResolvingCreateTopicsResult` also switched to reading the inherited `createTopicsResult` field instead of its own constructor parameter. |
| ~~`ksml-test-runner/.../AssertionRunner.java:246` — `return message;` after a redundant re-check of a condition the only caller already guaranteed~~ **(Fixed 2026-08-17)** — collapsed the two redundant checks; the extraction logic now uses a single `idx > 0` guard. |
| ~~`ksml-test-runner/.../AssertionRunner.java:99-101` — "Undeclared stream" guard already enforced upstream by `TestDefinitionParser.parseAssertBlocks()` before an `AssertBlock` can exist~~ **(Fixed 2026-08-17)** — removed the dead null-check and error return. |
| ~~`ksml-test-runner/.../KSMLTestRunner.java:75-79` — Manual "no test paths" check after picocli's own `arity="1..*"` already throws (and is already caught) for that case~~ **(Fixed 2026-08-17)** — removed the redundant check. |

Not personally re-verified line-by-line beyond `JoinOperationParser`/`LeftJoinOperationParser`/
`NativeDataObjectMapper`, but traced with the same producer/caller/subtype methodology.

10 of the 13 rows above were fixed in commit `45f3ae2d` ("Fixed unreachable/dead code items"); the
remaining 3 (`JoinOperationParser`, `LeftJoinOperationParser`, `AvroSchemaMapper`) were re-verified
against current source on 2026-08-17 and left as-is, since in each case the `throw` is a
compiler-mandated terminal for an `instanceof`/cascade Java can't statically prove exhaustive —
there's no line left to delete.

## Confirmed unused (zero references anywhere in the repo)

- ~~`io.axual.ksml.parser.StructParser` **(verified directly)** — dead interface, superseded by `StructsParser`; see the parser architecture discussion.~~ **(Fixed 2026-08-20)** — deleted.
- ~~`KSMLRunnerConfig.mapper` field and `.getKafkaConfig()` **(verified directly via grep)** — actual config loading uses a separate `ObjectMapper`; all real callers use `getKafkaConfigMap()` instead.~~ **(Already resolved)** — gone by the time this pass started, superseded by the unrelated `Refactor parsers (#669)` commit; no action needed.
- `ksml-query` REST helpers, re-verified item by item:
  - ~~`HostDiscovery.discoverLocal()`~~ **(Fixed 2026-08-20)** — removed, along with its test (the only caller).
  - ~~`RestClient.getHostIPForDiscovery()` (dead duplicate of `Utils.getHostIPForDiscovery()`)~~ **(Already resolved)** — no longer present; `RestClient` was cleaned up separately at some point before this pass.
  - ~~`Utils.getRemoteStoreData(...)` (both overloads) + `closeRESTClient()`~~ **(Already resolved)** — neither exists anymore.
  - ~~`WindowedKeyValueBeans.add(...)` (both overloads)~~ **(Fixed 2026-08-20)** — the class actually had 3 `add` overloads by now (mirroring `KeyValueBeans`); the 3-arg convenience and the `add(WindowedKeyValueBeans)` merge overload had zero production callers (only their own unit test), so both were removed. The remaining `add(WindowedKeyValueBean)` overload is genuinely used by `StoreResource` and was kept.
- ~~`TestExecutionContext.registryClient()` (ksml-test-runner) — Lombok-generated accessor, zero callers.~~ **(Fixed 2026-08-20)** — dropped `@Getter`, field stays private.
- ~~`DataException.validationFailed(String, Object)`.~~ **(Fixed 2026-08-20)** — removed; had zero callers anywhere, not even a test.
- `getRegistryClient()` accessors, re-verified item by item:
  - ~~`ConfluentProtobufSerdeSupplier`~~ **(Fixed 2026-08-20)** — dropped `@Getter` (and the stale "mocked by tests" comment above it), field stays private.
  - ~~`ApicurioProtobufNotationProvider` and `ConfluentJsonSchemaSerdeSupplier`~~ **(Already resolved)** — neither class exposes this accessor anymore.
- ~~`TransactionalIdResolver`'s 4 convenience methods (`resolveTransactionalId(s)`/`unresolveTransactionalId(s)`).~~ **(Fixed 2026-08-20)** — removed. Unlike the sibling `GroupResolver`/`TopicResolver` interfaces, whose equivalent collection-resolving methods *are* called in production (e.g. `ResolvingAdmin`), nothing ever called these — the two real transactional-id call sites use the base `resolve()`/`unresolve()` directly. `TransactionalIdResolver` is now a documented empty marker interface (`extends Resolver`, no added methods). Digging into this also turned up a related duplication bug: `ResolvingProducerConfig` was building its own throwaway `TransactionalIdPatternResolver` instead of using the one already built by its `ResolvingClientConfig` superclass — fixed to delegate to the inherited `transactionalIdResolver()`, matching how `ResolvingConsumerConfig` already used `groupResolver()`. As part of the same cleanup, `ResolvingClientConfig`'s three resolver fields were tightened from `public` to `private` (with existing `@Getter`s), and call sites updated accordingly.
- ~~The entire `ResolvingStrategy` interface — no implementers anywhere.~~ **(Already resolved)** — the interface no longer exists in the codebase.

All fixes dated 2026-08-20 above are uncommitted in the working tree as of this writing; verified via the full test suites of `ksml-kafka-clients`, `ksml-query`, `ksml-test-runner`, `ksml-data`, and `ksml-data-protobuf-confluent`.

## Repo hygiene (not dead code, but worth fixing)

~~**`ksml-data/src/main/java/io/axual/ksml/data/util/MapUtil.java:6-10`** **(verified directly)** —
a literal, committed, unresolved git merge-conflict marker sitting inside the license header
comment:

```java
<<<<<<< HEAD
 * Copyright (C) 2021 - 2024 Axual B.V.
=======
 * Copyright (C) 2021 - 2025 Axual B.V.
>>>>>>> main
```

Harmless at compile time (inside `/* */`), but should never have been committed and should be
cleaned up.~~ **(Already resolved)** — re-checked 2026-08-20: the license header is clean, no
conflict markers remain.

## Lower confidence / worth a look, not conclusively dead

- `CachedPatternResolver` (ksml-kafka-clients) configures its Guava caches with
  `expireAfterAccess`/`expireAfterWrite(Duration.ZERO)`, which per Guava semantics makes caching
  a no-op. It runs and is wired in — it just delivers none of its intended benefit.
- `ProtobufFileElementSchemaMapper.toDataSchema`'s `namespace` parameter appears to have no
  effect on the returned schema (namespace always comes from `fileElement.getPackageName()`);
  same root cause likely makes `ApicurioProtobufSchemaParser.parse`'s `contextName` parameter
  dead too.
- `ProtobufSchema.fileDescriptor` record component is populated at both construction sites but
  never read anywhere (`.fileDescriptor()` has zero call sites); only `.protoFileElement()` is
  consumed.
- `DataException.conversionFailed(DataType, DataType)` / `(String, String)` overloads have no
  production callers (only their own unit test).
- `CsvDataObjectMapper.toDataObject`'s multi-row loop continuation is unreachable because the
  configured Jackson CSV reader always yields a single `String[]`, causing an unconditional
  `return` on the first iteration — plausibly intentional given KSML's one-row-per-record model,
  flagged for awareness rather than as a bug.

## Adjacent finding, out of scope for this review

~~**`ksml-query/.../WindowedKeyValueStoreResource.java` (`getKey`, ~line 100)** — the
remote-dispatch branch builds the wrong REST URL (a plain key-value path instead of the windowed
path with a timestamp segment) and deserializes the response as the wrong bean type. This is a
*live* bug (the code runs and has an effect, just an incorrect one) rather than dead code, so
it's out of scope for this document, but worth a follow-up ticket.~~ **(Already resolved)** —
re-checked 2026-08-20: fixed in commit `a2b3b378` ("Applied changes after reviewing the changes",
2026-07-01), which predates this document's cleanup pass and is already an ancestor of every
branch here. The URL now correctly targets `/state/windowed/{store}/local/get/{key}/{timestamp}`
and deserializes as `WindowedKeyValueBean`, with explicit regression coverage in
`WindowedKeyValueStoreResourceTest.getKeyRoutesToRemoteInstance`. No action needed.

## Possible clean up of DataSchemaMapper

The "namespace parameter is superfluous" observation on `ProtobufFileElementSchemaMapper` (in the
"Lower confidence" section above) turned out to be one instance of a wider pattern once traced
across every implementor of `DataSchemaMapper<T>`:

```java
public interface DataSchemaMapper<T> {
    DataSchema toDataSchema(String namespace, String name, T value);
    default DataSchema toDataSchema(String name, T value) { return toDataSchema(null, name, value); }
    default DataSchema toDataSchema(T value) { return toDataSchema(null, null, value); }
    T fromDataSchema(DataSchema schema);
}
```

It may be possible to simplify this interface by getting rid of the three-parameter
`toDataSchema(String namespace, String name, T value)` method (or its `namespace` parameter
specifically) — **(verified directly, traced 2026-08-27)**. This is not a proposal to act on yet;
it's meant as the basis for a discussion on whether to refactor, so the breakdown below is
deliberately precise about what each implementor's method body actually does with `namespace`
versus what real (non-test) production call sites actually pass into it.

**Implementors that genuinely read `namespace` to build the returned schema:**

- `CsvSchemaMapper.toDataSchema` — `new StructSchema(namespace, name, "CSV schema", fields, false)`.
- `XmlSchemaMapper.toDataSchema` — wraps it into a private `XMLSchemaParseContext`, then
  `new StructSchema(context.namespace, element.getName(), ...)` for the top-level struct.
- `JsonSchemaMapper.toDataSchema` — `StructSchema.builder().namespace(namespace)...build()`.

**Implementors where `namespace` is dead — never read, or only forwarded to a recursive self-call:**

- `DataTypeDataSchemaMapper.toDataSchema` — the only appearance of `namespace` in the body is a
  recursive self-call for the `MapType` branch (`toDataSchema(namespace, name, mapType.valueType())`);
  it's never used to construct a schema. `StructType`/`TupleType` branches ignore it entirely.
- `NativeDataSchemaMapper.toDataSchema` — `namespace` isn't referenced at all, not even forwarded;
  `name` is dead too (`ParseNode.fromRoot(json, "Schema")` hardcodes the literal string `"Schema"`).
- `AvroSchemaMapper.toDataSchema` — already documented via its own javadoc as ignored, using
  `schema.getNamespace()`/`schema.getName()` from the Avro `Schema` object instead.
- `ProtobufFileElementSchemaMapper.toDataSchema` — uses `context.namespace` (derived from
  `fileElement.getPackageName()`) instead of the parameter.
- `ProtobufSchemaMapper.toDataSchema` — pure passthrough to the call above, so dead transitively.

**What real call sites actually pass:**

- `CsvSchemaParser.parse`, `XmlSchemaParser.parse`, `JsonSchemaLoader.parse`,
  `ApicurioProtobufSchemaParser.parse` all go through the 2-arg convenience default
  (`toDataSchema(name, value)` → `toDataSchema(null, name, value)`), so `namespace` is always
  `null` at every one of these entry points today — even for Csv/Xml/Json, whose method bodies are
  wired to use it.
- `AvroDataObjectMapper.java:173,294` and `ProtobufDataObjectMapper.java:70` are the only call
  sites that pass a genuine, non-null `namespace` (`avroSchema.getNamespace()`,
  `descriptor.getFile().getPackage()`) — and in both cases the callee silently discards it,
  re-deriving the same value internally from the schema/descriptor object it was also given.

**Net effect:** no code path anywhere in the repo today produces a different result because of the
`namespace` parameter — for Avro/Protobuf because the implementation ignores it outright, and for
Csv/Xml/Json because every real caller only ever supplies `null`. Whether that argues for dropping
the parameter (or the whole 3-arg method) from the interface, keeping it as documented-but-unused
API surface (matching `AvroSchemaMapper`'s existing javadoc precedent), or something else, is the
open question for discussion — no code changes have been made for this finding.

_Note: discussed, holding back on this for now._
