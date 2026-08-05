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

- `ksml-data-jsonschema-confluent/src/main/resources/META-INF/services/io.axual.ksml.data.notation.NotationProvider` — confirmed 0 bytes (`wc -c`)
- `ksml-data-protobuf-confluent/src/main/resources/META-INF/services/io.axual.ksml.data.notation.NotationProvider` — confirmed 0 bytes

A working sibling (`ksml-data-avro-confluent`'s equivalent file) correctly contains
`io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider`. Notation providers
are only discovered via `ServiceLoader` (see `NotationFactories` in `ksml-runner`), so
`ConfluentJsonSchemaNotationProvider` and `ConfluentProtobufNotationProvider` — and everything
they construct — are never instantiated in production. There's an acknowledged TODO about this
gap at `ksml-runner/src/main/java/io/axual/ksml/runner/config/NotationConfig.java:70-74`.

### 4. `ConfluentProtobufSchemaParser.parse()` is a permanent no-op **(verified directly)**

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

## Confirmed unreachable/dead code (no behavioral bug, just code that can't run)

| Location | Why it's dead |
|---|---|
| `ksml/.../operation/parser/JoinOperationParser.java:86-88` **(verified directly)** | All 3 `instanceof` checks (`StreamDefinition`/`TableDefinition`/`GlobalTableDefinition`) cover everything `JoinTargetDefinitionParser` can produce without throwing first — traced through `TopologyResourceParser`'s return paths and confirmed no other `TopicDefinition` subtype is ever registered under a name this lookup can find |
| `ksml/.../operation/parser/LeftJoinOperationParser.java:86-88` **(verified directly)** | Same pattern, same reasoning as above |
| `ksml-data/.../mapper/NativeDataObjectMapper.java:129,132` **(verified directly)** | Byte-identical duplicate `instanceof Tuple<?>` check; line 129 always matches first |
| `ksml-data-avro/.../AvroDataObjectMapper.java:182` | `: null` fallback for a field lookup that's provably always found, since `structSchema` is a 1:1 map of the same `avroSchema`'s fields |
| `ksml-data-avro/.../AvroSchemaMapper.java:304` | Final `throw` in a cascade covering all 6 concrete `DataSchema` subtypes plus fixed singletons — nothing else is ever constructed |
| `ksml-data-csv/.../CsvSchemaMapper.java:38-45` | `return null` after a cascade that, traced exhaustively, always returns a `DataList` |
| `ksml-kafka-clients/.../PatternResolver.java:243,252` | `count > 0` checked redundantly, one line after `count` was unconditionally incremented |
| `ksml-kafka-clients/.../ResolvingClientConfig.java:84-91` | Catches `ConfigException`, but the called overload only ever throws the broader `ClientException` |
| `ksml-kafka-clients/.../ResolvingSerializer.java` / `ResolvingDeserializer.java` (several lines) | Null-fallback for `topicResolver`, which is always assigned non-null at every real construction site |
| `ksml-kafka-clients/.../ExtendableCreateTopicsResult.java:32-35` | The one subclass dereferences the same parameter immediately after `super(result)`, so a null would NPE there regardless of the substitution logic |
| `ksml-test-runner/.../AssertionRunner.java:246` | `return message;` after a redundant re-check of a condition the only caller already guaranteed |
| `ksml-test-runner/.../AssertionRunner.java:99-101` | "Undeclared stream" guard already enforced upstream by `TestDefinitionParser.parseAssertBlocks()` before an `AssertBlock` can exist |
| `ksml-test-runner/.../KSMLTestRunner.java:75-79` | Manual "no test paths" check after picocli's own `arity="1..*"` already throws (and is already caught) for that case |

Not personally re-verified line-by-line beyond `JoinOperationParser`/`LeftJoinOperationParser`/
`NativeDataObjectMapper`, but traced with the same producer/caller/subtype methodology.

## Confirmed unused (zero references anywhere in the repo)

- `io.axual.ksml.parser.StructParser` **(verified directly)** — dead interface, superseded by `StructsParser`; see the parser architecture discussion.
- `KSMLRunnerConfig.mapper` field and `.getKafkaConfig()` **(verified directly via grep)** — actual config loading uses a separate `ObjectMapper`; all real callers use `getKafkaConfigMap()` instead.
- `ksml-query` REST helpers: `HostDiscovery.discoverLocal()`, `RestClient.getHostIPForDiscovery()` (dead duplicate of `Utils.getHostIPForDiscovery()`), `Utils.getRemoteStoreData(...)` (both overloads) + `closeRESTClient()`, `WindowedKeyValueBeans.add(...)` (both overloads).
- `TestExecutionContext.registryClient()` (ksml-test-runner) — Lombok-generated accessor, zero callers.
- `DataException.validationFailed(String, Object)`.
- `getRegistryClient()` accessors on `ConfluentProtobufSerdeSupplier`, `ApicurioProtobufNotationProvider`, and `ConfluentJsonSchemaSerdeSupplier`.
- `TransactionalIdResolver`'s 4 convenience methods (`resolveTransactionalId(s)`/`unresolveTransactionalId(s)`).
- The entire `ResolvingStrategy` interface — no implementers anywhere.

## Repo hygiene (not dead code, but worth fixing)

**`ksml-data/src/main/java/io/axual/ksml/data/util/MapUtil.java:6-10`** **(verified directly)** —
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
cleaned up.

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

**`ksml-query/.../WindowedKeyValueStoreResource.java` (`getKey`, ~line 100)** — the
remote-dispatch branch builds the wrong REST URL (a plain key-value path instead of the windowed
path with a timestamp segment) and deserializes the response as the wrong bean type. This is a
*live* bug (the code runs and has an effect, just an incorrect one) rather than dead code, so
it's out of scope for this document, but worth a follow-up ticket.
