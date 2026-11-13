## Context

See `proposal.md` for the motivation and `specs/punctuation/spec.md` for the required behavior.

KSML builds topologies with the Kafka Streams DSL. Operations that run Python with store access already plug in a Processor API processor: `TransformValueOperation` calls `KStream.processValues(...)` with a `FixedKeyOperationProcessorSupplier`, and `TransformKeyValueOperation` calls `KStream.process(...)`. Both build on `OperationProcessor` / `FixedKeyOperationProcessor`, which in `init` look up the stores, wrap them with `ProxyUtil.wrapStateStore`, and pass the store proxies to the Python function. While a record is being processed, `OperationProcessor` keeps its timestamp, so Python writes to timestamped stores without a timestamp can use it. Between records that supplier raises `ProxyUtil.noRecordTimestamp()`.

Kafka Streams schedules a punctuator with `ProcessorContext.schedule(Duration, PunctuationType, Punctuator)` from `init`. It runs on the stream thread of the task, between records, and may call `context.forward(...)` and use the task's stores.

The `errorHandling.process` setting of the runner is not registered with Kafka Streams yet. An exception from a Python function goes to the uncaught exception handler. Registering it is tracked separately.

Example of the syntax this design implements:

```yaml
functions:
  emit_totals:
    type: punctuator
    stores: [totals]
    code: |
      keys = []
      with totals.all() as entries:
        for e in entries:
          context.forward(e["key"], e["value"],
                          headers=[{"key": "task", "value": context.taskId()}])
          keys.append(e["key"])
      for k in keys:
        totals.delete(k)

pipelines:
  flush_totals:
    from: orders
    via:
      - type: transformValue
        mapper: add_to_total     # writes to the totals store
        stores: [totals]
      - type: punctuate
        name: flush
        interval: 1m
        punctuationType: wallClockTime
        punctuator: emit_totals
    to: order_totals
```

## Goals / Non-Goals

**Goals:**
- One new operation and one new function type, built on the existing processor and user-function classes.
- A punctuator controls what it emits, including timestamp and headers, through a `context` that follows Kafka Streams' `ProcessorContext`, so new `Record` features can be added to `forward` without new syntax.
- The same store access and typing rules as other functions.
- Testable with `TopologyTestDriver` and the KSML test runner, for both punctuation types.

**Non-Goals:**
- A `punctuators:` option on existing operations, or a top-level section. A separate operation was chosen with the user.
- Registering the process error handler with Kafka Streams.
- Changing the input record path in any way: the operation does not transform records.

## Decisions

**D1 — Use `KStream.process`, not `processValues`.**
A punctuator forwards new records with any key. A `FixedKeyProcessor` can only forward `FixedKeyRecord`s derived from an input record, so it cannot forward punctuation output. The operation therefore uses `KStream.process(ProcessorSupplier, Named, String...)` with a new `PunctuateProcessor extends OperationProcessor`.
*Consequence:* like `transformKeyValue`, Kafka Streams marks the output as possibly re-keyed, so a later key-based operation (`groupByKey`, `join`) repartitions. This is correct, because emitted keys can be anything. The docs will mention it.
*Alternative:* `processValues` plus a separate side branch for emitted records. Rejected: two nodes and a merge for one operation, and the same repartition effect after the merge.

**D2 — The output types are the input types, checked in `forward`.**
`PunctuateOperation.apply` returns a `KStreamWrapper` with the input key and value types. `context.forward` converts the key and value with the same `DataObject` conversion as transformer results, against the stream's key and value types, and raises a Python error when they do not fit. There is no startup check, because a punctuator has no `resultType` (D7).
*Alternative:* keep a returned list of records with a declared `resultType` checked at startup (the first version of this design). Rejected: the glue code would decide timestamp and headers, and every new `Record` field would need new return syntax. Offering both a return value and `forward` was also rejected: two ways to emit, with an unclear order between them.

**D3 — `punctuationType` is required, with the values `wallClockTime` and `streamTime`.**
The two types behave very differently: wall-clock runs without input, stream time never does. A default would hide that choice. The names follow Kafka Streams' `PunctuationType.WALL_CLOCK_TIME` and `STREAM_TIME` in KSML's camelCase style. The parser uses the existing `enumField` helper so the JSON schema lists both values.

**D4 — One schedule per operation, created in `init`, cancelled in `close`.**
`PunctuateProcessor.init` calls `super.init` (stores), then `context.schedule(interval, type, this::punctuate)` and keeps the `Cancellable`. `close` cancels it. Kafka Streams creates one processor per task, so there is one schedule per task. More schedules are written as more `punctuate` operations.

**D5 — A "current time" for the store proxies instead of only the record timestamp.**
`OperationProcessor` replaces its `recordTimestamp` field with a `currentTimestamp`, set during `process` to the record timestamp and during a punctuation to the punctuation timestamp, and `null` otherwise. The store proxies keep using the same supplier, so timestamped writes from a punctuator use the punctuation timestamp. `process` stays `final`; the punctuation goes through a similar protected `final` method so subclasses cannot forget to set and clear the time.

**D6 — A `context` proxy with an allow-list of methods.**
A new `ProcessorContextProxy` in `io.axual.ksml.proxy.context` wraps the `ProcessorContext`. Only the methods listed below are reachable from Python, so anything else is invisible.
*Implementation note:* like every proxy, `ProcessorContextProxy` is listed in the allowed classes of `PythonContext`, and only its `@HostAccess.Export` methods are reachable from Python. GraalPy does not pass Python keyword arguments to Java methods, so Python code does not see the proxy directly: `PythonContext` defines a Python class `KSMLProcessorContext` in its global code, with the same methods, that takes `forward(..., timestamp=None, headers=None, to=None)` and calls the proxy by position. The exported `forward` takes polyglot `Value`s, so the key and value convert exactly like a function result. The proxy reaches the function as an injected variable, wrapped in that class (see D7).

| Python method | Maps to | Notes |
|---|---|---|
| `forward(key, value, timestamp=None, headers=None, to=None)` | `context.forward(new Record<>(k, v, ts, headers), childName)` | `ts` defaults to the punctuation timestamp; headers converted with the existing `HeaderDataObjectMapper`, the same format as `metadataTransformer`; `to` picks a named operation further down (D10); types checked per D2 against the receiving operation |
| `commit()` | `context.commit()` | A request; Kafka Streams commits at its next opportunity |
| `currentSystemTimeMs()` | `context.currentSystemTimeMs()` | |
| `currentStreamTimeMs()` | `context.currentStreamTimeMs()` | |
| `applicationId()` | `context.applicationId()` | |
| `taskId()` | `context.taskId().toString()` | For example `"0_2"` |

Not exposed, and why:
- `schedule`: a schedule created from Python is never cancelled and can grow without bound. Schedules come from the definition.
- `getStateStore`: stores must be declared, so the topology connects them and the proxies apply KSML's typing.
- `appConfigs`, `appConfigsWithPrefix`: may contain credentials.
- `keySerde`, `valueSerde`, `stateDir`, `metrics`: internal. KSML already gives Python its own `metrics` and `log`.
- `recordMetadata`: always empty during a punctuation.
- Kafka Streams child names: these are generated by KSML and are not stable. `to` takes KSML operation names instead (D10).

The method names keep Kafka Streams' camelCase, as the store API does (`putIfAbsent`, `getWithTimestamp`).
The proxy has a `valid` flag that `PunctuateProcessor` clears when the call returns, so a `context` kept in a global variable raises an error instead of forwarding outside the punctuation, the same rule KSML already applies to store iterators.
The class takes the `ProcessorContext` and a default timestamp, not punctuation-specific state, so a later change can pass a `context` to other function types without a redesign.

**D7 — The punctuator function type.**
New `PunctuatorDefinition` (type `punctuator`, built-in parameter `timestamp` of type `long`, custom parameters merged as in `ForEachActionDefinition`, `validateNoResultTypeDefined()`; it then sets an internal result type `UNKNOWN`, so `PythonFunction` hands back a returned value and `UserPunctuator` can warn about it) and `UserPunctuator`, which calls the function with the timestamp, and injects the store proxies and the context proxy.
*Injection:* `UserFunction.call` takes a map from variable name to injected object as its first argument, instead of only the state stores: `stores` (`INJECT_STORES`) for the store proxies and `context` (`INJECT_CONTEXT`) for the context proxy. `PythonFunction` sets every injectable variable on each call, to the injected object or to its default (no stores, `None`), and rejects unknown names. The generated caller saves the injected globals and restores them when it returns, because a call can run inside another one: a `filter` after `punctuate` runs during `context.forward`, and must not leave its `context = None` to the punctuator.
*Consequence:* `context` is a global name, like `stores`, so a global variable or store called `context` in a punctuator is overwritten during the call. The docs say so.
*Alternative:* `context` as a second parameter, passed as a `DataObject` wrapper around the proxy. Rejected: it needs a fake data type for a Java object, and the call API already had a way to hand objects to Python next to the parameters. A non-`None` return value is ignored, with a warning logged once per operation. A new constant `KSMLDSL.Functions.TYPE_PUNCTUATOR` and `KSMLDSL.Operations.PUNCTUATE`, plus `INTERVAL` and `PUNCTUATION_TYPE` field names, are added, so the parser and JSON schema use them.

**D8 — Errors are wrapped with context and rethrown.**
`PunctuateProcessor.punctuate` catches an exception from the Python call, wraps it in an `ExecutionException` whose message names the operation and the punctuation timestamp, and rethrows. Kafka Streams then handles it like an exception from `process`, which keeps the outcome the same as for other functions (spec: "Errors in a punctuator"). Records already passed to `forward` stay forwarded, as in a Java `Punctuator`.
*Alternative:* catch and log, then continue. Rejected: it would make punctuators the only functions that ignore the error handling configuration.

**D9 — Test runner: `advanceWallClockTime` on a produce block.**
`ProduceBlock` gets an optional `Duration`. `TestDataProducer` calls `TopologyTestDriver.advanceWallClockTime(duration)` after producing the block's messages. Stream-time punctuations need no new field: message timestamps already drive stream time in the test driver. The constant goes into `KSMLTestDSL.Produce`.

**D10 — Forwarding to a named operation further down: a side path per target.**
Kafka Streams' `forward(record, childName)` only reaches direct children of a processor node, and a punctuator cannot forward to an operation further down. So KSML makes every possible target a direct child at topology build time:

```
input ─► punctuate ──(main)──► compute ─► merge ─► enrich ─► ...
                  └──(side: enrich)────────┘
```

- `OperationChain` applies the operations of a pipeline (also of a `branch` sub-pipeline) and, after a `punctuate` operation, finds every later operation in the same `via` list that has a `name` and takes a `KStream` as input. Every operation gets a generated name, so `OperationConfig` and `BaseOperation` keep the name as written in the definition (`definedName`) to tell them apart.
- When a named operation follows, the next operation gets a named pass-through node (`<punctuate name>-next`), so the processor can forward input records to it by child name.
- For each one, `PunctuateOperation` adds a named pass-through node on the punctuate output (`<punctuate name>-to-<target name>`), and the pipeline builder merges that side stream into the main stream right before the target.
- `PunctuateProcessor` forwards input records and `forward()` calls without `to` only to the main child, by its child name, so side paths only carry records sent to them. A call with `to` forwards to the side node of that target.
- The context proxy gets a map from target name to child name and to the target's input key and value types, which it uses for the type check (D2) and to list the valid names in errors.

Every named stream operation becomes a target without extra syntax, because the user asked for every following operation in the pipeline to be reachable and there is no security reason to limit it.
*Cost:* one merge node per named stream operation after a `punctuate`, also when the punctuator never uses it. A merge is a pass-through, so the cost per record is small. A wrong name is found at runtime, with an error that lists the valid names.
*Not targetable:* operations whose input is a table or a grouped or windowed stream (a merge needs two `KStream`s), operations in other pipelines or inside `branch` sub-pipelines, and the pipeline's `to`. These can be added later if needed.
*Alternative:* a declared list of targets on the `punctuate` operation. Rejected with the user: it adds syntax without adding safety.
*Alternative:* one shared side path, merged before every target, with a header to filter on. Rejected: every target would need a filter, and the header would have to be removed again before output.

## Risks / Trade-offs

- **[Risk]** The extra pass-through and merge nodes shift the generated names of later unnamed processors. Adding a `punctuate` operation to an existing pipeline can therefore change the names of internal topics and stores of unnamed stateful operations after it, as any change to a pipeline can. → Document it, and advise naming stateful operations, as the Kafka Streams docs do.

- **[Risk]** When the process error handler is registered with Kafka Streams later, Kafka Streams calls it for punctuation errors with no record. `ErrorHandling.handle(ErrorHandlerContext, Record, Exception)` reads `rec.key()` and would fail. → Make `handle` accept a `null` record as part of this change, with a unit test, so the later work does not hit it.
- **[Risk]** A slow punctuator blocks the stream thread, so no records are processed and `max.poll.interval.ms` can be exceeded. → Document it, with the advice to keep punctuators short and iterate over stores with the existing iterator API.
- **[Risk]** Wall-clock punctuation is not exact: Kafka Streams checks it between poll loops, so a punctuation can be late under load or skipped calls are merged. → Document that `wallClockTime` runs "at least `interval` apart", as Kafka Streams does.
- **[Trade-off]** Type errors in forwarded records are found at runtime, not at startup, because a punctuator has no `resultType`. → `forward` raises a clear Python error naming both types, and the test runner lets users catch it before deployment.
- **[Risk]** `commit()` from a punctuator that runs often can cause many commits. → Document that it is a request and that Kafka Streams already commits every `commit.interval.ms`.
- **[Trade-off]** Re-keying (D1) makes later key-based operations repartition, also when the punctuator keeps the keys. Accepted; same as `transformKeyValue`.
- **[Trade-off]** Python state in `globalCode` is shared between tasks of the same stream thread. That is already true for every function and is not changed here.

## Migration Plan

New, opt-in syntax. No migration and no change for existing definitions. Rollback is removing the operation from a definition.
