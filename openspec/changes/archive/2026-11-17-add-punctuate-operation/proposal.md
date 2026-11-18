## Why

Kafka Streams lets a processor schedule a `Punctuator` that runs on a wall-clock or stream-time interval, with access to the processor's state stores and the ability to forward records. KSML has no way to do this, so common patterns cannot be written in a KSML definition: emitting aggregated state every minute, expiring old entries from a store, or sending a heartbeat when no records arrive. Users have to fall back to plain Kafka Streams for these cases.

## What Changes

- A new pipeline operation `punctuate` that can be placed in a pipeline's `via` list on a stream. Records pass through it unchanged. On a schedule, it calls a Python function.
- The operation takes:
  - `interval`: the duration between calls (KSML duration syntax, for example `30s` or `1m`).
  - `punctuationType`: `wallClockTime` or `streamTime`, as in Kafka Streams' `PunctuationType`. Required, with no default.
  - `punctuator`: a function, inline or by name.
  - `stores`: the state stores the function may use, as for other stateful operations.
- A new function type `punctuator`. It is called with the punctuation `timestamp` and a `context` object, may read and write the attached stores, and returns nothing.
- The `context` object gives the punctuator a selection of Kafka Streams' `ProcessorContext`:
  - `forward(key, value, timestamp=None, headers=None, to=None)` sends a record to the next operation, or with `to` to any named operation further down the same pipeline. The timestamp defaults to the punctuation timestamp. Headers use the same format as in `metadataTransformer`.
  - `commit()`, `currentSystemTimeMs()`, `currentStreamTimeMs()`, `applicationId()` and `taskId()`.
  - Methods that could break KSML's guarantees are not exposed: scheduling, store lookup by name, the application config, serdes and metrics.
- Records passed to `context.forward` are checked against the key and value types of the stream.
- Writes to timestamped stores during a punctuation use the punctuation timestamp, instead of failing because no record is being processed.
- The KSML test runner gets a way to advance wall-clock time in a test, so `wallClockTime` punctuators can be tested. Stream-time punctuators can already be tested by producing records with timestamps.
- JSON schema (`docs/ksml-language-spec.json`), operation and function reference docs, an example and release notes are updated.

No breaking changes. Existing definitions behave the same.

## Capabilities

### New Capabilities
- `punctuation`: scheduling Python functions in a pipeline on a wall-clock or stream-time interval. Covers the `punctuate` operation, the `punctuator` function type, the `context` object and its methods, store access and record forwarding during a punctuation, timestamps, typing and error handling.

### Modified Capabilities
- `test-definition-format`: a test step to advance the wall-clock time of the `TopologyTestDriver`, so wall-clock punctuations run in a test.

## Impact

- **Code (ksml)**: new `PunctuateOperation`, `PunctuateOperationParser`, a processor that schedules the punctuator in `init` and passes records through, `PunctuatorDefinition` and `UserPunctuator`, a Python proxy for the punctuation context, a new constant in `KSMLDSL`, registration in `PipelineOperationParser` and the function-type parser. The store proxies need a timestamp source for punctuations next to the record timestamp in `OperationProcessor`.
- **Code (ksml-test-runner)**: parsing and executing the new time-advance step.
- **Tests**: unit tests for parsing and typing, `TopologyTestDriver` tests for both punctuation types, store writes and forwarded records, and a test-runner suite.
- **Docs**: `docs/ksml-language-spec.json`, `docs/reference/operation-reference.md`, `docs/reference/function-reference.md`, the test-definition docs, an example under `examples/`, `docs/release-notes.md`.
- **Dependencies**: none. Uses the Kafka Streams 4.4 Processor API (`ProcessorContext.schedule`).
- **Out of scope**: a `context` object in other function types (the proxy is built so it can be reused later), punctuation on tables, cancelling or changing a schedule from Python, more than one schedule per operation (use several `punctuate` operations), and punctuation in KSML producers.
