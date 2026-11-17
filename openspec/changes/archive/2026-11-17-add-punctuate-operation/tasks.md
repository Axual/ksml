## 1. Processor time and error handling groundwork

- [x] 1.1 `OperationProcessor`: replace `recordTimestamp` with a `currentTimestamp` that is set during `process` and by a new protected `final` method that runs an action with a given punctuation timestamp, and cleared afterwards (design D5); verify with a unit test that a store proxy write without a timestamp uses the record timestamp in `process`, the punctuation timestamp inside the new method, and raises the existing `ValueError` outside both
- [x] 1.2 `ErrorHandling.handle(ErrorHandlerContext, Record, Exception)`: accept a `null` record (punctuation errors), logging without key and value; verify with a unit test in `ErrorHandlingTest`
- [x] 1.3 Run `mvn test -pl ksml` and verify that all existing tests still pass

## 2. The `punctuator` function type

- [x] 2.1 Add `ProcessorContextProxy` in `io.axual.ksml.proxy.context` with exactly the exported methods of design D6 (`forward` with optional `timestamp`, `headers` and `to`, `commit`, `currentSystemTimeMs`, `currentStreamTimeMs`, `applicationId`, `taskId`), key/value conversion against the types of the next operation and of each named target, and a `valid` flag; verify with unit tests on a mocked `ProcessorContext`: the forwarded `Record` (default and explicit timestamp, headers, tombstone, `to` mapped to the right child name), a key or value of the wrong type for the next operation and for a named target, an unknown `to` name (error lists the valid names), malformed headers, each delegating method, and a call after the flag is cleared
- [x] 2.2 Verify from Python (GraalVM test) that only the exported methods are visible: `schedule`, `getStateStore` and `appConfigs` raise an attribute error
- [x] 2.3 Add `KSMLDSL.Functions.TYPE_PUNCTUATOR` and `PunctuatorDefinition` (built-in parameter `timestamp: long`; `context` is injected; custom parameters merged, no result type allowed) and register it in the function-type parser; verify with parser unit tests that a named and an inline `type: punctuator` function parse with `globalCode` and custom parameters, and that a declared `resultType` is rejected
- [x] 2.4 Add `UserPunctuator`, which calls the function with the timestamp, the context proxy and the store proxies, and logs a warning once when the function returns a value; verify with unit tests for a call that forwards records, a call that returns a value (warning, nothing forwarded) and a call that raises
- [x] 2.5 Document the `punctuator` function type and its `context` methods in `docs/reference/function-reference.md` (parameters, `forward` with timestamp and headers, the other methods, store access, timestamps, no return value) and add it to the "Function Types Overview" table; verify that the docs test module (`ksml-docs-test`) passes

## 3. The `punctuate` operation

- [x] 3.1 Add `KSMLDSL.Operations.PUNCTUATE`, `INTERVAL` and `PUNCTUATION_TYPE`, and `PunctuateOperationParser` (required `interval`, `punctuationType` via `enumField`, `punctuator`; optional `stores` and `name`), registered in `PipelineOperationParser`; verify with parser tests for a valid operation, each missing required field, an interval of `0`, and an unknown `punctuationType` (spec: "Punctuate operation", "Schedule interval")
- [x] 3.2 Add `PunctuateProcessor extends OperationProcessor`: pass records through unchanged, schedule the punctuator in `init`, cancel it in `close`, create a context proxy per call with the stream's key and value types and the punctuation timestamp, invalidate it after the call, and wrap Python errors with the operation name and timestamp (design D1, D4, D6, D8); verify with unit tests using a mocked `ProcessorContext`
- [x] 3.3 Add `PunctuateOperation` using `KStream.process` for streams, rejecting tables, grouped and windowed streams, and passing the stream's key and value types to the processor (design D1, D2); verify with topology-build tests for `punctuate` after a stream and after a table
- [x] 3.4 Wire a side path and a merge for every later named operation with a stream input in the same `via` list, and pass the target map to the processor (design D10); verify with topology-description tests that named stream operations get a side path and merge, and that unnamed operations, operations after `groupBy` or `toTable`, and operations in other pipelines do not
- [x] 3.5 Add `TopologyTestDriver` tests for the scenarios in `specs/punctuation/spec.md`: pass-through, wall-clock punctuation without input, stream-time punctuation (0 s, 30 s, 61 s), `context.forward` order, timestamps and headers into `to`, a `filter` after `punctuate`, `forward(..., to=...)` skipping an operation while input records still reach the target once, reading and deleting from a key-value store, a timestamped store write, a forwarded value of the wrong type, `context.commit()`, a `context` used after the call, and an exception after a forward; verify that they pass
- [x] 3.6 Regenerate `docs/ksml-language-spec.json` with the build and verify that it contains the `punctuate` operation and the `punctuator` function type with both `punctuationType` values
- [x] 3.7 Document the `punctuate` operation in `docs/reference/operation-reference.md`: fields, both punctuation types and their timing (at least `interval` apart, no stream-time calls without input), re-keying and repartitioning after it, forwarding to named operations and which operations can be targets, naming stateful operations after it, and keeping punctuators short; verify that `ksml-docs-test` passes

## 4. Test runner support

- [x] 4.1 Add `KSMLTestDSL.Produce.ADVANCE_WALL_CLOCK_TIME`, parse it as an optional duration on `ProduceBlock`, and include it in the generated test JSON schema (`docs/ksml-test-spec.json`); verify with parser tests for a valid duration, a negative duration and an invalid value
- [x] 4.2 In `TestDataProducer`, call `TopologyTestDriver.advanceWallClockTime` after producing a block's messages; verify with a test-runner suite under `ksml-test-runner/src/test/resources` that has a wall-clock punctuator, interleaved produce blocks and a block without messages (spec: "Advancing wall-clock time after a produce block")
- [x] 4.3 Document `advanceWallClockTime` in the test-runner docs (`ksml-test-runner/README.md` and the test-definition section of the docs site); verify that the documented example runs with the test runner

## 5. Example and integration

- [x] 5.1 Add `examples/21-example-punctuate.yaml` that sums values per key in a store and emits and clears the totals every minute, and add it to `ExampleTopologyTest`; verify that the example test passes
- [x] 5.2 Add an entry to `docs/release-notes.md`
- [x] 5.3 Run `mvn clean install` for the whole project, including the coverage gates, and verify that it passes
- [x] 5.4 Run the example with `docker compose` and the locally built image, and verify in the output topic that totals are emitted every minute also when no new records arrive

## Workflow follow-up

- After implementation and review, archive the change with `openspec archive add-punctuate-operation`.
