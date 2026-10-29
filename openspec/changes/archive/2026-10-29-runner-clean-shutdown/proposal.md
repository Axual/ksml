## Why

The KSML runner does not stop the right way in two cases, found by tests that compare KSML with plain Kafka Streams:

1. **The process keeps running when Kafka Streams fails.** Kafka Streams goes to `ERROR`, but the JVM stays alive, so Docker and Kubernetes never restart it.
2. **Kafka Streams is not closed on SIGTERM.** Offsets are not committed, so records are processed again after a restart (`count` counts them twice). With static membership the restart can fail.

Both problems are in the same shutdown code, so they are fixed together. Details are in `design.md`.

## What Changes

- The runner process SHALL exit with a non-zero exit code when Kafka Streams or the producer fails at runtime, instead of leaving the JVM running.
- `KafkaStreamsRunner` SHALL close the Kafka Streams client in every terminal case, including when it is in `ERROR`, using a bounded close timeout.
- `KSMLRunner.runRunners` SHALL always shut down its executor service, also when a runner completes exceptionally.
- The JVM shutdown hook SHALL stop the producer and streams runners and then wait (bounded by a fixed 30 s close timeout plus a 5 s margin) until they have finished, so that Kafka Streams commits its offsets and closes before the JVM exits.
- Removing the shutdown hook after the runners finish SHALL tolerate a JVM shutdown already being in progress.
- `KSMLRunner.main` is split into a testable `run(...)` method that returns the exit code, and a thin `main` that calls `System.exit` with it (except for the schema-printing path, which keeps returning normally).
- New unit tests and two new integration tests (real SIGTERM with committed offsets check; runtime failure with non-zero exit code check) are added **before** the fix, and are shown to fail against the current code.
- Documentation: `docs/reference/configuration-reference.md` (Error Handling Configuration) and `docs/release-notes.md`.

No KSML YAML syntax, runner configuration or JSON schema changes. No breaking changes.

## Capabilities

### New Capabilities
- `runner-lifecycle`: how the KSML runner process ends — exit codes on runtime failure, graceful and bounded shutdown on SIGTERM, and closing Kafka Streams in every terminal state.

### Modified Capabilities
(none — no existing spec covers the runner process lifecycle)

## Impact

- **Code**: `ksml-runner/src/main/java/io/axual/ksml/runner/KSMLRunner.java` (`main`, `runRunners`, shutdown hook, new `run`/`shutdownRunners` helpers), `ksml-runner/src/main/java/io/axual/ksml/runner/backend/KafkaStreamsRunner.java` (`run()` close logic, `CLOSE_TIMEOUT` constant).
- **Tests**: `KSMLRunnerTest`, `KSMLRunnerHelpersTest`, `KafkaStreamsRunnerTest` (ksml-runner); new `RunnerShutdownIT` and `RunnerFailureIT` plus small additions to `KSMLRunnerTestUtil.KSMLRunnerWrapper` and `KSMLContainer` (ksml-integration-tests).
- **Operations**: on SIGTERM the process now takes up to 35 s to commit and close; Kubernetes pods need a `terminationGracePeriodSeconds` of at least that (the Kubernetes default is 30 s). A failing application now exits non-zero, so orchestrators restart it.
- **Docs**: configuration reference and release notes.
- **Dependencies**: none.
- **Out of scope**: registering the KSML process error handler with Kafka Streams, and changing state store defaults; both are tracked separately. The static-membership restart scenario (`group.protocol: streams` with `group.instance.id`, a count across a restart) is verified with the external Kafka Streams comparison tests, not in this repository.
