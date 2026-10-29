## Context

`KSMLRunner.main` builds a `KafkaProducerRunner` and/or a `KafkaStreamsRunner` and hands them to `runRunners`, which runs both on a two-thread executor, registers a JVM shutdown hook, and waits for both futures with `CompletableFuture.allOf(...).join()`. Each runner's `stop()` only sets an `AtomicBoolean`; the runner's own thread notices the flag (the streams runner polls every 200 ms) and then cleans up — for the streams runner that is `kafkaStreams.close()` in the `finally` of `run()`.

Analysis of `main` at 995a4dde found five causes behind the two problems — two that were already known and three more that would keep the bug alive if only the known ones were fixed:

**Process keeps running after a runtime failure**
1. `main`'s `catch (Throwable t)` rethrows `FatalError.report(t)`, so the trailing `System.exit(0)` is never reached. An exception escaping `main` only ends the main thread; any remaining non-daemon thread keeps the JVM alive.
2. In `runRunners`, `allOf(...).join()` throws `CompletionException` when a runner fails, so `closeExecutorService(executorService)` is skipped and the fixed pool's non-daemon worker threads stay alive.
3. `KafkaStreamsRunner.run()` only calls `kafkaStreams.close()` when `isRunning()`; in `ERROR` the client is never closed.

**Kafka Streams not closed on SIGTERM**
4. The shutdown hook calls `stopRunnersQuietly`, which only calls `Runner.stop()` (sets a flag) and returns. The JVM halts as soon as all hooks return, before the runner thread reaches `kafkaStreams.close()`.
5. Once the hook waits, the main thread's `join()` returns during shutdown and `runRunners`' `finally` calls `Runtime.removeShutdownHook`, which throws `IllegalStateException("Shutdown in progress")` in that situation.

## Goals / Non-Goals

**Goals:**
- Non-zero exit code whenever a runner fails at runtime, promptly after the failure is logged.
- On SIGTERM: stop the runners, let Kafka Streams commit and close, and let the producer close, before the JVM exits — bounded in time.
- Close the Kafka Streams client in every terminal state.
- Prove the defects with tests that fail on the current code before fixing them (test-first).

**Non-Goals:**
- Making the shutdown timeout configurable.
- Changing the uncaught-exception handler policy (`ErrorHandling.uncaughtException` keeps returning `SHUTDOWN_CLIENT`) or registering the process error handler with Kafka Streams (tracked separately).
- Changing state store defaults (tracked separately).
- Leaving the consumer group explicitly on close; the default close matches plain Kafka Streams, which is what the static-membership restart comparison test checks against.

## Decisions

**D1 — The shutdown hook waits on the runner futures instead of closing the backends itself.**
The hook calls `stopRunnersQuietly(producer, streams)` and then waits on `allOf(producerFuture, streamsFuture)` with a timeout. The runner threads stay the only owners of their clients, so there is no concurrent `close()` from two threads.
*Alternative:* the hook calls `kafkaStreams.close()` directly (pre-PR #86 behavior). Rejected: it duplicates ownership and races with the runner's own `finally`.
Consequence: the hook needs something to wait on before the runners are submitted, because it is registered first (as before, so a signal during start-up is still handled). `runRunners` therefore gives the hook a `runnersDone` future that is completed when both runner futures complete, and also in `runRunners`' `finally`, so a waiting hook is released when the runners never started.

**D2 — A fixed close timeout of 30 s.**
`KafkaStreamsRunner.CLOSE_TIMEOUT` is used as the Kafka Streams close timeout; the hook waits that plus 5 s so a full close can finish. The constant lives in `KafkaStreamsRunner` rather than `KSMLRunner`, because the `backend` package does not depend on `KSMLRunner` and should not start to. 30 s matches the Kubernetes default `terminationGracePeriodSeconds`. Decided with the user to keep it a constant rather than a new configuration option, to avoid growing the config surface and schema.

**D3 — Always close Kafka Streams, with a bounded close.**
`KafkaStreamsRunner.run()` closes the client in `finally` regardless of state, using the bounded close of the Kafka 4.4 API (`CloseOptions` with a timeout). It logs `Stopping Kafka Streams` when it was running (the manual reproduction looks for it), `Kafka Streams failed, closing it` in a failed state, and `Kafka Streams has stopped` otherwise. The failed state is read before the close, so the result does not depend on the state Kafka Streams leaves after `close()`. The `RunnerException` for a failed state is still thrown, after the close.

**D4 — Exit code returned from a testable `run(...)` method.**
`main`'s body moves into a package-private static `run(...)` that returns `0` on success and `1` after reporting a fatal error via `FatalError.report`. `main` calls `System.exit(run(...))`. The schema-printing path keeps returning from `main` without `System.exit`, because `KSMLRunnerTest` calls `main` in-process for it. Testing an exit code without calling `System.exit` in the test JVM is the main reason for this split.
*Alternative:* intercept `System.exit` in tests. Rejected: `SecurityManager` is gone in current JDKs and agent-based interception is heavy.

**D5 — `closeExecutorService` in a `finally`; `removeShutdownHook` guarded.**
Prevents the lingering pool threads (cause 2) and tolerates removal during shutdown (cause 5).

**D6 — Test-first, with seams introduced as pure refactors.**
Phase 1 first extracts `run(...)` and the hook body `shutdownRunners(...)` **without** behavior change, so new unit tests compile and fail on the real defect rather than on a missing symbol. Then the new tests and ITs are written and run, and their failures checked against the expected reason. Only then is the fix applied.

**D7 — Integration tests run the real runner out of process.**
`KSMLContainer` uses `KSMLRunnerTestUtil.KSMLRunnerWrapper`, which starts `KSMLRunner` in a separate JVM from the current test classpath, and `Process.destroy()` sends a real SIGTERM. So the ITs exercise the current source without a Docker image build.
- `RunnerShutdownIT` uses a `count` pipeline and sets `commit.interval.ms: 300000`, so the only way an offset can be committed is the commit at close. That makes the pre-fix failure deterministic instead of depending on hitting the 30 s default commit window.
- `RunnerFailureIT` produces the failing (`div: 0`) record from the test after the runner has started, so a fixed runner does not exit within `waitForReady`'s start-up window and break `start()`.
- `KSMLRunnerWrapper` gets `waitForExit(Duration)` (exit code, if it exited) and `terminate(Duration)` (SIGTERM and wait); the existing `stop()` only waits 10 s before `destroyForcibly`, which is less than the close budget.

## Risks / Trade-offs

- [Shutdown can now take up to 35 s] → Documented; orchestrators need a grace period of at least that. Kubernetes' default is exactly 30 s, so a close that needs the full budget may be killed at the edge — acceptable since a normal close takes far less, and the docs advise raising the grace period.
- [`System.exit` called from `main` while shutdown hooks are running blocks that thread] → Harmless: the JVM exits once the hooks finish; the exit code is the one from the signal (143), as before.
- [Producer close timing] → `KafkaProducerRunner` closes its producer via try-with-resources when its loop ends; the hook's wait covers that too. A producer blocked in a long send is bounded by the same timeout.
- [Executor `shutdownNow` interrupting a runner] → Only after the runners' futures have completed or the 2 s grace in `closeExecutorService` elapsed; same as today on the success path.
- [IT timing flakiness] → Tests poll with timeouts rather than sleeping; the commit interval trick removes the dependency on the 30 s commit window.

## Migration Plan

No migration: no configuration or API change for users. Rollback is reverting the change. Release notes describe the new exit-code and shutdown behavior and the grace-period advice.

## Open Questions

None left. Resolved during implementation:

- Kafka 4.4.0 has `org.apache.kafka.streams.CloseOptions.timeout(Duration)`, used with `KafkaStreams.close(CloseOptions)`; the nested `KafkaStreams.CloseOptions` is the older variant.
- Integration tests are skipped by default; run single ones with `mvn verify -pl ksml-integration-tests -Pintegration-tests -Dit.test=RunnerShutdownIT,RunnerFailureIT`.
