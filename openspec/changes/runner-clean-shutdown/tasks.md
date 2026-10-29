## 1. Seams for testing (pure refactor, no behavior change)

- [x] 1.1 Extract the body of `KSMLRunner.main` into a package-private static `run(...)` method; keep today's behavior (the catch still rethrows via `FatalError.report`, `main` keeps its `System.exit(0)`, the schema-only path still returns without exit)
- [x] 1.2 Extract the shutdown hook body into a package-private static `shutdownRunners(Runner producer, Runner streams, CompletableFuture<?> done, Duration timeout)` that for now only calls `stopRunnersQuietly` (no wait), and let the hook call it
- [x] 1.3 Run `mvn test -pl ksml-runner -am` and confirm the existing tests still pass

## 2. Tests first — prove the defects

- [x] 2.1 `KafkaStreamsRunnerTest`: update `testErrorHandling` to verify that Kafka Streams in `ERROR` is closed and that `RunnerException` is still thrown (remove the "we don't verify close()" comment)
- [x] 2.2 `KSMLRunnerHelpersTest`/`KSMLRunnerTest`: test that a streams runner failure makes `run(...)` return exit code `1`
- [x] 2.3 `KSMLRunnerHelpersTest`: test that `runRunners` shuts its executor down (no live worker threads) when the streams runner fails
- [x] 2.4 `KSMLRunnerHelpersTest`: test that `shutdownRunners` stops the runners and waits until a slow runner's future completes, and gives up after the timeout
- [x] 2.5 `KSMLRunnerTestUtil.KSMLRunnerWrapper` / `KSMLContainer`: add `waitForExit(Duration)` (exit code if exited) and `terminate(Duration)` (SIGTERM, then wait)
- [x] 2.6 Add `RunnerShutdownIT` with resources under `ksml-integration-tests/src/test/resources/runner-shutdown/`: count pipeline, `commit.interval.ms: 300000`, produce 10 records, wait for count 10, terminate, assert the committed offset of the input topic is 10 via `Admin.listConsumerGroupOffsets`
- [x] 2.7 Add `RunnerFailureIT`: `mapValues` pipeline doing `amount // div`, produce a `div: 0` record from the test after start, assert the process exits with a non-zero code within 60 s
- [x] 2.8 Run the new unit tests and both ITs against the unfixed code; record the output and confirm each test fails for the expected reason (checkpoint: report to the user before continuing)

## 3. Fix

- [x] 3.1 Add a fixed 30 s close timeout: `KafkaStreamsRunner.CLOSE_TIMEOUT`, with the shutdown hook waiting 5 s longer (see design D2; started together with 1.2, which needs the timeout parameter)
- [x] 3.2 `KafkaStreamsRunner.run()`: always close Kafka Streams in `finally` with a bounded close (verify the Kafka 4.4 `CloseOptions` API), keep the existing log lines, keep throwing `RunnerException` for a failed state after the close
- [x] 3.3 `KSMLRunner.runRunners`: register the shutdown hook after creating the runner futures, passing `allOf(futures)` to `shutdownRunners`; move `closeExecutorService` into a `finally`; guard `removeShutdownHook` against `IllegalStateException`
- [x] 3.4 `shutdownRunners`: after stopping the runners, wait on the future with the timeout; log on timeout, interruption or failure
- [x] 3.5 `run(...)`/`main`: on a fatal error report it via `FatalError.report` and return `1`; `main` calls `System.exit(run(...))` except on the schema-only path
- [x] 3.6 Run all tests from section 2 and confirm they pass; run `mvn clean test -pl ksml-runner -am` including the coverage gate

## 4. Docs and verification

- [x] 4.1 `docs/reference/configuration-reference.md` "Error Handling Configuration": with `stopOnFail` the runner exits with a non-zero code so Docker/Kubernetes can restart it; on SIGTERM it commits and closes Kafka Streams (up to 30 s), so give the pod enough grace time
- [x] 4.2 `docs/release-notes.md`: add an entry for the new exit-code and shutdown behavior
- [x] 4.3 Run all ITs with `mvn verify -pl ksml-integration-tests -Pintegration-tests` (after `mvn install` of `ksml-runner`) (a `RepartitionIT` timeout under a full verify is a known flake, not a regression)
- [ ] 4.4 Optionally reproduce both problems manually with Docker (an image from `./build-local-docker.sh`, the compose setup in `docs/local-docker-compose-setup-with-sr/`): a `mapValues` dividing by zero must make the container exit non-zero; `docker compose stop ksml` right after a `count` must log `Stopping Kafka Streams` and commit offsets, with no recount after a restart
- [ ] 4.5 Have the static-membership restart scenario rerun with the external Kafka Streams comparison tests
