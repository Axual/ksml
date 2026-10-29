# Parity tests: does KSML behave like Kafka Streams?

KSML builds a Kafka Streams topology from a YAML definition. The parity tests check
that this topology behaves like the same topology written in plain Java with the
Kafka Streams DSL. They found real bugs, for example tombstones that are counted,
joins that cannot be built, and state that is lost after a restart. Each confirmed
difference is marked in its test, so a fix shows up as a failing test that you then
update.

The code is in `ksml/src/test/java/io/axual/ksml/parity/`, and the KSML side of each
case is in `ksml/src/test/resources/parity/`.

## How a case works

Each case gives the same input records to two programs:

- a plain Kafka Streams app, written in Java (the reference);
- a KSML definition, written in YAML.

Then it compares every output record (key, value, timestamp and order) and whether the
app fails. A case can also compare headers, state stores (test driver mode only), the
topology shape and the number of changelog topics. In test driver mode each case runs
with topology optimization `none` and `all`. Docker mode uses `all`.

All cases use string keys and JSON values, simple functions, and edge input such as
null keys, tombstones and late records. The cases were chosen to find problems, so the
number of differences is not a pass rate for KSML in general.

## Two run modes

| | Test driver mode (default) | Docker mode |
|---|---|---|
| Kafka | No broker. `TopologyTestDriver` runs both topologies in memory | Two real Kafka brokers, one per side, started with Docker Compose |
| Kafka Streams side | The Java topology, on its own test driver | The same Java topology, run as an app in a container |
| KSML side | The YAML, turned into a topology by the KSML code in this repo | The YAML, run by the KSML runner in a Docker image built from this repo |
| Run time | Under a minute | About 25 minutes |
| Good for | Exact, repeatable checks in every build | Restarts, several partitions, internal topics, apps that fail |
| When it runs | In every `mvn test` | Every night in CI, or by hand |

## Run the tests

Like all KSML tests, the parity tests need a GraalVM JDK. On another JDK they are
skipped.

### Test driver mode

The cases run in every `mvn test`. To run only the parity cases:

```shell
mvn test -pl ksml -am -Dtest='*ParityTest' -Dsurefire.failIfNoSpecifiedTests=false
```

The Docker-only cases are skipped in this mode.

### Docker mode

1. Build the image from the repo: `./build-local-docker.sh`. It creates
   `axual/ksml:local`.
2. Run the cases:

   ```shell
   mvn test -pl ksml -am -Dtest='*ParityTest' -Dparity.mode=docker -Dsurefire.failIfNoSpecifiedTests=false
   ```

   To use another image, add `-Dparity.image=<image>`.
3. Stop the brokers afterwards:
   `docker compose -f parity-docker/docker-compose.yml --profile apps down -v`.

Docker mode uses the host ports 19092 and 29092. It creates and deletes topics only on
its own two brokers (`parity-docker/docker-compose.yml`).

On Linux, run the apps as your own user, so the test can delete the state that the
apps write into `ksml/target/parity-docker-run/`:

```shell
export PARITY_USER="$(id -u):0"
```

On macOS this is not needed, because Docker maps the file owner.

Docker mode never fails a test. It only writes the report, because real Kafka timing
can make small differences between runs. Read the report instead. Even a case that
could not run, for example because Docker is not running, only shows up there.

### Every night in CI

The workflow `.github/workflows/parity-nightly.yml` runs Docker mode every night on
`main`, with an image built from `main`. You can also start it by hand from the Actions
tab. It does not run on pull requests, because a full run takes 21 to 27 minutes.

The job summary counts the cases per status. The job fails when the report has a
`DIFF`, a `HARNESS-ERROR` or a `MARKER-GONE` line, and lists those cases. The full report
is kept as the build artifact `parity-report-docker-<run number>` for 30 days.

When the job fails, read the case file and both app logs in the artifact. A difference
where only the order of records with different keys changed is timing, not a bug: give
the case `anyOrderInDocker()`. A case that is not stable on real Kafka for another
reason gets `skipInDocker(reason)`.

## Read the results

Test driver mode writes to `ksml/target/parity-report/`, Docker mode to
`ksml/target/parity-report-docker/`. `summary.tsv` has one line per case (and per
optimization setting in test driver mode) for every run. A new run adds its lines to the
file, so run `mvn clean` first, or delete the file, to start a new report.

- `SAME`: KSML wrote the same as Kafka Streams.
- `KNOWN-DIFF Fxx`: a confirmed difference, see [Known differences](#known-differences).
- `DIFF`: a new difference. In test driver mode this fails the test.
- `MARKER-GONE Fxx` (Docker-only cases): the case has a marker, but KSML now writes the
  same as Kafka Streams.
- `HARNESS-ERROR` (Docker mode only): the case did not run.

For each case there is also a file such as `C12-none.txt`, with the differences, both
topologies, both outputs and the stores. Docker mode also keeps the log of both apps
for each case. The usability probes write what KSML reports for typical mistakes to
`usability.txt`.

## Known differences

A case with `.knownDifference("Fxx", "...", n)` documents a difference that was found
and confirmed. `n` is the number of differences that the report lists for the case, for
both optimization settings together. The test passes while that number stays the same.

The test fails as soon as the number changes:

- When KSML is fixed, it fails with "known difference Fxx is gone, KSML now behaves like
  Kafka Streams. Remove the marker." Remove the marker in the same pull request as the
  fix. From then on the case guards the fix.
- When a fix changes the difference, or a new problem adds to it, it fails with "known
  difference Fxx changed". Read the report of the case. If the change is right, update
  the number, and the marker text if the reason changed.

The number is not a perfect guard: a case lists at most 12 wrong records per topic, so a
new problem in a topic that already shows 12 may not change it.

A Docker-only case uses `.knownDifference("Fxx", "...")` without a number, because Docker
mode does not fail a test. When that difference is gone, the report shows
`MARKER-GONE Fxx`, and the nightly job fails.

| Finding | What is different | Cases |
|---|---|---|
| F01 | A tombstone is not `null` inside Kafka Streams. Counts are wrong, functions get `None` and stop the app, joins write data instead of a delete | C12, C14, C15, C15b, C21, C33, C35, C36e |
| F05 | The KTable `aggregate` subtractor has the wrong parameter type | C21b |
| F06 | A store with default options is in memory and has no changelog, so it loses its state on restart | C44a (Docker only) |
| F07 | A global table join throws when the key mapper returns `None` | C34 |
| F08 | The stream-stream `outerJoin` passes no key to the joiner | C32, C42 |
| F09 | `toStream` with a `mapper` fails on a windowed table | C24b |
| F10 | `shutdownWhenFull` is ignored for `suppress` with `until: timeLimit` | C22b |
| F12 | `optimization: all` does not merge repartition topics after a KSML `selectKey` | C41 |
| F14 | A required store with a wrong retention changes the result | C27 |
| F16 | KSML always materializes a source table (by design) | C18 |
| F21 | No foreign-key extractor that reads the key | C38 |
| F27 | The runner does not close Kafka Streams on SIGTERM, so a static member cannot join again | C53 (Docker only) |

Some problems have no case of their own. The runner keeps running after Kafka Streams
fails (see the notes of C14, C21, C43 and others in Docker mode), and unknown fields in a
definition are ignored (usability probes u03, u11 and u12).

Fixed so far, and now guarded by their cases:

- `cogroup` with two streams (C17).
- The table-table foreign-key join: swapped types (F02) and other key types (F03) (C36b,
  C36d).
- `foreignKeyExtractor` on a stream join is rejected at startup (F04). The unit tests
  `JoinWithTableOperationTest` and `LeftJoinWithTableOperationTest` guard this. C36c
  checks the documented way, `toTable` before the join.

## Add a case

A new operation, or a change to how an operation behaves, needs a parity case.

1. Write the KSML side as a YAML file in `ksml/src/test/resources/parity/`.
2. Add a test method to the matching `*ParityTest.java`, for example:

   ```java
   @Test
   void c60Example() {
       ParityCase.of("C60", "what this case checks")
               .input("in").output("out")
               .reference(builder -> builder.stream("in", CONSUMED)
                       .filter((k, v) -> v != null)
                       .to("out", PRODUCED))
               .ksml("parity/c60-example.yaml")
               .pipe("in", "k1", "{\"amount\":5}", 1000)
               .pipe("in", "k2", null, 2000)
               .check();
   }
   ```

3. Write the Kafka Streams side from the Kafka Streams docs, not from the KSML code.
   Otherwise the reference copies KSML's behaviour.
4. Include edge input: a null key, a tombstone, a late record.
5. Check in the report that both sides really write records. A case where both sides
   write nothing is `SAME`, but it proves nothing.
6. If the case shows a real difference, add `.knownDifference("Fxx", "...", n)` with a
   new finding number and the number of differences in the report, add it to the table
   above, and open an issue for it. Keep the inputs that differ in a small case of their
   own, so the rest of the scenario stays in a case without a marker.

Useful options on `ParityCase`:

- `store(name)` compares a state store.
- `config(key, value)` sets a Kafka Streams setting on both sides, for example
  `dsl.store.format` or `group.protocol`.
- `compareHeaders()`, `compareShape()` (topology shape) and `compareChangelogs()`.
- `partitions(topic, n)` and `restartAfter(step)` for Docker mode.
- `dockerOnly()` and `skipInDocker(reason)`.
- `global(topic)` for a global table topic.
- `compareFinalValues()` for topics with several partitions.
- `anyOrderInDocker()` when several tasks write to the same output partition, for example after a
  `repartition`: in Docker mode the records are then compared per key, so only the order
  between keys is ignored. Records with the same key keep their order, which holds as long
  as the key decides the partition.

## Files

| File | Role |
|---|---|
| `ParityCase.java` | Builder for one case: the reference topology, the KSML file, the input, and what to compare. Also the run modes |
| `ParityRunner.java` | Builds one side and runs it on a `TopologyTestDriver` |
| `ParityComparison.java` | Compares both sides and lists the differences |
| `ParityReport.java` | Writes the report |
| `DockerParity.java` | Docker mode: creates topics, writes the input, starts both apps with Docker Compose, waits, and reads the output |
| `ReferenceApp.java` | The Kafka Streams app that Docker mode runs in the `reference` container |
| `Format.java`, `Normalizer.java`, `Json.java`, `SideResult.java` | Helpers |
| `*ParityTest.java` | The cases, grouped by area: stateless, grouped streams, tables, windows, joins, topology, Docker only, usability probes, and the Kafka 4.4 features |
