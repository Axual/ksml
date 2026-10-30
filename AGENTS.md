# AGENTS.md

Instructions for AI coding assistants, and a short guide for people, working on KSML. The full rules for contributions are in [CONTRIBUTING.md](CONTRIBUTING.md).

Claude Code reads this file only when there is no `CLAUDE.md`. If you have your own `CLAUDE.md` (it is ignored by git), add the line `@AGENTS.md` to it.

## KSML in short

KSML (Kafka Streams Made Language) lets you write a Kafka Streams application in YAML instead of Java. KSML reads the YAML definition and builds a normal Kafka Streams topology from it. Small pieces of logic are written in Python, which runs inside the JVM with GraalPy.

## Build and test

KSML needs a GraalVM JDK (the version is in `.graalvm-jdk-version`). On another JDK, the KSML tests are skipped.

```shell
mvn clean package                        # build
mvn clean test                           # unit tests
mvn -T1C clean verify -DskipITs=false    # all tests, as CI runs them; the integration tests need Docker
```

Run one test class or one test method. Tests in a module such as `ksml/` need the other modules built first, so use `-pl` for the module and `-am` for its dependencies:

```shell
mvn test -pl ksml -am -Dtest=TestClassName -Dsurefire.failIfNoSpecifiedTests=false
mvn test -pl ksml -am -Dtest=TestClassName#testMethodName -Dsurefire.failIfNoSpecifiedTests=false
```

`-Dsurefire.failIfNoSpecifiedTests=false` stops the other modules from failing because they do not have that test.

## Parity tests: KSML compared with Kafka Streams

The parity tests check that KSML behaves like Kafka Streams. Each case gives the same input to a KSML definition and to the same topology written in plain Kafka Streams (Java), and compares the output. The code is in `ksml/src/test/java/io/axual/ksml/parity/`, and the KSML side of each case is in `ksml/src/test/resources/parity/`. The details are in [developerdocs/parity-tests.md](developerdocs/parity-tests.md).

- A new operation, or a change to how an operation behaves, needs a parity case:
  - write the KSML side as a YAML file in `ksml/src/test/resources/parity/`;
  - add a test method to the matching `*ParityTest.java` class;
  - write the Kafka Streams side from the Kafka Streams documentation, not from the KSML code, otherwise it copies KSML's behaviour;
  - include edge input: a null key, a tombstone (a record with a null value), a late record;
  - check in the report that both sides really write records. A case where both sides write nothing is `SAME`, but it proves nothing.
- Run the parity cases (about a minute, also part of every `mvn test`):

  ```shell
  mvn test -pl ksml -am -Dtest='*ParityTest' -Dsurefire.failIfNoSpecifiedTests=false
  ```

- A known difference has a marker, `.knownDifference("Fxx", "why", n)`, where `n` is the number of differences the report lists for the case.
  - If a case fails with "known difference Fxx is gone", check the report to confirm that both sides write records, then remove the marker in the same pull request.
  - If a case fails with "known difference Fxx changed", read the report. If the change is right, update the number in the marker, and the text if the reason changed. If not, it is a new problem.
  - The tests do not update any issue tracker. When a marker goes, also update the issue that tracks that finding.
- After changing `ParityCase` or `DockerParity`, run at least one case in Docker mode. Test driver mode cannot show class path problems of the reference app that runs in a container:

  ```shell
  ./build-local-docker.sh    # builds the image axual/ksml:local
  mvn test -pl ksml -am -Dtest='KStreamStatelessParityTest#c01FilterAndFilterNot' -Dparity.mode=docker -Dsurefire.failIfNoSpecifiedTests=false
  ```

- The files in `ksml/src/test/resources/parity/usability/` contain typical user mistakes on purpose. Do not fix them.

## Code layout

| Module | What it is |
|---|---|
| `ksml/` | KSML core: parses the YAML and builds the Kafka Streams topology. `TopologyGenerator` is the main entry point, `definition/` holds the classes for YAML elements, `operation/` the operations (filter, map, join and so on), `python/` the Python execution |
| `ksml-data-*` | Data formats, one module each (Avro, JSON, JSON Schema, XML, CSV, Protobuf, Binary), with schema registry support for Confluent and Apicurio |
| `ksml-runner/` | The standalone application that runs KSML definitions (`KSMLRunner.java`). It loads the configuration and runs Kafka Streams |
| `ksml-query/` | REST API to query state stores |
| `ksml-kafka-clients/` | Kafka clients (producer, consumer, admin) that can resolve topic and group names |
| `ksml-test-runner/` | Runs YAML test definitions for KSML pipelines |
| `ksml-integration-tests/` | Integration tests with real Kafka and schema registries (Testcontainers) |
| `ksml-examples-test/` | Automated tests for the definitions in `examples/` |
| `ksml-docs-test/` | Checks every definition and runner YAML file in the docs and the modules against the KSML schemas |
| `ksml-reporting/` | Combines the code coverage reports of all modules |

## KSML concepts

- Streams and tables: the inputs (`KStream`, `KTable`, `GlobalKTable`).
- Functions: custom logic in Python, inline or from a file.
- Pipelines: processing chains, from an input, through a list of operations, to an output.
- Operations: built-in steps such as filter, map, join and aggregate.
- State stores: storage for stateful operations.

```yaml
streams:        # input streams and tables
functions:      # reusable Python functions
pipelines:      # processing pipelines
  - from:       # input stream
    via:        # list of operations
    to:         # output topic
```

## Writing tests

- Use the KSML test annotations for definitions: `@ExtendWith(KSMLTestExtension.class)`, `@KSMLTopic(topic = "...")` and `@KSMLTest(topology = "pipelines/....yaml")`.
- Use Awaitility to wait for something, not `Thread.sleep` or your own wait loops.
- Working examples of KSML definitions are in `examples/` (with numbered prefixes), `ksml/src/test/resources/docs-examples/` and `ksml-integration-tests/src/test/resources/docs-examples/`. Look there when you are not sure about the syntax.

## Docs and docs examples

The docs are in `docs/`. The example definitions that the docs show are in `ksml/src/test/resources/docs-examples/` (and some in `ksml-integration-tests/src/test/resources/docs-examples/`). The docs pull them in with `include`, as in `docs/tutorials/beginner/data-formats.md`.

- Every processing example has a matching producer example, so people can run both. The topic names in both must match.
- Explain each processing example briefly: what it does and why it is useful.
- The examples show KSML syntax, so the Python code can be short and simple.
- Use `convertValue` for format conversions (for example Avro to JSON to string).
- Use `valueType: json` in examples, unless the example is about another format. JSON messages are easy to read in the Kowl UI.
- Put a blank line before every Markdown list, so it renders as a list.
- Write bullet points as short, natural sentences, without a leading keyword.

Test a docs example with the Docker Compose setup in `docs/local-docker-compose-setup-with-sr/`:

1. Check that every topic the definitions use is created by the `kafka-setup` service in `docker-compose.yml`. If one is missing, add it for the test, and remove it again afterwards.
2. Put the producer and processor definitions in `examples/`, and reference them in the `definitions` section of `examples/ksml-runner.yaml`.
3. Start fresh and follow the KSML log:

   ```shell
   docker compose down --volumes && docker compose up -d && docker compose logs ksml -f
   ```

4. It works when the log has no errors and shows the expected messages. If there are errors, fix the definitions by comparing them with the working examples above.
5. If you are sure the problem is a bug in KSML, report it with the producer and processor definitions, the steps to reproduce, and why you think it is a bug.
