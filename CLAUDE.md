# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KSML is a Java-based wrapper around Apache Kafka Streams that allows users to define stream processing topologies using YAML configuration files with inline Python functions. It eliminates the need to write Java code for stream processing applications.

## Commands

GraalVM is required to build and run KSML, because Python functions run on GraalPy. The project compiles for Java 23, and `.graalvm-jdk-version` names the GraalVM version that CI and the image use.

```bash
# Build all modules
mvn clean package

# Run all unit tests
mvn clean test

# Run unit and integration tests
mvn clean verify

# Check license headers
mvn license:check-file-header
```

Tests in a submodule (for example `ksml/`) need their dependencies built first. Use `-pl` to pick the module and `-am` to build what it needs:

```bash
mvn test -pl ksml -am -Dtest=TestClassName -Dsurefire.failIfNoSpecifiedTests=false
mvn test -pl ksml -am -Dtest=TestClassName#testMethodName -Dsurefire.failIfNoSpecifiedTests=false
```

`-Dsurefire.failIfNoSpecifiedTests=false` stops the other modules from failing when they do not have the test class.

Build the local Docker image `axual/ksml:local`:

```bash
./build-local-docker.sh
```

The script builds the jars, downloads GraalVM and runs the Docker build. Do not run `docker buildx build` alone, because the `Dockerfile` needs the `build-output/` folder and the GraalVM tar files that the script creates.

## Architecture Overview

### Module Structure

- **ksml** - Core module that parses KSML YAML definitions and builds Kafka Streams topologies
- **ksml-data** - Core data type and schema logic
- **ksml-data-*** - Data format modules (avro, json, xml, csv, protobuf, binary, soap)
- **ksml-kafka-clients** - Custom Kafka clients with namespace support
- **ksml-runner** - Standalone application to run KSML definitions
- **ksml-query** - REST API for querying Kafka Streams state stores
- **ksml-reporting** - Reporting and metrics functionality

### Key Design Patterns

1. **Definition Parser**: The core module uses a hierarchical parser to convert YAML definitions into Kafka Streams topologies. Start with `io.axual.ksml.definition.parser.TopologyDefinitionParser` and `io.axual.ksml.TopologyGenerator` to understand the flow.
2. **Function Execution**: Python functions in YAML are executed using GraalVM's polyglot capabilities. See `io.axual.ksml.python` package for implementation.
3. **Data Type System**: Extensible data type system with support for multiple formats. Each data format has its own module implementing common interfaces from `ksml-data`.
4. **Operation Types**: Stream processing operations (filter, map, join, aggregate, etc.) are defined in `io.axual.ksml.operation` with corresponding definitions in `io.axual.ksml.definition`.

### KSML syntax

The syntax is defined in `docs/ksml-language-spec.json` as a JSON Schema. A syntax change needs an update of that schema. When you are not sure about the syntax, look at working definitions:

- `ksml/src/test/resources/docs-examples`
- `ksml-integration-tests/src/test/resources/docs-examples`
- `examples/`

## Tests

- Use the test annotations `@KSMLTest` and `@KSMLTopic` with `@ExtendWith(KSMLTestExtension.class)` (package `io.axual.ksml.testutil`).
- Use Awaitility to wait in tests, not custom wait loops or `Thread.sleep`.
- `@KSMLTest` tests are skipped, not failed, when the JVM is not GraalVM, and the build stays green. Before you say that tests pass, check that the Skipped count is not high. If it is, set `JAVA_HOME` to a GraalVM JDK.
- New functionality needs unit tests, and integration tests where it touches Kafka or a schema registry.
- A user-facing feature also needs a docs update in `docs/`.

### Parity tests

The parity tests compare KSML with plain Kafka Streams. See `developerdocs/parity-tests.md`.

- When fixing a finding (Fxx), run `mvn test -pl ksml -am -Dtest='*ParityTest' -Dsurefire.failIfNoSpecifiedTests=false`.
- If a case says "known difference Fxx is gone", check the report to confirm both sides write records, then remove the marker in the same PR.
- If a case says "known difference Fxx changed", read the report. If the change is right, update the number in the marker, and the text if the reason changed.
- A new or changed operation needs a parity case.
- After changing `ParityCase` or `DockerParity`, run one case in Docker mode, because test driver mode cannot show class path problems of the reference app. Build the image with `./build-local-docker.sh`, delete `ksml/target/parity-report-docker/`, and run the case with `-Dparity.mode=docker`. Docker mode never fails a test, so read `ksml/target/parity-report-docker/summary.tsv` and look for `DIFF`, `HARNESS-ERROR` or `MARKER-GONE`. Stop the brokers afterwards (see `developerdocs/parity-tests.md`).

## Docs examples

### How to write them

1. Give each processor example a matching producer example, so people can run both. The topic names in both definitions must match.
2. Explain each processor example in the docs in a few sentences: what it does and why it is useful.
3. The docs show KSML syntax, so keep the Python code short and simple.
4. Put the definitions in `ksml/src/test/resources/docs-examples`, with a `@KSMLTest` test in `io.axual.ksml.docs.examples`. Use `ksml-integration-tests/src/test/resources/docs-examples` with an `*IT` test only when the example needs real Kafka or a schema registry. Add the definitions to the page with the `include` syntax, as `docs/tutorials/beginner/data-formats.md` does.
5. Use `valueType: json` when the format does not matter, so messages are easy to read in the Kowl UI. When the example is about a format (Avro, XML, CSV, Protobuf and so on), use that format, and use `convertValue` to convert between formats (for example Avro to JSON to String).
6. Add a blank line before every Markdown list, otherwise it does not render as a list.
7. Write bullet points without a leading keyword, as short and natural sentences.

### How to test them with Docker Compose

Use the setup in `docs/local-docker-compose-setup-with-sr` (Kafka, Apicurio schema registry, KSML and Kowl). The docs include files from this folder, so do not leave test changes in it.

1. Check that the `kafka-setup` service in `docker-compose.yml` creates every topic the definitions use. Topic auto-create is off, so add missing topics for the test.
2. Put your definitions in `docs/local-docker-compose-setup-with-sr/examples/producer.yaml` and `processor.yaml`.
3. Check that the `definitions` section of `docs/local-docker-compose-setup-with-sr/examples/ksml-runner.yaml` points to both files.
4. Start from a clean state and follow the KSML log:

   ```bash
   cd docs/local-docker-compose-setup-with-sr
   docker compose down --volumes && docker compose up -d && docker compose logs ksml -f
   ```

5. The test passes when the log has no errors and the log messages are what you expect. If there are errors, fix the definitions (see the working examples above) and run again.
6. The `ksml` service uses a released image. To test a code change, build a local image (see Commands) and put a `docker-compose.override.yml` that sets this image for the `ksml` service in the same folder. Docker Compose reads that file by itself.
7. If you are sure that KSML itself has a bug, write a bug report with the producer and processor definitions, the steps to reproduce, and why you are sure it is a bug. Do not commit the report.
8. When you are done, undo your test changes in this folder (the topics, the definitions) and delete `docker-compose.override.yml`. Keep only changes that your task is about.

## Python Proxies
- Python runs with `HostAccess.EXPLICIT`. Every Java proxy class that Python code uses must be listed in `ALLOWED_JAVA_CLASSES` in `io.axual.ksml.python.PythonContext`, and every method Python may call must be annotated with `@HostAccess.Export`. Methods without the annotation stay invisible to Python, which is how proxies hide the parts of the wrapped Kafka object that are not safe to expose.
- GraalPy does not pass Python keyword arguments to Java methods, and Python looks up special methods such as `__enter__` on the type, which a Java object can not offer. When Python code needs either, add a small Python class that calls the exported methods by position. These classes live in the `ksml` Python module (`ksml/src/main/resources/io/axual/ksml/python/ksml.py`), which KSML loads into every Python context; name the class in the proxy's `InjectedParameter` (see `ProcessorContextProxy.INJECT_CONTEXT`), or wrap the proxy from Java (see `StoreIteratorProxy.toPython`). The classes must hold no state besides the proxy, because one Python context serves all stream threads.
- `log` and `metrics` are Python builtins, set up by the `ksml` module. `log` finds the logger of the running KSML function through a context variable that each generated `_caller` sets and restores, so it works in helper functions and modules too.
- Objects are made available to Python functions through the injections map of `UserFunction.call` (`stores`, `context`), not as function parameters. A function only accepts the variables it declares: `stores` when it uses state stores, and the `InjectedParameter`s of its definition. `PythonFunction` imports an injected parameter's Python class from the `ksml` module and wraps the injected object in it.

## Line Endings
- All files use LF (`\n`) line endings, without exception. Write new files with LF only, and never introduce CRLF.
- If a CRLF file shows up anyway (`git ls-files --eol | grep -E 'i/crlf|w/crlf|mixed'` lists it), convert it to LF in a separate commit, never mixed with functional changes, so the functional diff stays readable.
- After editing files, check with `git diff --stat` that only the edited lines changed; a diff that rewrites a whole file usually means its line endings changed.

## SonarCloud

The project is analysed by SonarCloud. The `sonar-issues` skill (`.claude/skills/sonar-issues`) reads the issues and the quality gate of a pull request or branch from the SonarCloud API. It needs your own SonarCloud token in the file `.sonar-token` in the repository root, which git ignores.
