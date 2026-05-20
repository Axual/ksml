# test-runner-packaging Specification

## Purpose
TBD - created by archiving change ksml-test-runner. Update Purpose after archive.
## Requirements
### Requirement: Separate Maven module
The test runner SHALL be a separate Maven module named `ksml-test-runner` in the project root, with its own `pom.xml` declaring dependencies on `ksml` (core), `kafka-streams-test-utils` (compile scope), and Picocli.

#### Scenario: Module builds independently
- **WHEN** `mvn clean package` is run on the `ksml-test-runner` module
- **THEN** it SHALL produce a runnable JAR with all dependencies available

### Requirement: Separate JAR artifact
The `ksml-test-runner` module SHALL produce a JAR artifact (e.g., `ksml-test-runner-<version>.jar`) with a manifest `Main-Class` entry pointing to the test runner's main class. Dependencies SHALL be copied to a `libs/` directory, following the same packaging pattern as `ksml-runner`.

#### Scenario: JAR is executable
- **WHEN** `java -jar ksml-test-runner-<version>.jar test.yaml` is invoked with dependencies on the classpath
- **THEN** the test runner main class SHALL execute

### Requirement: Docker image inclusion
The KSML Docker image SHALL include the test runner JAR alongside the existing runner JAR. The test runner SHALL be invocable by overriding the Docker entrypoint or command.

#### Scenario: Run test runner in Docker
- **WHEN** the Docker image is started with `java -jar /opt/ksml/ksml-test.jar /tests/my-test.yaml`
- **THEN** the test runner SHALL execute inside the container using the container's GraalVM runtime

#### Scenario: Default entrypoint unchanged
- **WHEN** the Docker image is started without overriding the entrypoint
- **THEN** it SHALL still run the existing KSML runner (backwards compatible)

### Requirement: Parent POM integration
The `ksml-test-runner` module SHALL be listed in the root `pom.xml` modules list so it is built as part of the standard `mvn clean package` lifecycle.

#### Scenario: Full project build
- **WHEN** `mvn clean package` is run from the project root
- **THEN** the `ksml-test-runner` module SHALL be built along with all other modules

