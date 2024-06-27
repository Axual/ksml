# Release Notes

## Releases

* [Release Notes](#release-notes)
    * [Releases](#releases)
        * [1.0.0 (2024-06-28)](#100-2024-06-28)
        * [0.8.0 (2024-03-08)](#080-2024-03-08)
        * [0.9.1 (2024-06-21)](#091-2024-06-21)
        * [0.9.0 (2024-06-05)](#090-2024-06-05)
        * [0.2.2 (2024-01-30)](#022-2024-01-30)
        * [0.2.1 (2023-12-20)](#021-2023-12-20)
        * [0.2.0 (2023-12-07)](#020-2023-12-07)
        * [0.1.0 (2023-03-15)](#010-2023-03-15)
        * [0.0.4 (2022-12-02)](#004-2022-12-02)
        * [0.0.3 (2021-07-30)](#003-2021-07-30)
        * [0.0.2 (2021-06-28)](#002-2021-06-28)
        * [0.0.1 (2021-04-30)](#001-2021-04-30)

### 1.0.0 (2024-06-28)

* Reworked parsing logic, allowing alternatives for operations and other definitions to co-exist in the KSML language
  specification. This allows for better syntax checking in IDEs.
* Lots of small fixes and completion modifications.

### 0.9.1 (2024-06-21)

* Fix failing test in GitHub Actions during release
* Unified build workflows

### 0.9.0 (2024-06-05)

* Collectable metrics
* New topology test suite
* Python context hardening
* Improved handling of Kafka tombstones
* Added flexibility to producers (single shot, n-shot, or user condition-based)
* JSON Logging support
* Bumped GraalVM to 23.1.2
* Bumped several dependency versions
* Several fixes and security updates

### 0.8.0 (2024-03-08)

* Reworked all parsing logic, to allow for exporting the JSON schema of the KSML specification:
    * docs/specification.md is now derived from internal parser logic, guaranteeing consistency and completeness.
    * examples/ksml.json contains the JSON schema, which can be loaded into IDEs for syntax validation and completion.
* Improved schema handling:
    * Better compatibility checking between schema fields.
* Improved support for state stores:
    * Update to state store typing and handling.
    * Manual state stores can be defined and referenced in pipelines.
    * Manual state stores are also available in Python functions.
    * State stores can be used 'side-effect-free' (e.g. no AVRO schema registration)
* Python function improvements:
    * Automatic variable assignment for state stores.
    * Every Python function can use a Java Logger, integrating Python output with KSML log output.
    * Type inference in situations where parameters or result types can be derived from the context.
* Lots of small language updates:
    * Improve readability for store types, filter operations and windowing operations
    * Introduction of the "as" operation, which allows for pipeline referencing and chaining.
* Better data type handling:
    * Separation of data types and KSML core, allowing for easier addition of new data types in the future.
    * Automatic conversion of data types, removing common pipeline failure scenarios.
    * New implementation for CSV handling.
* Merged the different runners into a single runner.
    * KSML definitions can now include both producers (data generators) and pipelines (Kafka Streams topologies).
    * Removal of Kafka and Axual backend distinctions.
* Configuration file updates, allowing for running multiple definitions in a single runner (each in its own namespace).
* Examples updated to reflect the latest definition format.
* Documentation updated.

### 0.2.2 (2024-01-30)

**Changes:**

* Fix KSML java process not stopping on exception
* Fix stream-stream join validation and align other checks
* Bump logback to 1.4.12
* Fix to enable Streams optimisations to be applied to topology
* Fix resolving admin client issues causing warning messages

### 0.2.1 (2023-12-20)

**Changes:**

* Fixed an issue with AVRO and field validations

### 0.2.0 (2023-12-07)

**Changes:**

* Optimized Docker build
* Merged KSML Runners into one module, optimize Docker builds and workflow
* KSML documentation updates
* Docker image, GraalPy venv, install and GU commands fail
* Update GitHub Actions
* Small robustness improvements
* Issue #72 - Fix build failures when trying to use venv and install python packages
* Manual state store support, Kafka client cleanups and configuration changes
* Update and clean up dependencies
* Update documentation to use new runner configurations
* Update to GraalVM for JDK 21 Community

### 0.1.0 (2023-03-15)

**Changes:**

* Added XML/SOAP support
* Added data generator
* Added Automatic Type Conversion
* Added Schema Support for XML, Avro, JSON, Schema
* Added Basic Error Handling

### 0.0.4 (2022-12-02)

**Changes:**

* Update to kafka 3.2.3
* Update to Java 17
* Support multiple architectures in KSML, linux/amd64 and linux/arm64
* Refactored internal typing system, plus some fixes to store operations
* Introduce queryable state stores
* Add better handling of NULL keys and values from Kafka
* Implement schema support
* Added Docker multistage build
* Bug fix for windowed objects
* Store improvements
* Support Liberica NIK
* Switch from Travis CI to GitHub workflow
* Build snapshot Docker image on pull request merged

### 0.0.3 (2021-07-30)

**Changes:**

* Support for Python 3 through GraalVM
* improved data structuring
* bug fixes

### 0.0.2 (2021-06-28)

**Changes:**

* Added JSON support, Named topology and name store supported

### 0.0.1 (2021-04-30)

**Changes:**

* First alpha release 
