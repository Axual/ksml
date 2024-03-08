[<< Back to index](index.md)

# Release Notes

## Releases

<!-- TOC -->
* [Release Notes](#release-notes)
  * [Releases](#releases)
    * [0.2.2 (2024-01-30)](#022-2024-01-30)
    * [0.2.1 (2023-12-20)](#021-2023-12-20)
    * [0.2.0 (2023-12-07)](#020-2023-12-07)
    * [0.1.0 (2023-03-15)](#010-2023-03-15)
    * [0.0.4 (2022-12-02)](#004-2022-12-02)
    * [0.0.3 (2021-07-30)](#003-2021-07-30)
    * [0.0.2 (2021-06-28)](#002-2021-06-28)
    * [0.0.1 (2021-04-30)](#001-2021-04-30)
<!-- TOC -->

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
* Added datagenerator
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
* Switch from Travis CI to Github workflow
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
