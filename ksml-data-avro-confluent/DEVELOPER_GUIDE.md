# KSML Data Avro Confluent (ksml-data-avro-confluent) — Developer Guide

Audience: developers maintaining or extending ksml/ksml-data-avro-confluent; also structured for AI‑assisted refactoring and code generation.

Version/date: 2025-08-19

How to use this document
- If you’re wiring or modifying Confluent Avro integration, start with the Module overview and Key classes, then see Wiring and SPI.
- For broader context on data/object mapping and Avro schema handling, read the developer guides in ksml-data and ksml-data-avro.
- For testing and local development, see the Testing and Troubleshooting sections below.

Overview (what this module provides)
- Vendor-specific wiring for Avro notation using Confluent’s Schema Registry serdes.
- Provides a concrete Avro serde supplier built on:
  - io.confluent.kafka.serializers.KafkaAvroSerializer
  - io.confluent.kafka.serializers.KafkaAvroDeserializer
- Exposes a NotationProvider so KSML can discover and use an Avro notation variant named "confluent_avro".
- Reuses all Avro schema and data mapping logic from ksml-data-avro (no duplication here).

Module relationships
- Depends on:
  - ksml-data for Notation SPI and DataObject/DataType/Schemas abstractions.
  - ksml-data-avro for AvroNotation, AvroSchemaParser/Mapper, and AvroDataObjectMapper.
  - Confluent artifacts for Avro serdes and Schema Registry client.
- Provides:
  - A vendor-specific Avro serde supplier (ConfluentAvroSerdeSupplier).
  - A NotationProvider that yields AvroNotation wired for the Confluent vendor (ConfluentAvroNotationProvider).

Where the main logic lives (and does NOT live)
- DOES NOT implement Avro schema parsing/mapping or object conversion. That logic is centralized in ksml-data-avro.
- DOES provide the Kafka serialization/deserialization bridge using Confluent’s serdes and Schema Registry client.

Key classes (primary entry points)
- io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider
  - Implements NotationProvider for the Avro notation, vendor "confluent".
  - notationName(): "avro"
  - vendorName(): "confluent"
  - createNotation(context): returns an AvroNotation instance named "confluent_avro"
    - Wires VendorNotationContext with:
      - ConfluentAvroSerdeSupplier (optionally with a provided SchemaRegistryClient)
      - AvroDataObjectMapper from ksml-data-avro
    - Ensures filename extension is ".avsc" and default type matches AvroNotation.DEFAULT_TYPE.

- io.axual.ksml.data.notation.avro.confluent.ConfluentAvroSerdeSupplier
  - Implements AvroSerdeSupplier.
  - vendorName(): "confluent"
  - get(DataType type, boolean isKey): returns a Serdes.WrapperSerde<Object> backed by
    - KafkaAvroSerializer
    - KafkaAvroDeserializer
  - SchemaRegistryClient handling:
    - If a client instance is provided at construction, it is passed to both serializer and deserializer constructors.
    - If not provided, default no-arg constructors are used and runtime configuration must supply registry settings (e.g. schema.registry.url).

Wiring and SPI (how everything is discovered)
- Service discovery:
  - ConfluentAvroNotationProvider is registered (via resources/META-INF/services in the project root, inherited from module setup) so KSML can auto-discover it using ServiceLoader.
- Naming conventions:
  - Base notation name: "avro"
  - Vendor: "confluent"
  - Effective notation name: "confluent_avro" (used for configuration namespacing and lookups in KSML).
- Responsibilities split:
  - ksml-data-avro: provides AvroNotation, AvroSchemaParser/Mapper, AvroDataObjectMapper.
  - ksml-data-avro-confluent: supplies the vendor-specific serdes and provider wiring.

Configuration and usage notes
- Runtime configuration for Confluent serdes happens via standard Kafka client properties:
  - schema.registry.url (required unless using a provided SchemaRegistryClient)
  - basic.auth.credentials.source, basic.auth.user.info, sasl/ssl props, etc., as applicable to your environment.
- Key vs value serde behavior:
  - ConfluentAvroSerdeSupplier.get(type, isKey) returns the same underlying serializer/deserializer classes regardless of isKey.
  - Use standard Kafka client configs to differentiate key/value settings if needed (e.g., subject naming strategies).
- Supplying a SchemaRegistryClient:
  - Useful for tests (mocked clients) or applications that manage a shared client instance.
  - Pass it to ConfluentAvroNotationProvider (if supported) or directly instantiate ConfluentAvroSerdeSupplier with a client.

Development guidelines
- Prefer to modify Avro mapping in ksml-data-avro rather than here.
- Keep this module focused on:
  - Vendor serde provision
  - Notation provider wiring
  - Minimal Confluent-specific conventions
- Maintain backward compatibility of serde behavior when possible. Changes to serializer/deserializer construction should be well justified and documented here.

Testing
- Existing unit tests illustrate expected behavior:
  - ConfluentAvroNotationProviderTest
    - Verifies provider metadata, AvroNotation naming/extension/default type, and wiring of ConfluentAvroSerdeSupplier.
    - Ensures an optionally provided SchemaRegistryClient is propagated to the supplier.
  - ConfluentAvroSerdeSupplierTest
    - Verifies vendorName() == "confluent".
    - Ensures get(...) returns Serdes.WrapperSerde with KafkaAvroSerializer/KafkaAvroDeserializer.
    - Confirms that when a SchemaRegistryClient is supplied to the supplier, it is retained/exposed by the supplier.
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-avro-confluent -am test
  - Specific test class: mvn -pl ksml-data-avro-confluent -am -Dtest=ConfluentAvroSerdeSupplierTest test
- Notes on environment:
  - Tests do not require a live Schema Registry; use mocks for SchemaRegistryClient.
  - Ensure Confluent dependencies resolve in your local Maven settings (Confluent repo if needed via parent POM).

Build and compatibility notes
- This module inherits build and test plugin configuration from the parent POM.
- JUnit Jupiter, AssertJ, Mockito are used for tests.
- Changing documentation does not affect compilation.
- Keep Confluent artifact versions aligned with the parent BOM and other vendor modules to avoid classpath conflicts.

Troubleshooting tips
- Missing Schema Registry URL:
  - If you construct serdes without a provided SchemaRegistryClient, ensure schema.registry.url and related properties are present in your Kafka client config.
- Subject naming / compatibility issues:
  - Configure Confluent subject naming strategies via serializer configs as needed.
- Classpath conflicts:
  - Verify only one Avro implementation is on the classpath and that Confluent serdes match the Avro version pulled in via ksml-data-avro.
- ServiceLoader discovery:
  - If the Confluent Avro notation is not discovered, ensure the service registration file exists and that the module is on the runtime classpath.

Examples (pseudo-code)
- Creating a serde via supplier (value serde):
  - var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"));
  - var valueSerde = supplier.get(DataType.UNKNOWN, false);
- With a mocked/provided SchemaRegistryClient:
  - SchemaRegistryClient client = ...;
  - var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"), client);
  - var keySerde = supplier.get(DataType.UNKNOWN, true);
- Creating a notation via provider:
  - var provider = new ConfluentAvroNotationProvider();
  - var notation = provider.createNotation(new NotationContext("avro", "confluent"));
  - assert notation.name().equals("confluent_avro");

References
- Core concepts and Notation/Vendor SPI: ksml-data/DEVELOPER_GUIDE.md
- AvroNotation behavior, schema parser/mapper, and data object mapping: ksml-data-avro/DEVELOPER_GUIDE.md
- Confluent serdes documentation: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html
