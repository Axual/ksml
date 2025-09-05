# KSML Data JSON Schema Confluent (ksml-data-jsonschema-confluent) — Developer Guide

Audience: maintainers of ksml/ksml-data-jsonschema-confluent; structured for AI‑assisted edits.

Version/date: 2025-09-05

How to use this document
- Skim Overview and Relationships to understand the module’s purpose.
- Use Class reference to find extension points.
- For fundamentals (DataObject, DataType, Notation, VendorNotation), see ksml-data/DEVELOPER_GUIDE.md and ksml-data-jsonschema/DEVELOPER_GUIDE.md.

Overview (what this module provides)
- Vendor-specific wiring for JSON Schema using Confluent Schema Registry.
- Supplies a VendorNotationProvider that creates JsonSchemaNotation backed by a Confluent serde supplier and a JsonSchemaDataObjectMapper.
- Provides a JsonSchema Serde supplier that wraps Confluent’s JsonSchema serializer/deserializer.

Module relationships
- Depends on ksml-data for SPI (Notation, VendorNotation, NotationContext, VendorNotationContext) and the DataObject model.
- Depends on ksml-data-jsonschema for shared JSON Schema notation logic (JsonSchemaNotation) and JsonSchemaDataObjectMapper.
- Uses Confluent Schema Registry client and serializers/deserializers for runtime serdes.

Key behavior
- Default notation name: "jsonschema"; vendor name: "confluent".
- JsonSchemaDataObjectMapper bridges vendor boundary (native JsonNode/Map/List/primitives) ↔ KSML DataObject.
- SerdeSupplier composes Confluent KafkaJsonSchemaSerializer/Deserializer; no extra config injection here (configuration is passed through from NotationContext.serdeConfigs()).

Class reference
- io.axual.ksml.data.notation.jsonschema.confluent.ConfluentJsonSchemaNotationProvider
  - Extends VendorNotationProvider.
  - notationName() = "jsonschema"; vendorName() = "confluent" (via base class fields).
  - createNotation(context): builds VendorNotationContext with ConfluentJsonSchemaSerdeSupplier and JsonSchemaDataObjectMapper, then returns JsonSchemaNotation.

- io.axual.ksml.data.notation.jsonschema.confluent.ConfluentJsonSchemaSerdeSupplier
  - Implements JsonSchemaSerdeSupplier.
  - Optional constructor arg: io.confluent.kafka.schemaregistry.client.SchemaRegistryClient (primarily for tests); when null, default constructors are used.
  - get(type, isKey): returns a Serde<Object> composed of KafkaJsonSchemaSerializer/KafkaJsonSchemaDeserializer.
  - vendorName() = "confluent".

Testing
- This module contains minimal tests verifying provider metadata/wiring and serde supplier basics:
  - ConfluentJsonSchemaNotationProviderTest
  - ConfluentJsonSchemaSerdeSupplierTest
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-jsonschema-confluent -am test
  - Note: Ensure ksml-data and ksml-data-jsonschema are built/installed first if needed.

Guidelines for contributors
- Keep provider class minimal—goal is wiring and not behavior.
- Schema/data conversion logic lives in ksml-data-json and ksml-data-jsonschema; avoid duplicating here.
- If you add custom configuration behavior, prefer putIfAbsent style to preserve user overrides and consider creating a ConfigInjectionSerde (see Apicurio module for an example).

Troubleshooting
- Missing Schema Registry URL or auth configuration will surface when serdes are instantiated/used by JsonSchemaNotation.
- If Notation discovery fails, verify ServiceLoader configuration (if present) and that notation/vendor names align with expectations.

References
- Core SPI: io.axual.ksml.data.notation (ksml-data)
- Vendor notation base: io.axual.ksml.data.notation.vendor (ksml-data)
- Shared JSON Schema logic: ksml-data-jsonschema
- Confluent Schema Registry docs: https://docs.confluent.io
