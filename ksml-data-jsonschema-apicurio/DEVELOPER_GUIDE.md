# KSML Data JSON Schema Apicurio (ksml-data-jsonschema-apicurio) — Developer Guide

Audience: maintainers of ksml/ksml-data-jsonschema-apicurio; structured for AI‑assisted changes.

Version/date: 2025-09-05

How to use this document
- Skim the Overview and Relationships to understand the module’s scope.
- See Class reference for integration points.
- For core concepts (DataObject, DataType, Notation, VendorNotation), see ksml-data/DEVELOPER_GUIDE.md and ksml-data-jsonschema/DEVELOPER_GUIDE.md.

Overview (what this module provides)
- Vendor-specific wiring for JSON Schema using Apicurio Registry.
- Supplies a VendorNotationProvider that creates JsonSchemaNotation backed by an Apicurio serde supplier and a JsonSchemaDataObjectMapper.
- Provides a JsonSchema Serde supplier that wraps Apicurio’s JsonSchema serializer/deserializer and injects sensible default configs.

Module relationships
- Depends on ksml-data for SPI (Notation, VendorNotation, NotationContext, VendorNotationContext) and DataObject model.
- Depends on ksml-data-jsonschema for shared JSON Schema notation logic (JsonSchemaNotation) and JsonSchemaDataObjectMapper.
- Uses Apicurio Registry client and serde classes for runtime serializers/deserializers.

Key behavior
- Default notation name: "jsonschema"; vendor name: "apicurio".
- JsonSchemaDataObjectMapper bridges vendor boundary (native JsonNode/Map/List/primitives) ↔ KSML DataObject.
- ConfigInjectionSerde wrapper ensures default Apicurio configs are applied only when user hasn’t provided them (putIfAbsent).
  - apicurio.registry.artifact-resolver-strategy = TopicIdStrategy
  - apicurio.registry.headers.enabled = false (payload encoding)
  - apicurio.registry.as-confluent = true
  - apicurio.registry.use-id = contentId
  - apicurio.registry.id-handler = Legacy4ByteIdHandler

Class reference
- io.axual.ksml.data.notation.jsonschema.apicurio.ApicurioJsonSchemaNotationProvider
  - Extends VendorNotationProvider.
  - notationName() = "jsonschema"; vendorName() = "apicurio" (via base class fields).
  - createNotation(context): builds VendorNotationContext with ApicurioJsonSchemaSerdeSupplier and JsonSchemaDataObjectMapper, then returns JsonSchemaNotation.

- io.axual.ksml.data.notation.jsonschema.apicurio.ApicurioJsonSchemaSerdeSupplier
  - Implements JsonSchemaSerdeSupplier.
  - Optional constructor arg: io.apicurio.registry.rest.client.RegistryClient (mainly for tests).
  - get(type, isKey): returns a Serde<Object> wrapped in ConfigInjectionSerde that delegates to Apicurio JsonSchema serializer/deserializer.
  - vendorName() = "apicurio".

Testing
- This module contains unit tests for the serde supplier defaults (ApicurioJsonSchemaSerdeSupplierTest).
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-jsonschema-apicurio -am test

Guidelines for contributors
- Keep provider class minimal—its goal is wiring and not behavior.
- Adjust default Apicurio configs in ApicurioJsonSchemaSerdeSupplier only when necessary; preserve putIfAbsent semantics.
- Schema/data mapping logic lives in ksml-data-json and ksml-data-jsonschema modules; avoid duplicating it here.

Troubleshooting
- Missing registry URL: vendor serdes are created lazily by JsonSchemaNotation; failures surface when serdes are first used.
- If user-supplied serde configs are ignored, verify ConfigInjectionSerde.modifyConfigs uses putIfAbsent.

References
- Core SPI: io.axual.ksml.data.notation (ksml-data)
- Vendor notation: io.axual.ksml.data.notation.vendor (ksml-data)
- Shared JSON Schema logic: ksml-data-jsonschema
- Apicurio Registry docs: https://www.apicur.io/