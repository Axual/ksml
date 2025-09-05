# KSML Data JSON Schema (ksml-data-jsonschema) — Developer Guide

Audience: developers maintaining or extending ksml/ksml-data-jsonschema; also structured for AI‑assisted refactoring and code generation.

Version/date: 2025-09-05

How to use this document
- If you’re here to add or change functionality, start with “Overview” and “Extension points,” then jump to the Class reference.
- If you’re onboarding or need a mental model, skim the Overview and Relationships to other modules.
- For background on core DataObjects, DataTypes, Schemas, and Mappers, see ksml-data/DEVELOPER_GUIDE.md in the parent module.

Overview (what this module provides)
- ksml-data-jsonschema provides vendor-backed JSON Schema notation wiring for KSML.
- It centralizes the integration of vendor serdes (e.g., Confluent, Apicurio) with a consistent KSML data model while reusing the generic JSON data/schema mapping implemented in ksml-data-json.
- Core responsibilities:
  - Provide a Notation implementation (JsonSchemaNotation) that delegates schema parsing and data conversion to the shared JSON components and obtains serdes from a vendor-specific supplier.
  - Offer a mapper (JsonSchemaDataObjectMapper) for vendor serdes that expose Jackson JsonNode at their boundary, bridging to KSML DataObjects.
  - Define a marker interface (JsonSchemaSerdeSupplier) so vendor modules can contribute concrete serdes.

Module relationships
- Depends on ksml-data for:
  - DataObject, DataType, DataSchema model and utilities.
  - Notation SPI and VendorNotation wiring (VendorNotation, VendorNotationContext, VendorSerdeSupplier).
- Reuses mapping/parsing from ksml-data-json:
  - JsonDataObjectConverter (JSON String ↔ DataObject orchestration)
  - JsonSchemaLoader (JSON Schema String → DataSchema via JsonSchemaMapper)
- Reused by vendor-specific modules to supply serdes:
  - ksml-data-jsonschema-confluent
  - ksml-data-jsonschema-apicurio
- Those vendor modules implement JsonSchemaSerdeSupplier and provide VendorNotationContext wiring; they do not reimplement schema/data mapping logic.

Key concepts from ksml-data (see ksml-data/DEVELOPER_GUIDE.md)
- DataObjects represent runtime values with attached DataTypes.
- DataSchemas represent structural types (struct, list, map, union, etc.).
- Mappers bridge native runtime representations and KSML’s model.
- Notations bundle serdes, mappers, and schema parsers and are discovered via ServiceLoader.
- VendorNotation extends BaseNotation to work with vendor-provided serde suppliers.

JSON Schema specifics in this module
- Default type: UnionType(StructType | ListType)
  - JSON top-level may be either object or array; JsonSchemaNotation’s default type reflects this.
- File extension: .json (JSON Schema documents are JSON files)
- Notation wiring:
  - Converter: JsonDataObjectConverter (from ksml-data-json). Handles DataString JSON ↔ structured DataObject conversions when requested by the notation.
  - Schema parser: JsonSchemaLoader (from ksml-data-json). Parses JSON Schema string to DataSchema using JsonSchemaMapper.
  - Serdes: created via VendorNotation using a vendor-supplied SerdeSupplier; type support is constrained by the notation’s default type (Struct|List union).
- Vendor-backed serdes:
  - The vendor modules own the actual Kafka Serializer/Deserializer creation and configuration.
  - This module wraps them into KSML’s DataObjectSerde so KSML pipelines operate over DataObjects consistently.

Class reference (primary entry points)
- io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation
  - Extends io.axual.ksml.data.notation.vendor.VendorNotation.
  - Name: "jsonschema"; default DataType: Union(Struct|List); schema file extension: .json.
  - Wires converter (JsonDataObjectConverter) and schema parser (JsonSchemaLoader).
  - serde(DataType, isKey): obtains a vendor serde from VendorSerdeSupplier and wraps it into DataObjectSerde when the requested type is assignable from the default union type.

- io.axual.ksml.data.notation.jsonschema.JsonSchemaDataObjectMapper
  - Implements DataObjectMapper<Object>.
  - Bridges vendor-facing Jackson JsonNode values ↔ KSML DataObject by converting via native (Map/List/primitives) using NativeDataObjectMapper.
  - Accepts only JsonNode (or null) in toDataObject; throws DataException otherwise. From DataObject returns a JsonNode.

- io.axual.ksml.data.notation.jsonschema.JsonSchemaSerdeSupplier
  - Marker interface extending io.axual.ksml.data.notation.vendor.VendorSerdeSupplier.
  - Implemented by vendor modules (e.g., confluent/apicurio) to supply concrete Kafka Serde instances for JSON Schema.
  - notationName() defaults to "jsonschema" to bind suppliers to this notation.

Extension points and how to reuse in vendor modules
- Typical flow for a vendor module (e.g., ksml-data-jsonschema-confluent):
  1) Implement JsonSchemaSerdeSupplier to return vendor-specific Serializer/Deserializer for keys and values.
  2) Provide a VendorNotationContext that supplies:
     - vendorName (e.g., "confluent" or "apicurio")
     - a JsonSchemaDataObjectMapper (or another appropriate DataObjectMapper) for serde boundary conversions
     - serde configuration properties (URL, auth, etc.)
  3) Instantiate JsonSchemaNotation with the VendorNotationContext (wired via ServiceLoader in the vendor module) so KSML can discover the notation.
  4) Reuse JsonSchemaLoader and JsonDataObjectConverter for schema parsing and JSON conversions; no need to duplicate logic.

Guidelines for contributors
- When changing schema parsing behavior, adjust the shared JsonSchemaLoader/JsonSchemaMapper in ksml-data-json (not here) and ensure tests remain valid in both modules.
- When changing vendor serde integration behavior, adapt JsonSchemaNotation and validate VendorNotation tests still pass.
- Keep JsonSchemaNotation minimal—its purpose is wiring and defaults. Vendor-specific behavior lives in the corresponding vendor modules and contexts.

Testing
- This module contains unit tests that document expected behavior and follow AssertJ chained assertions and SoftAssertions:
  - JsonSchemaNotationTest — defaults, name/extension, converter/parser types, serde selection via VendorNotation
  - JsonSchemaSerdeSupplierTest — supplier notationName and dummy get()
  - JsonSchemaDataObjectMapperTest — JsonNode ↔ DataObject round-trips, null handling, error on unsupported inputs
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-jsonschema -am test
  - Note: In some environments, build/install ksml-data and ksml-data-json first if dependencies are missing:
    - mvn -pl ksml-data -am install
    - mvn -pl ksml-data-json -am install

Build and compatibility notes
- Adding or modifying this documentation does not affect Java compilation.
- Ensure any change to mapping rules remains backward compatible for downstream vendor modules unless a major version change is intended.
- This module does not include its own JSON parsing logic; it reuses ksml-data-json for JsonSchemaLoader and JsonDataObjectConverter. Keep versions in sync.

Troubleshooting tips
- Missing vendor configs (e.g., schema registry URL) lead to failures only when serdes are actually created/used. VendorNotation creates serdes lazily.
- If a serde is requested for an unsupported type, VendorNotation will fail with a clear message (no serde available for data type). Confirm your requested DataType is assignable from the notation’s default union (Struct|List).
- If a vendor serde exposes native JsonNode and mapping seems incorrect, inspect JsonSchemaDataObjectMapper and JsonNodeUtil conversions in ksml-data.

References
- Background and core concepts: ksml-data/DEVELOPER_GUIDE.md
- Notation SPI and bases: io.axual.ksml.data.notation.Notation, io.axual.ksml.data.notation.base.BaseNotation, io.axual.ksml.data.notation.vendor.VendorNotation (ksml-data)
- Shared JSON components (this module depends on): ksml-data-json — JsonSchemaLoader, JsonDataObjectConverter, JsonSchemaMapper
- Example vendor integrations:
  - ksml-data-jsonschema-confluent
  - ksml-data-jsonschema-apicurio
