# KSML Data JSON (ksml-data-json) — Developer Guide

Audience: developers maintaining or extending ksml/ksml-data-json; also structured for AI‑assisted refactoring and code generation.

Version/date: 2025-09-05

How to use this document
- If you’re here to add or change functionality, start with “Overview” and “Extension points,” then jump to the Class reference.
- If you’re onboarding or need a mental model, skim the Overview and Relationships to other modules.
- For background on core DataObjects, DataTypes, Schemas, and Mappers, see ksml-data/DEVELOPER_GUIDE.md in the parent module.

Overview (what this module provides)
- ksml-data-json extends ksml-data with generic JSON schema and data conversion capabilities.
- It centralizes JSON String ↔ KSML mappings so different components can reuse consistent logic.
- Core responsibilities:
  - Parse JSON Schema documents to KSML DataSchema and generate JSON Schema from KSML DataSchema.
  - Map JSON text to KSML DataObjects and back, via a native object graph (Map/List/primitives) boundary.
  - Provide a notation implementation (JsonNotation) wiring serde, converter, and schema parsing for JSON.

Module relationships
- Depends on ksml-data for:
  - DataObject, DataType, DataSchema model and utilities.
  - Notation SPI and BaseNotation wiring.
  - NativeDataObjectMapper for native <-> DataObject mapping.
- Reused by other modules or runtime contexts that want JSON serialization (serde) or JSON Schema parsing.

Key concepts from ksml-data (see ksml-data/DEVELOPER_GUIDE.md)
- DataObjects represent runtime values with attached DataTypes.
- DataSchemas represent structural types (struct, list, map, union, etc.).
- Mappers bridge native runtime representations and KSML’s model.
- Notations bundle serdes, mappers, and schema parsers and can be discovered via ServiceLoader.

JSON specifics in ksml-data-json
- Default type: UnionType(StructType | ListType)
  - JSON top-level may be either object or array; the default union type reflects this.
- File extension: .json — used by JsonNotation for schema/data artifacts.
- String boundary: mapping is intentionally split
  - JsonStringMapper: String ↔ native Java (Map/List/primitives) using Jackson.
  - NativeDataObjectMapper: native ↔ DataObject according to expected DataType (from ksml-data).
  - JsonDataObjectMapper composes the two to provide String ↔ DataObject.
- JSON Schema subset and conventions handled by JsonSchemaMapper/Loader:
  - Supported keywords: type, properties, required, additionalProperties, anyOf, enum, items, $defs, $ref.
  - additionalProperties:
    - true => additional fields allowed with ANY schema.
    - false => no additional fields.
    - object => schema describing additional fields’ type.
  - types: null/boolean/integer/number/string/array/object with mapping to KSML DataSchema primitives and collections.
  - arrays: items describes value schema; defaults to ANY when missing.
  - objects: may be defined inline or via $ref into $defs.
  - unions: anyOf becomes UnionSchema; order preserved.
  - enums: enum array becomes EnumSchema with symbols.
- Pretty printing: JsonStringMapper can emit formatted JSON when constructed with prettyPrint = true. Semantics do not change.

Class reference (primary entry points)
- io.axual.ksml.data.notation.json.JsonNotation
  - Extends BaseNotation.
  - Name: "json"; default DataType: Union(Struct|List); schema/data file extension: .json.
  - Wires converter (JsonDataObjectConverter) and schema parser (JsonSchemaLoader).
  - serde(DataType, isKey): returns JsonSerde for MapType, ListType, StructType, or types assignable to DEFAULT_TYPE.

- io.axual.ksml.data.notation.json.JsonNotationProvider
  - Implements NotationProvider; exposes notationName() = "json" and constructs JsonNotation from a NotationContext.

- io.axual.ksml.data.notation.json.JsonSerde
  - Extends StringSerde; injects JsonDataObjectMapper for String ↔ DataObject at the serde boundary.
  - Validates against expected DataType; integrates with NativeDataObjectMapper from the NotationContext.

- io.axual.ksml.data.notation.json.JsonStringMapper
  - Implements StringMapper<Object>.
  - Uses Jackson to parse/generate JSON String to/from a native object graph (Map/List/primitives).
  - Null-safe; throws DataException on parse/serialization failure; optional pretty printing.

- io.axual.ksml.data.notation.json.JsonDataObjectMapper
  - Implements DataObjectMapper<String>.
  - Composes JsonStringMapper and NativeDataObjectMapper to map JSON String ↔ DataObject.
  - No extra logic beyond delegation (keeps JSON boundary localized).

- io.axual.ksml.data.notation.json.JsonDataObjectConverter
  - Implements Notation.Converter.
  - Two directions:
    - Structured DataObject (DataStruct/DataMap/DataList) -> DataString JSON (serialize).
    - DataString JSON -> structured DataObject when target type is ListType/MapType/StructType/UnionType (parse).
  - Returns null when conversion does not apply (e.g., primitive to struct).

- io.axual.ksml.data.notation.json.JsonSchemaLoader
  - Implements Notation.SchemaParser; thin wrapper delegating to JsonSchemaMapper.

- io.axual.ksml.data.notation.json.JsonSchemaMapper
  - Implements DataSchemaMapper<String>.
  - Parses JSON Schema String to KSML DataSchema via DataObject view; supports required keywords listed above.
  - Generates JSON Schema (as compact String) from StructSchema; collects referenced structures into $defs and emits $ref.

Extension points and guidance
- Adjusting JSON Schema conversion rules:
  - Modify JsonSchemaMapper. Ensure tests cover:
    - required fields vs. optional
    - additionalProperties true/false/object
    - anyOf/enum handling
    - items for arrays; $defs/$ref for nested structs
- Adjusting data conversion rules:
  - JsonStringMapper for String<->native format details (pretty print, parsing behavior).
  - NativeDataObjectMapper (in ksml-data) governs native<->DataObject conversions; adjust there when needed.
  - JsonDataObjectConverter only orchestrates when to parse/serialize; keep it minimal.
- Keep JsonNotation minimal—its purpose is wiring and defaults. Behavior should live in mappers/loader.

Testing
- This module contains unit tests that document expected behavior and style (AssertJ chained assertions and SoftAssertions):
  - JsonSchemaMapperTest — JSON Schema ↔ DataSchema
  - JsonSchemaLoaderTest — Loader delegates and error propagation
  - JsonDataObjectMapperTest — JSON String ↔ DataObject round-trips
  - JsonStringMapperTest — String ↔ native graph, pretty printing
  - JsonDataObjectConverterTest — Converter directions and non-applicable cases
  - JsonNotationTest — Notation defaults, serde selection, wiring
  - JsonNotationProviderTest — Provider name and factory behavior
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-json -am test
  - Note: In some environments, you must build/install ksml-data first: mvn -pl ksml-data -am install

Build and compatibility notes
- Adding or modifying this documentation does not affect Java compilation.
- Keep JSON mapping decisions backward compatible unless a breaking change is intended.
- The module leverages Jackson; ensure version alignment with the parent build to avoid shading conflicts.

Troubleshooting tips
- DataString input fails to parse: JsonStringMapper throws DataException with a concise message; verify JSON validity.
- Wrong serde selected: JsonNotation only supports MapType, ListType, StructType, or unions assignable to DEFAULT_TYPE.
- Additional properties behavior:
  - When StructSchema.areAdditionalFieldsAllowed() is true and additional field schema is ANY, mapper emits additionalProperties: true.
  - When false, emits additionalProperties: false.
  - When set to a specific schema, emits additionalProperties as an object spec.
- Union mapping: anyOf members convert to UnionSchema in order; ensure downstream consumers expect this.

References
- Background and core concepts: ksml-data/DEVELOPER_GUIDE.md
- Notation SPI and base: io.axual.ksml.data.notation.Notation and io.axual.ksml.data.notation.base.BaseNotation (ksml-data)
- JSON implementation entry points (this module): JsonNotation, JsonSerde, JsonDataObjectMapper, JsonStringMapper, JsonSchemaMapper, JsonSchemaLoader, JsonDataObjectConverter
