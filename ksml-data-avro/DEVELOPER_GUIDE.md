# KSML Data Avro (ksml-data-avro) — Developer Guide

Audience: developers maintaining or extending ksml/ksml-data-avro; also structured for AI‑assisted refactoring and code generation.

Version/date: 2025-08-18

How to use this document
- If you’re here to add or change functionality, start with the “Module overview” and “Extension points” sections, then jump to the Class reference.
- If you’re onboarding or need a mental model, skim the Overview and Relationships to other modules.
- For background on core DataObjects, DataTypes, Schemas, and Mappers, see ksml-data/DEVELOPER_GUIDE.md in the parent module.

Overview (what this module provides)
- ksml-data-avro extends ksml-data with generic Avro schema and data conversion capabilities.
- It centralizes Avro → KSML and KSML → Avro mappings so different Avro vendors/serde implementations can define their own notation wiring and reuse this logic.
- Core responsibilities:
  - Parse Avro .avsc schemas to KSML DataSchema.
  - Map Avro runtime values (GenericRecord, arrays, maps, primitives, bytes) to KSML DataObjects and back.
  - Provide a notation implementation (AvroNotation) that integrates with vendor-backed serdes via VendorNotation.

Module relationships
- Depends on ksml-data for:
  - DataObject, DataType, DataSchema model and utilities.
  - Notation SPI and VendorNotation base wiring.
- Reused by vendor-specific modules to supply serdes:
  - ksml-data-avro-confluent
  - ksml-data-avro-apicurio
- Those vendor modules provide concrete AvroSerdeSupplier implementations and VendorNotationContext wiring; they do not reimplement schema/data mapping logic.

Key concepts from ksml-data (see ksml-data/DEVELOPER_GUIDE.md)
- DataObjects represent runtime values with attached DataTypes.
- DataSchemas represent structural types (struct, list, map, union, etc.).
- Mappers bridge native runtime representations and KSML’s model.
- Notations bundle serdes, mappers, and schema parsers and are discovered via ServiceLoader.

Avro specifics in ksml-data-avro
- Default type: StructType — AvroNotation assumes StructType at top level for Avro.
- File extension: .avsc — used by AvroNotation to detect schema files.
- Unions and optionality: handled in AvroSchemaMapper and AvroDataObjectMapper; nullable unions are recognized and mapped to required/optional DataSchemas and null runtime values.
- Bytes: Avro ByteBuffer <-> KSML DataBytes.
- Collections: Avro arrays and maps map to DataList and DataMap with element/value types resolved from Avro Schemas.

Class reference (primary entry points)
- io.axual.ksml.data.notation.avro.AvroNotation
  - Extends io.axual.ksml.data.notation.vendor.VendorNotation.
  - Name: "avro"; default DataType: StructType; schema file extension: .avsc.
  - Delegates parsing to AvroSchemaParser.
  - Works with a VendorNotationContext to obtain serdes and native mappers from vendor-specific modules.

- io.axual.ksml.data.notation.avro.AvroSchemaParser
  - Implements Notation.SchemaParser.
  - Parses Avro schema JSON (.avsc) using org.apache.avro.Schema.Parser.
  - Converts the parsed Avro Schema to KSML DataSchema via AvroSchemaMapper.
  - Enforces StructSchema at the top level; throws SchemaException otherwise.

- io.axual.ksml.data.notation.avro.AvroSchemaMapper
  - Implements DataSchemaMapper<org.apache.avro.Schema>.
  - Converts Avro Schema <-> KSML DataSchema, including:
    - Structs, fields, order.
    - Unions (nullable and multi-member), required flags, default values.
    - Primitive types (boolean, int/long, float/double, string, bytes, etc.).
    - Arrays and maps with element/value schema conversion.
  - Used by AvroSchemaParser for parsing and by AvroDataObjectMapper when converting by schema.

- io.axual.ksml.data.notation.avro.AvroDataObjectMapper
  - Implements DataObjectMapper<Object>.
  - Maps Avro runtime values to KSML DataObjects:
    - GenericRecord -> DataStruct
    - array -> DataList
    - map -> DataMap
    - ByteBuffer -> DataBytes
    - primitives -> DataPrimitive wrappers (DataString, DataLong, etc.) with numeric/boolean/float handling
  - Maps KSML DataObjects back to Avro-compatible values (e.g., GenericRecord, lists, maps, primitives) using Avro Schemas when available to drive exact conversion.
  - Handles union unwrap, nulls for optional fields, and type inference from Avro Schemas.

- io.axual.ksml.data.notation.avro.AvroSerdeSupplier
  - Marker interface extending io.axual.ksml.data.notation.vendor.VendorSerdeSupplier.
  - Implemented by vendor modules (e.g., confluent/apicurio) to provide concrete Kafka Serde instances for Avro.

Extension points and how to reuse in vendor modules
- Typical flow for a vendor module (e.g., ksml-data-avro-confluent):
  1) Implement AvroSerdeSupplier to return vendor-specific Serializer/Deserializer pairs for keys and values.
  2) Provide a VendorNotationContext that supplies the AvroSerdeSupplier and an AvroDataObjectMapper instance.
  3) Instantiate AvroNotation with the VendorNotationContext (wired via ServiceLoader in the vendor module) so KSML can discover the notation.
  4) Reuse AvroSchemaParser/Mapper and AvroDataObjectMapper for schema and data conversions; no need to duplicate.

Guidelines for contributors
- When changing schema conversion rules, update AvroSchemaMapper and ensure tests in ksml-data-avro cover:
  - Union nullability and required flags
  - Default value conversions
  - Array/map element/value typing
- When changing data mapping rules, update AvroDataObjectMapper and ensure coverage for:
  - GenericRecord <-> DataStruct mapping
  - ByteBuffer/bytes handling
  - Lists/Maps with nested structs and unions
- Keep AvroNotation minimal—its purpose is wiring and defaults. Vendor-specific behavior belongs in the vendor modules and contexts.

Testing
- This module contains unit tests that document expected behavior:
  - AvroSchemaMapperTest
  - AvroSchemaParserTest
  - AvroNotationTest
- Run tests for this module only:
  - Maven: mvn -pl ksml-data-avro -am test

Build and compatibility notes
- Adding or modifying this documentation does not affect Java compilation.
- Ensure any change to mapping rules remains backward compatible for downstream vendor modules unless a major version change is intended.

Troubleshooting tips
- If an Avro schema does not produce a StructSchema at the top level, AvroSchemaParser will throw a SchemaException. Confirm the .avsc’s top type.
- For union fields with null, ensure the Avro schema places or includes 'null' correctly and check required/default mapping in AvroSchemaMapper.
- If bytes appear incorrect on round-trip, verify ByteBuffer handling in AvroDataObjectMapper and printer modes from ksml-data for debug output.

References
- Background and core concepts: ksml-data/DEVELOPER_GUIDE.md
- Vendor notation infrastructure: io.axual.ksml.data.notation.vendor.VendorNotation and VendorNotationContext (ksml-data)
- Example vendor integrations:
  - ksml-data-avro-confluent
  - ksml-data-avro-apicurio
