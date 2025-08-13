# KSML Data (ksml-data) — Developer Guide

This guide explains the KSML DataObject model and how native Java values are converted to DataObjects. It is intended for maintainers and also formatted so AI tools can consume it to assist refactorings.

Version/date: 2025-08-12


## 1. DataObject types and native conversion rules

Overview:
- DataObject defines the common abstraction for all KSML values. Every DataObject carries a DataType (type metadata) and supports pretty-printing via DataObject.Printer.
- Implementations are either scalar (wrapping a single primitive-like value) or composite (wrapping collections, maps, structs, or tuples of other DataObjects).
- Native Java ↔ DataObject mappings are implemented primarily by NativeDataObjectMapper and ConvertUtil.

This section enumerates current DataObject types, their roles, and how native Java objects are converted into them.


### 1.1 Scalar DataObjects (extend DataPrimitive<T>)

All scalars validate the wrapped value against their SimpleType on construction and expose the raw value via value(). toString rendering uses ValuePrinter and printer modes.

- DataNull
  - Purpose: Represents a logical null sentinel in the KSML model. Singleton: DataNull.INSTANCE.
  - Type: SimpleType(Null.class, "null"). Assignability accepts only null runtime values.
  - Native conversion: Any native null value maps to DataNull.INSTANCE (ConvertUtil.convertNullToDataObject or mapper branch). When an expected target type is provided for a null, ConvertUtil will return a typed empty/null instance for some types (see 1.4 Null handling rules).

- DataBoolean
  - Purpose: Boolean scalar wrapper.
  - Type: SimpleType(Boolean.class, "boolean").
  - Native conversion: Java Boolean -> DataBoolean.

- DataByte
  - Purpose: 8-bit integer scalar.
  - Type: SimpleType(Byte.class, "byte").
  - Native conversion: Java Byte -> DataByte. With an expected numeric type (Byte/Short/Integer/Long) the mapper coerces by narrowing/widening before wrapping the appropriate type instance (see 1.3 Numeric coercion).

- DataShort
  - Purpose: 16-bit integer scalar.
  - Type: SimpleType(Short.class, "short").
  - Native conversion: Java Short -> DataShort. Coerces to expected numeric type if provided.

- DataInteger
  - Purpose: 32-bit integer scalar.
  - Type: SimpleType(Integer.class, "int").
  - Native conversion: Java Integer -> DataInteger. Coerces to expected numeric type if provided.

- DataLong
  - Purpose: 64-bit integer scalar.
  - Type: SimpleType(Long.class, "long").
  - Native conversion: Java Long -> DataLong. Coerces to expected numeric type if provided.

- DataFloat
  - Purpose: 32-bit floating point scalar.
  - Type: SimpleType(Float.class, "float").
  - Native conversion: Java Float -> DataFloat. If expected is Double, mapper upcasts to DataDouble; otherwise DataFloat.

- DataDouble
  - Purpose: 64-bit floating point scalar.
  - Type: SimpleType(Double.class, "double").
  - Native conversion: Java Double -> DataDouble. If expected is Float, mapper downcasts to DataFloat; otherwise DataDouble.

- DataBytes
  - Purpose: Binary value as byte[]. Internally copies input array defensively.
  - Type: SimpleType(byte[].class, "bytes").
  - Native conversion: Java byte[] -> DataBytes.

- DataString
  - Purpose: UTF-8 string wrapper. Provides isEmpty() and factory from(String).
  - Type: SimpleType(String.class, "string").
  - Native conversion: CharSequence and String -> DataString(value.toString()). Also used to represent EnumType values at runtime (EnumType is a DataType whose value representation is stringy).

Notes:
- Equality/hash for DataPrimitive is defined on the wrapped value.
- toString respects Printer options (INTERNAL, EXTERNAL_*). DataBytes prints hex bytes.


### 1.2 Composite DataObjects

- DataList
  - Purpose: Ordered collection of DataObject elements. Can represent null (isNull()) or a concrete list.
  - Type: ListType(valueType). valueType defaults to DataType.UNKNOWN unless specified.
  - Native conversion: Java List<?> -> DataList. The mapper uses the expected ListType (if provided) to set element valueType; otherwise it infers from the first element or uses UNKNOWN. Each element is recursively mapped with toDataObject(expectedElementType, element) and then compatibility-converted if needed.

- DataMap
  - Purpose: String-keyed map of DataObject values. Can represent null (isNull()). Keys are always strings; values are typed.
  - Type: MapType(keyType=string, valueType).
  - Native conversion: Java Map<?,?> -> If expected is MapType, convert to DataMap with that valueType. Non-string keys are converted to strings via MapUtil.stringKeys prior to mapping values.

- DataStruct
  - Purpose: Structured, possibly schema-backed map-like object with string keys and DataObject values. Preserves ordering with a comparator and supports presence checks for optional vs required fields.
  - Type: StructType(schema?) where schema may be null (schemaless) or an actual StructSchema.
  - Native conversion: Java Map<?,?> -> If expected is StructType, convert to DataStruct using either its schema (typed fields) or schemaless mode. For schemaless conversion when expected isn’t provided, the default path converts maps to DataStruct with no schema.
  - From DataMap to DataStruct/StructType: ConvertUtil.convertMapToStruct enforces schema field presence and recursively converts field values to their field types.

- DataTuple
  - Purpose: Positional product type (fixed length). Extends Tuple<DataObject> and implements DataObject; length and element types are captured by TupleType.
  - Type: TupleType(subTypes...)
  - Native conversion: io.axual.ksml.data.value.Tuple<?> -> DataTuple; element-by-element conversion with inferred or expected subtypes.

Notes:
- Composite types implement toString(Printer) to include schema names according to Printer mode and to render children using childObjectPrinter().


### 1.3 Numeric coercion rules (native → DataObject)

The NativeDataObjectMapper performs limited numeric coercion guided by an expected DataType:
- Input Byte/Short/Integer/Long:
  - If expected is one of {byte, short, int, long}, mapper converts the primitive to the appropriate range and wraps it in the corresponding DataX object.
  - If expected is null or not a numeric type, it wraps using the runtime type (e.g., Integer -> DataInteger).
- Input Float/Double:
  - If expected is Float, a Double input is downcast to DataFloat.
  - If expected is Double, a Float input is upcast to DataDouble.
  - Otherwise wrap with the runtime type.

Downcasting may lose precision; this is intentional and used when the expected type demands it.


### 1.4 Null handling rules

- Mapping native null without expected type returns DataNull.INSTANCE.
- ConvertUtil.convertNullToDataObject(expected) returns a typed-null placeholder when possible:
  - For scalars: new DataBoolean(), new DataByte(), new DataShort(), new DataInteger(), new DataLong(), new DataFloat(), new DataDouble(), new DataBytes(), new DataString() — these are DataPrimitive wrappers with a null internal value.
  - For composite types:
    - ListType(v): new DataList(v, true) — isNull() == true
    - StructType(s): new DataStruct(s, true) — isNull() == true
    - UnionType: DataNull.INSTANCE
  - For DataNull, UNKNOWN, or null expected: DataNull.INSTANCE

This allows typed pipelines to preserve “shape” when nulls flow through, matching Kafka tombstone semantics for structs.


### 1.5 Map vs Struct inference

When converting a native Map<?,?> without a concrete expected type:
- If an expected StructType is provided, it becomes a DataStruct with that schema.
- If an expected MapType is provided, it becomes a DataMap with valueType.
- Otherwise, default behavior constructs a schemaless DataStruct (see NativeDataObjectMapper.convertMapToDataStruct(Map, DataSchema=null)). Keys are treated as strings.


### 1.6 List element typing

When converting a native List<?>:
- If expected is ListType(T), DataList is created with valueType T and each element is converted to T (with compatibility conversion as needed).
- If expected is absent, ListType is inferred from the first element (or UNKNOWN for empty lists). Elements are converted accordingly; later validation may fail if heterogeneous types are encountered downstream.


### 1.7 Strings and Enums

- Strings: Any CharSequence becomes DataString. ConvertUtil also supports string-based parsing into numeric and composite types when a target type is provided (e.g., parse "123" into DataInteger when expected is int; parse JSON-like strings to lists/maps/structs/tuples).
- Enums: EnumType (a DataType) models enumerations of string symbols. There is no dedicated DataEnum object type; enum values are represented using DataString with one of the allowed symbols. Assignability checks for EnumType verify the DataObject’s string value against the symbol list.


### 1.8 Tuple details

- Native io.axual.ksml.data.value.Tuple<?> is converted to DataTuple with per-element conversion.
- TupleType captures the element types; ConvertUtil enforces equal arity when converting strings to tuples or converting between tuple types.


### 1.9 Round-tripping (DataObject → native)

NativeDataObjectMapper.fromDataObject performs the inverse mapping:
- DataNull -> null
- DataBoolean -> Boolean, DataByte -> Byte, DataShort -> Short, DataInteger -> Integer, DataLong -> Long, DataFloat -> Float, DataDouble -> Double
- DataBytes -> byte[]
- DataString -> String
- DataList -> List<Object> (null if list isNull())
- DataMap -> Map<String,Object> (null if map isNull())
- DataStruct -> Map<String,Object> (typed structs include required fields and present optional fields; schemaless copy all entries)
- DataTuple -> Tuple<Object>


### 1.10 Printing and type names

- DataObject.Printer modes control inclusion of schema names. INTERNAL is used for logs; EXTERNAL_* can include schema on the top-level value and/or nested values.
- Complex types derive type names/specs from their subtypes (e.g., ListType[T] prints name “ListOfT” and spec “[T]”).


### 1.11 Guidance for refactoring/AI tools

- Treat DataObject as the sole polymorphic value container in ksml-data. New scalar types should extend DataPrimitive<T> with a SimpleType; new composite types should extend ComplexType-backed implementations.
- Preserve null-handling semantics: DataNull is canonical null; composite types can be “null” containers via isNull(); scalar wrappers may hold null values.
- Maintain NativeDataObjectMapper as the single place for native→DataObject mapping rules; ConvertUtil should perform DataType-guided compatibility conversions and string parsing. Updates to conversion rules must keep tests in sync (StringSerdeTest, DataObjectSerdeTest) and avoid breaking assignability contracts in DataType implementations.
- When adding schema-aware types (e.g., unions), follow the recursion patterns in ConvertUtil and ensure convertNullToDataObject knows how to build a typed-null where appropriate.


---

End of section 1. Subsequent sections will document notation integration, schema mapping, and serde boundaries.


## 2. DataType implementations

This section enumerates the DataType hierarchy and the behavioral rules each implementation enforces. These types carry metadata used to validate DataObjects and guide conversions in ConvertUtil and NativeDataObjectMapper.

### 2.1 DataType (interface) and UNKNOWN
- Key API:
  - containerClass(): the Java container class used for assignability checks.
  - name(): human-readable name (often used by printers and logs).
  - spec(): short spec string (e.g., "[T]", "Map(T)", or primitive names) used in toString for some types.
  - isAssignableFrom(DataType), isAssignableFrom(Class<?>), isAssignableFrom(Object), and default isAssignableFrom(DataObject) which accepts DataNull.INSTANCE as universally assignable to signal null-tolerant checks.
- UNKNOWN: DataType.UNKNOWN acts as a wildcard:
  - containerClass() = Object.class, name() = "Unknown", spec() = "?".
  - All isAssignableFrom variants return true.
  - toString() returns spec().

### 2.2 SimpleType
- Represents scalar/primitive-like types backed by a single containerClass (e.g., String, Integer, byte[]).
- isAssignableFrom(DataType other) checks class assignability when other is SimpleType; isAssignableFrom(Class<?>) delegates to containerClass.isAssignableFrom.
- equals/hashCode are based on mutual assignability and containerClass.
- toString() returns spec (typically same as name unless customized).
- Used by all scalar DataObject wrappers and by EnumType as base.

### 2.3 ComplexType (abstract)
- Base class for composite types with one or more subTypes.
- Holds containerClass, name, spec, and DataType[] subTypes.
- Helpers:
  - buildName and buildSpec assemble readable names/specs from subTypes.
- Assignability rules:
  - isAssignableFrom(Class<?>) checks containerClass assignability.
  - isAssignableFrom(DataType) succeeds when the other is a ComplexType with compatible containerClass and each subType[i] isAssignableFrom(other.subType[i]).
- equals/hashCode based on mutual assignability, containerClass, and subTypes.
- toString() returns name.

### 2.4 ListType
- Extends ComplexType with containerClass = java.util.List.
- Name/spec examples: "ListOfString", spec "[string]".
- API:
  - valueType(): single subType (index 0).
  - createFrom(DataType type): extracts ListType from an outer ComplexType with List container and one subtype.

### 2.5 MapType
- Extends ComplexType with containerClass = java.util.Map.
- Semantics: keys must be strings; valueType is configurable.
- Construction enforces key subType = DataString.DATATYPE; value subType is provided.
- Name/spec example: name "MapOfStringAndInt", spec "map(string)" (per DataSchemaConstants.MAP_TYPE).
- API:
  - keyType(): subType(0) — always DataString.DATATYPE.
  - valueType(): subType(1).

### 2.6 StructType
- Extends ComplexType with containerClass = java.util.Map but modeled as schema-aware struct.
- Holds optional name and StructSchema schema.
- If schema is StructSchema.SCHEMALESS, schema is normalized to null; name defaults to provided name, schema name, or "Struct".
- keyType() and valueType() expose subTypes from ComplexType base (initialized like a map with keyType string and UNKNOWN value type). toString() returns the struct name.
- fieldType(String fieldName, DataType incaseNoSchema, DataType incaseNoSuchField) resolves field type from schema using DataTypeDataSchemaMapper.
- Assignability override:
  - Accepts DataNull (Kafka tombstones) explicitly: if type == DataNull.DATATYPE return true.
  - Requires ComplexType assignability; when both are StructTypes and schema != null, defers to StructSchema.isAssignableFrom for structural compatibility; schemaless structs are considered compatible.

### 2.7 TupleType
- Extends ComplexType with containerClass = Tuple.class (io.axual.ksml.data.value.Tuple).
- Name/spec example: "TupleOfIntAndString", spec "(int, string)".
- Encodes fixed arity via subTypes; used by DataTuple.

### 2.8 EnumType
- Extends SimpleType with containerClass = String and name/spec from DataSchemaConstants.ENUM_TYPE.
- Holds symbols: List<Symbol>.
- Overrides isAssignableFrom(DataObject): returns true only if the DataObject renders to a string equal to one of the allowed symbols; ensures values conform to the enum domain. Note this override is value-based rather than type-only.

### 2.9 UnionType
- Extends ComplexType to represent tagged unions (sum types).
- MemberType record: (name, DataType type, int tag). Convenience constructor with only DataType is provided.
- Name/spec example: name "UnionOfIntOrString", spec "union(int, string)" (using DataSchemaConstants.UNION_TYPE).
- Assignability rules:
  - If type is the same instance, return true.
  - If the other is a UnionType, require same length and mutual assignability per position (member-wise both directions) via isAssignableFromOtherUnion.
  - Otherwise, succeeds if any memberType.type() isAssignableFrom(type) (allows non-union values that match a member).
- isAssignableFrom(Object value) overridden to accept any value assignable to at least one member type.
- equals/hashCode reflect member compatibility.

### 2.10 Notes on names and specs
- SimpleType.toString() returns spec (often the primitive name); ComplexType.toString() returns name assembled from subTypes.
- Naming conventions use capitalization of subtype names per ComplexType.buildName, and spec strings combine subtype specs with delimiters ([], (), commas). Map/Struct/Tuple/Union customize their spec prefixes via DataSchemaConstants.

### 2.11 Guidance for refactoring/AI tools (DataType)
- When introducing a new composite DataType, extend ComplexType and decide:
  - containerClass alignment for assignability from Class<?>>.
  - subType arity and order; update buildName/spec accordingly.
  - isAssignableFrom overrides if special rules apply (like UnionType short-circuit or StructType tombstone allowance).
- Keep equals consistent with assignability (mutual acceptance) as current types do.
- Ensure ConvertUtil handles recursion for any new type (string parsing, null-typed instance creation in convertNullToDataObject, mapping between complex types).
- If a DataType is schema-backed, add hooks to DataTypeDataSchemaMapper and keep serde and notation-specific mappers in sync.


## 3. Schema classes (io.axual.ksml.data.schema)

This section describes the internal schema model used by ksml-data. Schemas capture structural information used by StructType and by notation mappers. Assignability on schemas mirrors DataType assignability where applicable and is used by DataTypeDataSchemaMapper and NativeDataObjectMapper.

3.1 DataSchema (base)
- Purpose: Base class that carries a string type identifier (see DataSchemaConstants). Provides isAssignableFrom(DataSchema) with type-based semantics; specialized subclasses override for richer rules.
- Built-in singleton primitives:
  - ANY_SCHEMA: wildcard; assignable from any non-null schema.
  - NULL_SCHEMA, BOOLEAN_SCHEMA, BYTES_SCHEMA, STRING_SCHEMA, BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE schemas.
- Numeric assignability:
  - BYTE/SHORT/INTEGER/LONG are mutually assignable within the integer set.
  - FLOAT/DOUBLE are mutually assignable within the floating-point set.
- STRING assignability:
  - Accepts assignment from NULL and from ENUM (string domain), in addition to strict string.
- toString() returns the type id.

3.2 DataSchemaConstants
- Defines canonical type ids and the default namespace (DATA_SCHEMA_KSML_NAMESPACE).
- Types include: any, null, boolean, byte, short, integer, long, float, double, bytes, fixed, string, enum, list, map, struct, union.
- isType(String) utility checks membership.

3.3 NamedSchema (abstract)
- Extends DataSchema for schemas that carry namespace, name, and optional doc.
- name(): returns explicit name or a synthesized Anonymous<ClassName> when none is set.
- fullName(): namespace + "." + name (or name when namespace empty).
- isAssignableFrom: requires other to be a NamedSchema of same base type (ignores namespace/name/doc for cross-notation comparisons).

3.4 StructSchema
- Extends NamedSchema to model structured records with named fields.
- Fields:
  - fields(): ordered list; field(String): lookup by name; field(int): by index.
  - fieldsByName map maintained internally.
- Special constant: SCHEMALESS, a singleton unnamed StructSchema used to signal "no schema" for map-like/JSON objects. Code often checks schema == StructSchema.SCHEMALESS to treat as schemaless.
- Assignability rules:
  - Base type must be struct; other must also be StructSchema.
  - For each field in this schema: if it has no default (defaultValue == null), the other must contain the field; when the other contains the field, its schema must be assignable to this field’s schema.
  - Equality is based on mutual assignability (both directions).

3.5 DataField
- Describes a field: name (nullable for anonymous), schema (non-null), doc, required flag, constant flag, tag (int), defaultValue (DataValue), order (ASCENDING/DESCENDING/IGNORE).
- NO_TAG sentinel for absence of tag; when schema is a UnionSchema, tags on the union are suppressed (only union members carry tags).
- Overloaded constructors cover common cases (anonymous required, optional with/without doc, with tag, with default, order).
- Guards: if required and a defaultValue is present, defaultValue.value() must not be null.
- isAssignableFrom(DataField other) delegates to field schema assignability.

3.6 DataValue (record)
- Simple record wrapper around Object value, used for default values within DataField.

3.7 ListSchema
- list(valueSchema): models homogeneous lists; optional name.
- hasName()/name() mirror NamedSchema-like helpers for occasional named list types (used by notations like XML).
- isAssignableFrom: requires other to be a ListSchema and valueSchema of this to be assignable from the other's valueSchema (value-wise contravariant as implemented).
- toString: "list of <valueSchema>".

3.8 MapSchema
- map(string -> valueSchema); keys are always strings by convention.
- isAssignableFrom: requires other to be MapSchema and valueSchema assignable accordingly.

3.9 EnumSchema
- Named enum with List<Symbol> symbols and optional defaultValue.
- isAssignableFrom:
  - Accepts DataSchema.STRING_SCHEMA to allow string literals representing symbols.
  - Otherwise requires other EnumSchema and that this.symbols is a superset (allows all values of other).

3.10 FixedSchema
- Named fixed-size bytes with size >= 0.
- isAssignableFrom: other must be FixedSchema with size <= this.size (wider accepts narrower).

3.11 UnionSchema
- Union of member schemas stored as DataField[] (to carry per-member name/tag for formats like Protobuf oneOf).
- Constructor flattens nested unions recursively by default.
- contains(DataSchema schema): true when any member equals the schema.
- isAssignableFrom:
  - If other is UnionSchema: all other members must be assignable to this union (i.e., each member must fit one of this union’s members with allowAssignment check).
  - If other is non-union: true if any member schema is assignable from other.
- Member-field compatibility (allowAssignment): permits assignment when member names or tags are absent (anonymous) or when both name and tag match; designed to model Protobuf oneOf semantics.

3.12 TupleSchema
- A convenience StructSchema generated from TupleType via DataTypeDataSchemaMapper; named using DATA_SCHEMA_KSML_NAMESPACE and the TupleType.toString().
- Fields are auto-generated as elem0, elem1, ..., one per tuple subtype; requires at least one element.

3.13 Schema ↔ DataType mapping
- DataTypeDataSchemaMapper bridges between DataType and DataSchema:
  - toDataSchema(DataType) and fromDataSchema(DataSchema) convert between representations (used by ConvertUtil, NativeDataObjectMapper, and struct field conversion).
  - StructType.fieldType uses the mapper to resolve field schema to field DataType.
- NativeDataObjectMapper:
  - inferDataTypeFromNativeMap uses expected StructSchema to choose StructType; defaults to schemaless StructType otherwise.
  - convertMapToDataStruct uses StructType/StructSchema to convert map entries into typed DataStruct fields.

3.14 Guidance for refactoring/AI tools (Schema)
- Keep assignability rules symmetric where intended (mutual for equality) and carefully document any asymmetry (eg. FixedSchema size >= other.size, EnumSchema superset semantics).
- Preserve StructSchema.SCHEMALESS as a unique singleton and normalize it to null in StructType constructor as current code does (StructType treats SCHEMALESS as null schema).
- When adding a new schema class, update DataTypeDataSchemaMapper and any notation-specific mappers; add tests under src/test/java/io/axual/ksml/data/schema.
- Ensure DataField invariants hold (required + defaultValue != null implies defaultValue.value() != null), and respect union tagging conventions when targeting Protobuf.



## 4. Mappers: object and schema mapping layer

This section documents the mapper package and clarifies how mappers connect native values, DataObjects, DataTypes, and DataSchemas. Use this as a reference when extending conversions or adding new integrations.

4.1 Overview and responsibilities
- Object mapping (DataObjectMapper<T>): bridges native values of type T and KSML DataObject. Implementations:
  - NativeDataObjectMapper: the general-purpose native Java <-> DataObject mapper used across the library.
  - StringDataObjectMapper: a focused mapper for String notation/serde boundaries.
- Schema mapping (DataSchemaMapper<T>): bridges native type models and KSML schemas. Implementation:
  - DataTypeDataSchemaMapper: bidirectional mapping between DataType and DataSchema.
- Conversion helper: ConvertUtil performs DataObject -> DataObject conversions guided by DataType and may call string parsers/notation converters and create typed nulls.

Relationship of concepts:
- DataObject = runtime value container, always carries a DataType.
- DataType = logical type system used for validation and conversion (ListType, StructType, etc.).
- DataSchema = serializable schema model used by notations and schema registries.
- DataTypeDataSchemaMapper bridges DataType <-> DataSchema.
- NativeDataObjectMapper bridges native Java objects <-> DataObject, and leverages ConvertUtil and DataTypeDataSchemaMapper.

4.2 DataObjectMapper<T> (interface)
- API:
  - DataObject toDataObject(DataType expected, T value): map native value to DataObject; may consider expected type.
  - DataObject toDataObject(T value): convenience default delegating to the above with null expected.
  - T fromDataObject(DataObject value): map DataObject back to native type.
- Guidance:
  - Implementers should never throw for null values unless your mapper forbids nulls; prefer returning DataNull or typed-null via ConvertUtil when appropriate.
  - If unable to map (e.g., expected type mismatch), return null only if the calling context expects it; otherwise throw a DataException with helpful context.

4.3 NativeDataObjectMapper
- Purpose: Main entry for mapping arbitrary Java values (Boolean, Number, String, byte[], List, Map, Tuple, or existing DataObject) to DataObjects, optionally guided by an expected DataType; and mapping DataObject back to native values.
- Key collaborators: ConvertUtil (for compatibility conversions, string parsing, recursion), DataTypeDataSchemaMapper (schema<->type mapping), SchemaResolver<DataSchema> (optional, for resolving named schemas when converting maps to typed structs).
- Behavior highlights (native -> DataObject):
  - Null: ConvertUtil.convertNullToDataObject(expected) (typed nulls for scalars, null containers for lists/structs, DataNull for unknowns).
  - Already a DataObject: returned as-is (may be further converted if expected is provided and incompatible).
  - Booleans, Numbers, Bytes, Strings: wrapped in corresponding DataPrimitive types; numeric coercion uses expected type when present.
  - byte[] -> DataBytes; CharSequence -> DataString.
  - List<?> -> DataList; valueType inferred from expected ListType or first element (UNKNOWN when empty).
  - Map<?,?> -> If expected is MapType, become DataMap; if expected is StructType, become DataStruct respecting schema; otherwise schemaless DataStruct by default (keys coerced to String via MapUtil.stringKeys).
  - Tuple<?> -> DataTuple with per-element mapping and inferred TupleType.
  - Enum<?> -> EnumType inferred on the fly (symbols from enum constants) when inferring types.
- Behavior highlights (DataObject -> native):
  - Primitives unwrap to corresponding boxed types and arrays; DataNull maps to null.
  - DataList/DataMap/DataStruct convert recursively; for typed structs, required fields are preserved, optional present fields included; schemaless structs copy all entries.
  - DataTuple -> Tuple<Object> with per-element conversion.
- Type inference helpers: inferDataTypeFromObject(..) returns a DataType for heterogeneous inputs (including lists, maps, tuples, enums). inferDataTypeFromNativeMap may prefer StructType when expected schema is provided.
- Schema integration: When a StructType with schema is expected, map fields to DataObjects using DataTypeDataSchemaMapper.fromDataSchema per field; if a schema name must be loaded, SchemaResolver is used (loadSchemaByName).

4.4 StringDataObjectMapper
- Purpose: Narrow mapper used by StringSerde to translate between Kafka’s string value and DataString.
- toDataObject(expected, String): returns new DataString(value) only when expected == DataString.DATATYPE; otherwise returns null so callers can decide how to proceed.
- fromDataObject(DataObject): returns String only when the value is a DataString; otherwise null.
- Usage pattern: In serdes, StringDataObjectMapper is paired with NativeDataObjectMapper; tests (StringSerdeTest, DataObjectSerdeTest) exercise happy paths and mismatch errors.

4.5 DataSchemaMapper<T> (interface)
- API:
  - DataSchema toDataSchema(String namespace, String name, T value)
  - default overloads for name-only and nameless variants
  - T fromDataSchema(DataSchema schema)
- Guidance: Use for bridging type systems into DataSchema; if your T is a DataType, prefer DataTypeDataSchemaMapper.

4.6 DataTypeDataSchemaMapper
- Purpose: Canonical bridge between DataType and DataSchema used by ConvertUtil, NativeDataObjectMapper, and StructType.fieldType.
- toDataSchema(DataType):
  - UNKNOWN -> ANY_SCHEMA; primitives map to corresponding singleton schemas.
  - EnumType -> EnumSchema(namespace=null, name=enumType.name(), symbols=enumType.symbols()).
  - ListType -> ListSchema(toDataSchema(valueType)).
  - MapType -> MapSchema(toDataSchema(valueType)).
  - StructType -> StructSchema(copy) when schema present, else StructSchema.SCHEMALESS.
  - TupleType -> TupleSchema (auto-generates elem0..elemN using the mapper).
  - UnionType -> UnionSchema with DataField[] members, preserving member name and tag.
- fromDataSchema(DataSchema):
  - ANY/NULL/primitive schemas map back to corresponding DataTypes; String allows EnumSchema on assignability but maps to DataString type here.
  - EnumSchema -> EnumType(symbols); ListSchema -> ListType; MapSchema -> MapType; TupleSchema -> TupleType; StructSchema -> StructType(schema); UnionSchema -> UnionType with MemberType(name, type, tag).
- Notes:
  - StructSchema.SCHEMALESS is normalized to null schema in StructType; equality/assignability semantics are preserved per existing rules.
  - TupleSchema is a specialized StructSchema generated for TupleType; requires at least one element.

4.7 ConvertUtil and how it ties mappers together
- Entry points: convert(targetType, value) and overloads with notation; returns a DataObject of targetType.
- Recursion: For ListType/MapType/StructType/TupleType/UnionType, recursively converts children/members; union tries members in order.
- String parsing: convertStringToDataObject parses primitives and composite forms (JSON-like for lists/maps/structs and tuple notation).
- Nulls: convertNullToDataObject builds typed-null holders as described in Section 1.4.
- Integration with mappers: Uses NativeDataObjectMapper to re-map string-parsed natives into DataObjects of the appropriate types; uses DataTypeDataSchemaMapper for struct field type resolution and tuple field schema generation.

4.8 Putting it all together: typical flows
- Serde write (native -> bytes):
  1) NativeDataObjectMapper.toDataObject(expected, value) -> DataObject.
  2) Serde-specific mapper (e.g., StringDataObjectMapper for string) produces target DataObject for delegate serializer if applicable.
  3) Delegate serializer consumes the native payload (e.g., String) derived via DataObjectSerde’s nativeMapper.fromDataObject.
- Serde read (bytes -> native/DO):
  1) Delegate deserializer yields native value (e.g., String).
  2) Serde-specific mapper wraps into DataObject of expected type (e.g., DataString).
  3) Application/native layer can use NativeDataObjectMapper.fromDataObject to unwrap to plain Java if needed.
- Structs with schema:
  - Expected StructType(schema) ensures incoming maps are converted field-by-field using DataType derived from field schemas; missing required fields are handled per downstream validation/usage.
- Unions:
  - ConvertUtil attempts each union member in order; when parsing strings, convertStringToUnionMemberType tries to parse according to each member DataType.

4.9 Implementation guidance and pitfalls
- Keep NativeDataObjectMapper the single authority for native <-> DataObject rules. If you must add special-case support (e.g., new native container), update type inference and conversion symmetry.
- When adding schema-backed types or new schema classes, update DataTypeDataSchemaMapper in both directions.
- Respect assignability contracts: use DataType.isAssignableFrom and StructSchema/UnionSchema assignability when validating.
- Return helpful error messages: ConvertUtil.convertError includes source/target type and value; prefer using it when failing conversions.
- Be careful with nulls: composite DataObjects can be “null containers” (isNull()); scalars can wrap null values; DataNull is the canonical null sentinel.
- StringDataObjectMapper intentionally returns null for mismatched expectation; calling code (e.g., serdes) must validate types and throw DataException on mismatch as tests demonstrate.
- When adding a new DataObjectMapper for another native type T, follow the same pattern: honor expected types, preserve symmetry in fromDataObject, and avoid silent lossy conversions.


## 5. Serdes (Kafka serialization/deserialization layer)

This section documents the Serde components under io.axual.ksml.data.serde and clarifies how they connect Kafka’s byte[] boundary with KSML DataObjects, DataTypes, and the mapper layer.

5.1 Overview
- Serde<Object> components are used at Kafka boundaries to translate between bytes and objects. In KSML, serdes either:
  - Bridge native Java values and KSML DataObjects (DataObjectSerde, StringSerde), or
  - Wrap/decorate existing serdes for cross-cutting behavior (HeaderFilterSerde, ConfigInjectionSerde, WrappedSerde), or
  - Provide simple leaf serdes (NullSerde, ByteSerde), or
  - Orchestrate multiple serdes for union types (UnionSerde).
- Relationship to mappers:
  - NativeDataObjectMapper maps arbitrary native values to DataObject (and back), guided by expected DataType.
  - Specialized mappers (e.g., StringDataObjectMapper) handle serde-specific native types at the boundary.
  - ConvertUtil and DataTypeDataSchemaMapper are not used directly by serdes but are leveraged by mappers invoked from serdes.

5.2 DataObjectSerde
- Purpose: Generic adapter that composes a native-mapper and a serde-mapper around delegate Kafka Serializer/Deserializer.
- Constructor: DataObjectSerde(name, serializer, deserializer, expectedDataType, serdeMapper, nativeMapper).
- Serialization flow: native value -> nativeMapper.toDataObject(...) -> typed-null to null bytes when DataNull.INSTANCE -> serdeMapper.fromDataObject(...) -> delegate Serializer.
- Deserialization flow: delegate Deserializer -> serdeMapper.toDataObject(expected, native) -> DataObject.
- Error handling: Wraps any exception in DataException with uppercase name and helpful context ("... message could not be (de)serialized ...").
- Use cases: Building serdes for notations where serdeMapper knows how to wrap/unwrap boundary types (see StringSerde and tests in DataObjectSerdeTest).

5.3 StringSerde
- Purpose: Convenience Serde that targets DataString at the boundary using Kafka’s StringSerializer/StringDeserializer.
- Collaborators: NativeDataObjectMapper (user values -> DataObject), StringDataObjectMapper (String <-> DataString), expected DataType (usually DataString.DATATYPE).
- Serializer: validates via nativeMapper.toDataObject(expected, value); maps to String via stringMapper.fromDataObject; delegates to StringSerializer.
- Deserializer: StringDeserializer -> stringMapper.toDataObject(expected, str); validates assignability; returns DataObject.
- Errors: Throws DataException on type mismatch (see StringSerdeTest).

5.4 UnionSerde
- Purpose: Serde for UnionType values. Holds a list of member serializer/deserializer pairs obtained via SerdeSupplier per union member.
- Serialization: null/DataNull -> null bytes; otherwise iterate members and choose the first whose DataType isAssignableFrom the value (DataObject or native). If none match, throws DataException.
- Deserialization: null/empty -> DataNull.INSTANCE; otherwise try each member deserializer; first successful and type-compatible result wins; else throws DataException.
- Configuration: configure(...) cascades to all member serializers/deserializers.

5.5 HeaderFilterSerde
- Purpose: Decorator that filters specific record headers during serialize/deserialize.
- Behavior: After delegate calls, removes any headers whose keys are in filteredHeaders; for deserialization creates a filtered copy of headers before delegating.
- Usage: Wrap another Serde when hiding/removing sensitive headers; see HeaderFilterSerdeTest.

5.6 ConfigInjectionSerializer, ConfigInjectionDeserializer, ConfigInjectionSerde
- Purpose: Allow injecting or mutating configuration maps before delegating to underlying serializer/deserializer.
- Pattern: modifyConfigs(configs, isKey) protected hook point; ConfigInjectionSerde wires both sides.
- Behavior: All deserialize/serialize overloads delegate to the underlying implementations; only configure(...) is intercepted.
- Example: ConfigInjectionDeserializerTest demonstrates injecting extra config entries.

5.7 WrappedSerde
- Purpose: Simple wrapper that exposes a delegate’s serializer and deserializer while centralizing configure/close delegation.
- Behavior: serializer()/deserializer() return wrapper instances that call through to the delegate’s serializer/deserializer; configure propagates to both.
- Usage: Useful for adapting/transporting a Serde while unifying configuration.

5.8 NullSerde
- Purpose: Represent KSML DataNull over Kafka.
- Serialization: always returns null bytes.
- Deserialization: null or empty byte[] -> Null.NULL sentinel (data.value layer); otherwise throws DataException.

5.9 ByteSerde
- Purpose: Serialize/deserialize a single Java Byte.
- Serialization: null -> null bytes; otherwise single-byte array with the value.
- Deserialization: null/empty -> null; otherwise first byte as Byte.

5.10 SerdeSupplier
- Purpose: Strategy to supply a Serde for a given DataType and key/value role; used by UnionSerde to obtain member serdes.

5.11 Guidance and gotchas (Serde)
- Keep DataObjectSerde the main bridge when you need to combine native and boundary mappings. Prefer composing with dedicated boundary mappers rather than embedding conversion logic in serdes.
- Always validate types against the expected DataType where applicable; leverage DataType.isAssignableFrom for DataObject and native.
- Wrap errors in DataException with clear context; follow DataObjectSerde’s pattern so tests remain consistent.
- Be careful with nulls: null bytes and DataNull.INSTANCE must be handled explicitly (especially for unions and null serde). Prefer typed-null holders only inside DataObject values; at Kafka boundary, many serdes reduce to null bytes.
- When decorating serdes (headers/config wrappers), ensure that configure(...) and close() propagate correctly to avoid leaking delegate state.
- For new notations, create a dedicated boundary mapper (like StringDataObjectMapper), then implement a specialized Serde or use DataObjectSerde with appropriate delegates.


## 6. Notations (formats) and extending KSML via ServiceLoader

This section documents the Notation layer: how KSML models concrete data formats (Avro, Protobuf, JSON, CSV, XML, Binary, JSON Schema, etc.), how to implement new notations, and how the ServiceLoader-based discovery makes them available to KSML.

6.1 What is a Notation?
- A Notation encapsulates:
  - defaultType(): the most natural KSML DataType for the format (e.g., JSON supports list/struct union; Avro/Protobuf often map to StructType, etc.).
  - name(): contextual name ("vendor_notation" when vendor present; from NotationContext.name()).
  - filenameExtension(): conventional file extension (".avsc", ".proto", ".json", ".xml", ...).
  - serde(DataType, isKey): a Kafka Serde<Object> suitable for the requested type under this notation.
  - converter(): Notation.Converter able to turn DataObject values into other DataTypes specific to the notation when necessary (invoked by ConvertUtil before compatibility conversion).
  - schemaParser(): Notation.SchemaParser to parse textual schema artifacts into DataSchema (e.g., Avro schema JSON, Protobuf .proto, JSON Schema documents).
- Implementations often extend helpers:
  - BaseNotation: stores NotationContext, filename extension, default type, converter, and schema parser; provides name() and a common exception utility.
  - StringNotation: for notations carried as textual strings; returns a StringSerde wired with a provided String-mapper.
  - VendorNotation: for vendor-backed serdes (e.g., Confluent/Apicurio registries) using a VendorSerdeSupplier and a serde-boundary DataObjectMapper.

6.2 Core support classes
- NotationContext: carries notationName, optional vendorName, a NativeDataObjectMapper, and serdeConfigs (a mutable Map<String,String>) used to configure underlying serializers/deserializers. The name() is vendor_notation when vendor present.
- VendorNotationContext: extends NotationContext and additionally carries:
  - VendorSerdeSupplier serdeSupplier: supplies vendor-specific Serde<Object> instances for given DataType/isKey.
  - DataObjectMapper<Object> serdeMapper: boundary-mapper used by DataObjectSerde to adapt vendor serdes to KSML DataObjects.
- Notation.Converter: converts a DataObject to a DataObject of target type in a notation-aware way. ConvertUtil first tries targetNotation.converter() and then sourceNotation.converter() before falling back to compatibility conversions.
- Notation.SchemaParser: parses string schema content to a DataSchema; the returned schema is mapped further via DataTypeDataSchemaMapper when needed.
- SchemaResolver<T extends DataSchema>: a generic resolver to look up schemas by name; used by NativeDataObjectMapper when a struct field refers to a named schema.

6.3 Ready-made notation modules in this repository
- ksml-data-json: JsonNotation, JsonNotationProvider; serde via JsonSerde (string-based) supporting Map/Struct/List or their union.
- ksml-data-avro: AvroNotation + AvroSerdeSupplier; vendor flavors:
  - ksml-data-avro-confluent: ConfluentAvroNotationProvider (vendor), ConfluentAvroSerdeSupplier.
  - ksml-data-avro-apicurio: ApicurioAvroNotationProvider (vendor), ApicurioAvroSerdeSupplier.
- ksml-data-protobuf: ProtobufNotation + ProtobufSerdeSupplier; vendor flavors:
  - ksml-data-protobuf-confluent and -apicurio with corresponding providers/suppliers.
- ksml-data-jsonschema: JsonSchemaNotation and vendor flavors (confluent/apicurio) for schema registry backed serdes.
- ksml-data-xml, ksml-data-csv, ksml-data-binary, ksml-data-soap: additional notations following the same patterns.

6.4 ServiceLoader discovery: plug-in notations
- KSML discovers NotationProvider implementations via Java’s ServiceLoader in each module’s JAR.
- Provider interface: io.axual.ksml.data.notation.NotationProvider with methods:
  - notationName(): returns the base notation name (e.g., "avro", "json", "protobuf").
  - vendorName(): optional vendor qualifier (e.g., "confluent", "apicurio"); defaults to null.
  - createNotation(NotationContext ctx): returns a Notation instance constructed with the provided context.
- Registration: in your notation module’s resources, provide the service file:
  - META-INF/services/io.axual.ksml.data.notation.NotationProvider
  - File content: fully qualified class names of your provider(s), one per line, e.g.:
    io.axual.ksml.data.notation.json.JsonNotationProvider
    io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider
- Runtime: KSML (or a hosting app) obtains all providers via ServiceLoader.load(NotationProvider.class). It selects a provider by matching NotationContext.notationName() and NotationContext.vendorName() (if provided). The provider constructs the concrete Notation using the context.

6.5 Designing and implementing a new notation
Checklist:
1) Choose style:
   - String-based? Extend StringNotation and supply a DataObjectMapper<String> to bridge string <-> DataObject.
   - Vendor-backed? Extend VendorNotation and supply VendorSerdeSupplier and a serdeMapper appropriate for the vendor’s native payloads; consider value/key role semantics.
   - Custom? Extend BaseNotation and implement serde(), converter(), and schemaParser().
2) Define defaultType():
   - Pick the most natural KSML DataType for the format (e.g., for JSON: UnionType(StructType, ListType); for Protobuf/Avro: StructType with schema). Consider unions if the format supports multiple top-level shapes.
3) Implement serde(DataType type, boolean isKey):
   - Validate that the requested type is supported (using DataType.isAssignableFrom and your notation’s defaultType). Throw a clear DataException when not supported (see BaseNotation.noSerdeFor or patterns in JsonNotation and VendorNotation).
   - Build or obtain the delegate serializer/deserializer and wrap with DataObjectSerde when needed (vendor-backed) or return a specialized serde (e.g., StringSerde) for string-based notations. Configure using context().serdeConfigs().
4) Implement converter():
   - Provide conversions that are notation-specific (e.g., interpreting bytes/string/payload structures). ConvertUtil will consult this first; ensure the converter returns a DataObject whose type is compatible with the targetType when it can, otherwise return the original or null and let ConvertUtil fall back.
5) Implement schemaParser():
   - Parse input schemas (files or inline text) to DataSchema. If your format has native schema classes, map them to DataSchema via DataTypeDataSchemaMapper or your own mapper.
6) Provide a NotationProvider:
   - If vendor-agnostic: implement NotationProvider with notationName() and createNotation(ctx) new YourNotation(ctx);
   - If vendor-specific: either implement NotationProvider returning a VendorNotation instance and supply a VendorSerdeSupplier and serdeMapper, or extend VendorNotationProvider to expose vendorName() and notationName().
7) Register with ServiceLoader:
   - Include META-INF/services/io.axual.ksml.data.notation.NotationProvider in your module JAR and list your provider class.
8) Tests:
   - Follow patterns in src/test under ksml-data: BaseNotationTest, StringNotationTest, VendorNotationTest, NotationContextTest, VendorNotationProviderTest.
   - For vendor integrations, isolate tests to avoid requiring a real schema registry; use mocks/fakes as in ksml project tests.

6.6 Using NotationContext and serde configuration
- NotationContext.serdeConfigs is passed to underlying serializers/deserializers via Serde.configure(). Place notation/vendor-specific properties here (e.g., schema registry URL, subject naming strategies, auto-register settings).
- NativeDataObjectMapper in the context is used by StringSerde and DataObjectSerde for native<->DataObject boundary mapping; provide a custom mapper if your notation needs specialized native conversions.

6.7 Examples and patterns
- JsonNotation (string-based, BaseNotation):
  - defaultType = Union[list, struct]; serde() allows MapType, ListType, or the union; throws otherwise.
  - converter = JsonDataObjectConverter; schemaParser = JsonSchemaLoader.
- VendorNotation (vendor-backed):
  - serde() checks compatibility, obtains vendor Serde via VendorSerdeSupplier.get(type,isKey), then wraps with DataObjectSerde using the provided serdeMapper and context native mapper.
  - VendorNotationContext wires serde supplier and serde mapper into the base context.
- Provider examples:
  - JsonNotationProvider implements NotationProvider; vendor-specific providers live in notation-specific modules (e.g., ConfluentAvroNotationProvider, ApicurioJsonSchemaNotationProvider) and are registered through META-INF/services.

6.8 Guidance and pitfalls (Notation)
- Keep serde type validation strict; return clear error messages like "<name> serde not available for data type: <type>" to aid debugging.
- Ensure converter() is conservative: if unsure, return the input so ConvertUtil can attempt compatibility conversions. Do not mutate input DataObjects.
- Support nulls consistently: null bytes at the Kafka boundary map to DataNull.INSTANCE; typed nulls are built within ConvertUtil based on target DataType (see Section 1.4).
- Avoid heavy initialization in provider constructors; defer creating underlying serdes until serde(...) is called to prevent early failures when a notation isn’t used (pattern used by VendorNotation).
- If your notation supports named schemas, integrate a SchemaResolver for loading by name (see NativeDataObjectMapper.loadSchemaByName usage).
- When adding a new notation module, ensure the module’s POM/JAR packaging includes the service file and that classpaths are correct so ServiceLoader can find it.
