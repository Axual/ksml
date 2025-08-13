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
