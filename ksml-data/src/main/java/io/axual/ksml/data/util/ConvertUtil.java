package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.*;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Utility class for converting KSML DataObject instances between different DataType targets.
 * <p>
 * Conversion is performed recursively for complex types (lists, maps, structs, tuples) and can
 * optionally leverage Notation-specific converters to bridge between notations before applying
 * type compatibility conversions.
 */
public class ConvertUtil {
    private final NativeDataObjectMapper dataObjectMapper;
    private final DataTypeDataSchemaMapper dataSchemaMapper;

    /**
     * Create a new ConvertUtil.
     *
     * @param dataObjectMapper  mapper to convert between native values and KSML DataObject values
     * @param dataSchemaMapper  mapper to convert between KSML DataType and notation-specific schemas
     */
    public ConvertUtil(NativeDataObjectMapper dataObjectMapper, DataTypeDataSchemaMapper dataSchemaMapper) {
        this.dataObjectMapper = dataObjectMapper;
        this.dataSchemaMapper = dataSchemaMapper;
    }

    /**
     * Convert a value to the given target type.
     *
     * <p>This method uses recursive conversion for complex types and may fall back to
     * compatibility conversions when needed.
     *
     * @param targetType the desired target DataType
     * @param value      the input value as DataObject
     * @return the converted DataObject; if no conversion is needed, returns the original value
     * @throws io.axual.ksml.data.exception.DataException if conversion cannot be performed
     */
    public DataObject convert(DataType targetType, DataObject value) {
        return convert(targetType, value, false);
    }

    /**
     * Convert a value to the given target type, optionally allowing conversion failure.
     *
     * @param targetType the desired target DataType
     * @param value      the input value as DataObject
     * @param allowFail  if true, return null when conversion is not possible; otherwise throw
     * @return the converted DataObject or null if allowFail is true and conversion is not possible
     * @throws io.axual.ksml.data.exception.DataException if allowFail is false and conversion fails
     */
    public DataObject convert(DataType targetType, DataObject value, boolean allowFail) {
        return convert(null, null, targetType, value, allowFail);
    }

    /**
     * Convert a value to the given target type using the provided source and target notations.
     *
     * <p>The method first attempts notation-level conversion via Notation.converter(), then applies
     * type compatibility conversion as needed. Complex types are handled recursively.
     *
     * @param sourceNotation the notation of the input value; may be null if unknown
     * @param targetNotation the desired target notation; may be null to skip notation conversion
     * @param targetType     the desired target DataType
     * @param value          the input value as DataObject
     * @param allowFail      if true, return null when conversion is not possible; otherwise throw
     * @return the converted DataObject, or null if allowFail is true and conversion is not possible
     * @throws io.axual.ksml.data.exception.DataException if allowFail is false and conversion fails
     */
    public DataObject convert(Notation sourceNotation, Notation targetNotation, DataType targetType, DataObject value, boolean allowFail) {
        // If no conversion is possible or necessary, then just return the value object
        if (targetType == null || value == null) return value;

        // If the value represents a NULL, then convert it directly
        if (value == DataNull.INSTANCE) return convertNullToDataObject(targetType);

        // Perform type conversions recursively, going into complex types if necessary

        // If a union type is expected, then recurse into it before checking compatibility below
        if (targetType instanceof UnionType targetUnionType) {
            for (int index = 0; index < targetUnionType.memberTypes().length; index++) {
                var convertedValue = convert(sourceNotation, targetNotation, targetUnionType.memberTypes()[index].type(), value, true);
                if (convertedValue != null) return convertedValue;
            }
        }

        // Recurse into lists
        if (targetType instanceof ListType targetListType && value instanceof DataList valueList) {
            return convertList(targetListType, valueList, allowFail);
        }

        // Recurse into maps
        if (targetType instanceof MapType targetMapType && value instanceof DataMap valueMap) {
            return convertMap(targetMapType, valueMap, allowFail);
        }

        // Recurse into structs
        if (targetType instanceof StructType targetStructType && value instanceof DataStruct valueStruct) {
            return convertStruct(targetStructType, valueStruct, allowFail);
        }

        // Recurse into tuples
        if (targetType instanceof TupleType targetTupleType
                && value instanceof DataTuple valueTuple
                && targetTupleType.subTypeCount() == valueTuple.elements().size()) {
            return convertTuple(targetTupleType, valueTuple, allowFail);
        }

        // When we reach this point, real data conversion needs to happen

        // The first step is to use the converters from the notations to convert the type into the desired target type
        var convertedValue = applyNotationConverters(sourceNotation, targetNotation, targetType, value);

        // If the notation conversion was good enough, then return that result
        if (targetType.isAssignableFrom(convertedValue)) return convertedValue;

        // As a final attempt to convert to the right type, run it through the compatibility converter
        convertedValue = convertDataObject(targetType, convertedValue, allowFail);
        if (convertedValue != null) return convertedValue;

        // If we are okay with failing a conversion, then return null
        if (allowFail) return null;

        // We can't perform the conversion, so report a fatal error
        throw new DataException("Can not convert value to " + targetType);
    }

    private DataObject applyNotationConverters(Notation sourceNotation, Notation targetNotation, DataType targetType, DataObject value) {
        // First we see if the target notation is able to interpret the source value
        if (targetNotation != null && targetNotation.converter() != null) {
            final var target = targetNotation.converter().convert(value, targetType);
            if (target != null && targetType.isAssignableFrom(target.type())) return target;
        }

        // If the target notation was not able to convert, then try the source notation
        if (sourceNotation != null && sourceNotation.converter() != null) {
            final var target = sourceNotation.converter().convert(value, targetType);
            if (target != null && targetType.isAssignableFrom(target.type())) return target;
        }

        return value;
    }

    private DataObject convertDataObject(DataType targetType, DataObject value, boolean allowFail) {
        // If we're already compatible with the target type, then return the value itself. This line is here mainly
        // for recursive calls from lists and maps.
        if (targetType.isAssignableFrom(value)) return value;

        // Convert from anything to string
        if (targetType == DataString.DATATYPE) return new DataString(value.toString());

        final var result = switch (value) {
            // Come up with default values if we convert from Null
            case null -> convertNullToDataObject(targetType);
            case DataNull ignored -> convertNullToDataObject(targetType);
            // Convert numbers to their proper specific type
            case DataByte val -> {
                if (targetType == DataShort.DATATYPE) yield new DataShort(val.value().shortValue());
                if (targetType == DataInteger.DATATYPE) yield new DataInteger(val.value().intValue());
                if (targetType == DataLong.DATATYPE) yield new DataLong(val.value().longValue());
                yield val;
            }
            case DataShort val -> {
                if (targetType == DataByte.DATATYPE) yield new DataByte(val.value().byteValue());
                if (targetType == DataInteger.DATATYPE) yield new DataInteger(val.value().intValue());
                if (targetType == DataLong.DATATYPE) yield new DataLong(val.value().longValue());
                yield val;
            }
            case DataInteger val -> {
                if (targetType == DataByte.DATATYPE) yield new DataByte(val.value().byteValue());
                if (targetType == DataShort.DATATYPE) yield new DataShort(val.value().shortValue());
                if (targetType == DataLong.DATATYPE) yield new DataLong(val.value().longValue());
                yield val;
            }
            case DataLong val -> {
                if (targetType == DataByte.DATATYPE) yield new DataByte(val.value().byteValue());
                if (targetType == DataShort.DATATYPE) yield new DataShort(val.value().shortValue());
                if (targetType == DataInteger.DATATYPE) yield new DataInteger(val.value().intValue());
                yield val;
            }
            case DataDouble val -> {
                if (targetType == DataFloat.DATATYPE) yield new DataFloat(val.value().floatValue());
                yield val;
            }
            case DataFloat val -> {
                if (targetType == DataDouble.DATATYPE) yield new DataDouble(val.value().doubleValue());
                yield val;
            }
            // Convert from String to anything through recursion using the same target type
            case DataString stringValue -> convertStringToDataObject(targetType, stringValue.value(), allowFail);
            // Convert a list without a value type to a list with a specific value type
            case DataList listValue when targetType instanceof ListType targetListType ->
                    convertList(targetListType, listValue, allowFail);
            // Convert a map with one value type to a map with another value type
            case DataMap mapValue when targetType instanceof MapType targetMapType ->
                    convertMap(targetMapType, mapValue, allowFail);
            // Convert a map to a struct
            case DataMap mapValue when targetType instanceof StructType targetStructType ->
                    convertMapToStruct(targetStructType, mapValue, allowFail);
            // Convert a struct to a map
            case DataStruct structValue when targetType instanceof MapType targetMapType ->
                    convertStructToMap(targetMapType, structValue, allowFail);
            // Convert from schemaless structs to a struct with a schema
            case DataStruct structValue when targetType instanceof StructType targetStructType ->
                    convertStruct(targetStructType, structValue, allowFail);
            // If no conversion was found suitable, then just return the object itself
            default -> null;
        };

        if (result == null) {
            if (allowFail) return null;
            throw convertError(value != null ? value.type() : DataNull.DATATYPE, targetType, value);
        }
        return result;
    }

    @Nullable
    public DataObject convertStringToDataObject(DataType expected, String value, boolean allowFail) {
        if (expected == null) return new DataString(value);
        if (expected == DataNull.DATATYPE || value == null) return DataNull.INSTANCE;
        if (expected == DataByte.DATATYPE) return parseOrNull(() -> new DataByte(Byte.parseByte(value)));
        if (expected == DataShort.DATATYPE) return parseOrNull(() -> new DataShort(Short.parseShort(value)));
        if (expected == DataInteger.DATATYPE) return parseOrNull(() -> new DataInteger(Integer.parseInt(value)));
        if (expected == DataLong.DATATYPE) return parseOrNull(() -> new DataLong(Long.parseLong(value)));
        if (expected == DataFloat.DATATYPE) return parseOrNull(() -> new DataFloat(Float.parseFloat(value)));
        if (expected == DataDouble.DATATYPE) return parseOrNull(() -> new DataDouble(Double.parseDouble(value)));
        if (expected == DataString.DATATYPE) return new DataString(value);
        return switch (expected) {
            case EnumType ignored -> new DataString(value);
            case ListType listType -> convertStringToDataList(listType, value, allowFail);
            case MapType mapType -> convertStringToDataMap(mapType, value, allowFail);
            case StructType structType -> convertStringToDataStruct(structType, value, allowFail);
            case TupleType tupleType -> convertStringToDataTuple(tupleType, value, allowFail);
            case UnionType unionType -> convertStringToUnionMemberType(unionType, value, allowFail);
            default -> throw new DataException("Can not convert string to data type \"" + expected + "\": " + value);
        };
    }

    private DataObject parseOrNull(Supplier<DataObject> supplier) {
        try {
            return supplier.get();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private DataList convertStringToDataList(ListType listType, String value, boolean allowFail) {
        if (value == null || value.isEmpty()) return null;
        final var elements = JsonNodeUtil.convertStringToList(value);
        if (elements == null) {
            if (!allowFail) throw convertError(DataString.DATATYPE, listType, DataString.from(value));
            return null;
        }
        final var result = new DataList(listType.valueType());
        for (final var element : elements) {
            final var dataObject = dataObjectMapper.toDataObject(listType.valueType(), element);
            if (!listType.valueType().isAssignableFrom(dataObject))
                throw convertError(dataObject != null ? dataObject.type() : null, listType.valueType(), dataObject);
            result.add(dataObject);
        }
        return result;
    }

    private DataMap convertStringToDataMap(MapType mapType, String value, boolean allowFail) {
        if (value == null || value.isEmpty()) return null;
        final var map = JsonNodeUtil.convertStringToMap(value);
        if (map == null) {
            if (!allowFail) throw convertError(DataString.DATATYPE, mapType, DataString.from(value));
            return null;
        }
        final var result = new DataMap(mapType.valueType());
        map.forEach((key, val) -> result.put(key, dataObjectMapper.toDataObject(mapType.valueType(), val)));
        return result;
    }

    @Nullable
    private DataStruct convertStringToDataStruct(StructType structType, String value, boolean allowFail) {
        if (value == null || value.isEmpty()) return null;
        final var map = JsonNodeUtil.convertStringToMap(value);
        if (map == null) {
            if (!allowFail) throw convertError(DataString.DATATYPE, structType, DataString.from(value));
            return null;
        }
        final var result = new DataStruct(structType.schema());
        map.forEach(structFiller(structType, result, allowFail));
        return result;
    }

    @Nullable
    private DataTuple convertStringToDataTuple(TupleType tupleType, String value, boolean allowFail) {
        if (value == null || value.isEmpty()) return null;
        if (value.startsWith("(") && value.endsWith(")")) {
            // Replace round brackets by square brackets and parse as list
            value = "[" + value.substring(1, value.length() - 1) + "]";
        }
        final var elements = JsonNodeUtil.convertStringToList(value);
        if (elements == null) {
            if (!allowFail) throw convertError(DataString.DATATYPE, tupleType, DataString.from(value));
            return null;
        }
        if (elements.size() != tupleType.subTypeCount())
            throw new DataException("Error converting string to tuple: element count does not match");
        final var tupleElements = new DataObject[elements.size()];
        var index = 0;
        for (final var element : elements) {
            final var targetType = tupleType.subType(index);
            final var targetValue = dataObjectMapper.toDataObject(targetType, element);
            if (!targetType.isAssignableFrom(targetValue))
                throw convertError(targetValue != null ? targetValue.type() : null, targetType, targetValue);
            tupleElements[index++] = targetValue;
        }
        return new DataTuple(tupleElements);
    }

    public DataObject convertStringToUnionMemberType(UnionType unionType, String value, boolean allowFail) {
        final var valueString = new DataString(value);
        for (final var memberType : unionType.memberTypes()) {
            final var dataObject = convert(memberType.type(), valueString, true);
            if (dataObject != null && memberType.type().isAssignableFrom(dataObject))
                return dataObject;
        }
        if (!allowFail)
            throw convertError(DataString.DATATYPE, unionType, valueString);
        return null;
    }

    public static DataObject convertNullToDataObject(DataType expected) {
        if (expected == null || expected == DataNull.DATATYPE || expected == DataType.UNKNOWN) return DataNull.INSTANCE;
        if (expected == DataBoolean.DATATYPE) return new DataBoolean();
        if (expected == DataByte.DATATYPE) return new DataByte();
        if (expected == DataShort.DATATYPE) return new DataShort();
        if (expected == DataInteger.DATATYPE) return new DataInteger();
        if (expected == DataLong.DATATYPE) return new DataLong();
        if (expected == DataFloat.DATATYPE) return new DataFloat();
        if (expected == DataDouble.DATATYPE) return new DataDouble();
        if (expected == DataBytes.DATATYPE) return new DataBytes();
        if (expected == DataString.DATATYPE) return new DataString();
        return switch (expected) {
            case ListType listType -> new DataList(listType.valueType(), true);
            case StructType structType -> new DataStruct(structType.schema(), true);
            case UnionType ignored -> DataNull.INSTANCE;
            case MapType mapType -> new DataMap(mapType.valueType(), true);
            case EnumType ignored -> DataNull.INSTANCE;
            default -> throw new DataException("Can not convert NULL to " + expected);
        };
    }

    private DataObject convertList(ListType expected, DataList value, boolean allowFail) {
        // Create a new List with the given value type
        final var result = new DataList(expected.valueType(), value.isNull());
        // Copy all list elements into the new list, possibly making sub-elements compatible
        value.forEach(element -> result.add(convertDataObject(expected.valueType(), element, allowFail)));
        // Return the List with made-compatible elements
        return result;
    }

    private DataMap convertMap(MapType expected, DataMap value, boolean allowFail) {
        // Create a new Map with the given value type
        final var result = new DataMap(expected.valueType(), value.isNull());
        // Copy all map elements into the new map, possibly making sub-elements compatible
        value.forEach((key, val) -> result.put(key, convertDataObject(expected.valueType(), val, allowFail)));
        // Return the Map with made-compatible elements
        return result;
    }

    private DataStruct convertMapToStruct(StructType expected, DataMap value, boolean allowFail) {
        // Create a new Struct with the expected schema type
        final var result = new DataStruct(expected.schema(), value.isNull());
        // Copy all struct fields into the new struct, possibly making sub-elements compatible
        value.forEach(structFiller(expected, result, allowFail));
        // Return the Struct with compatible fields
        return result;
    }

    private DataMap convertStructToMap(MapType expected, DataStruct value, boolean allowFail) {
        // Create a new Map with the given value type
        final var result = new DataMap(expected.valueType(), value.isNull());
        // Copy all map elements into the new map, possibly making sub-elements compatible
        value.forEach((key, val) -> result.put(key, convertDataObject(expected.valueType(), val, allowFail)));
        // Return the Map with made-compatible elements
        return result;
    }

    private DataStruct convertStruct(StructType expected, DataStruct value, boolean allowFail) {
        // Create a new Struct with the expected schema type
        final var result = new DataStruct(expected.schema(), value.isNull());
        // Copy all struct fields into the new struct, possibly making sub-elements compatible
        value.forEach(structFiller(expected, result, allowFail));
        // Return the Struct with compatible fields
        return result;
    }

    private BiConsumer<String, Object> structFiller(StructType expected, DataStruct result, boolean allowFail) {
        return (key, value) -> {
            // If the expected struct type contains a schema, then only copy fields defined by the schema
            if (expected.schema() != null) {
                final var field = expected.schema().field(key);
                // Only copy if the field exists in the target structure
                if (field != null) {
                    final var fieldType = dataSchemaMapper.fromDataSchema(field.schema());
                    // Convert to that type if necessary
                    result.put(key, convertDataObject(fieldType, dataObjectMapper.toDataObject(value), allowFail));
                }
            } else {
                result.put(key, dataObjectMapper.toDataObject(value));
            }
        };
    }

    private DataObject convertTuple(TupleType expected, DataTuple value, boolean allowFail) {
        if (value.elements().size() != expected.subTypeCount())
            throw new DataException("Error converting tuple to " + expected + ": element count does not match");
        final var convertedDataObjects = new DataObject[value.elements().size()];
        for (int index = 0; index < value.elements().size(); index++) {
            convertedDataObjects[index] = convertDataObject(expected.subType(index), value.elements().get(index), allowFail);
        }
        return new DataTuple(convertedDataObjects);
    }

    private DataException convertError(DataType sourceType, DataType targetType, DataObject value) {
        final var sourceTypeStr = sourceType != null ? sourceType.toString() : "null type";
        final var targetTypeStr = targetType != null ? targetType.toString() : "null type";
        final var valueStr = value != null ? value.toString(DataObject.Printer.EXTERNAL_NO_SCHEMA) : DataNull.INSTANCE.toString();
        return new DataException("Can not convert " + sourceTypeStr + " to " + targetTypeStr + ": value=" + valueStr);
    }
}
