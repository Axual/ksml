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
import java.util.function.Supplier;

public class ConvertUtil {
    private final NativeDataObjectMapper dataObjectMapper;
    private final DataTypeDataSchemaMapper dataSchemaMapper;

    public ConvertUtil(NativeDataObjectMapper dataObjectMapper, DataTypeDataSchemaMapper dataSchemaMapper) {
        this.dataObjectMapper = dataObjectMapper;
        this.dataSchemaMapper = dataSchemaMapper;
    }

    public DataObject convert(DataType targetType, DataObject value) {
        return convert(targetType, value, false);
    }

    public DataObject convert(DataType targetType, DataObject value, boolean allowFail) {
        return convert(null, null, targetType, value, allowFail);
    }

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
            return convertList(targetListType, valueList);
        }

        // Recurse into structs
        if (targetType instanceof StructType targetStructType && value instanceof DataStruct valueStruct) {
            return convertStruct(targetStructType, valueStruct);
        }

        // Recurse into tuples
        if (targetType instanceof TupleType targetTupleType
                && value instanceof DataTuple valueTuple
                && targetTupleType.subTypeCount() == valueTuple.elements().size()) {
            return convertTuple(targetTupleType, valueTuple);
        }

        // When we reach this point, real data conversion needs to happen

        // First step is to use the converters from the notations to convert the type into the desired target type
        var convertedValue = applyNotationConverters(sourceNotation, targetNotation, targetType, value);

        // If the notation conversion was good enough, then return that result
        if (targetType.isAssignableFrom(convertedValue)) return convertedValue;

        // As a final attempt to convert to the right type, run it through the compatibility converter
        convertedValue = convertDataObject(targetType, convertedValue);
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

    private DataObject convertDataObject(DataType targetType, DataObject value) {
        // If we're already compatible with the target type, then return the value itself. This line is here mainly
        // for recursive calls from lists and maps.
        if (targetType.isAssignableFrom(value)) return value;

        // Convert from anything to string
        if (targetType == DataString.DATATYPE) return new DataString(value.toString());

        // Come up with default values if we convert from Null
        return switch (value) {
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
            case DataString stringValue -> convertStringToDataObject(targetType, stringValue.value());
            // Convert list without a value type to a list with a specific value type
            case DataList listValue when targetType instanceof ListType targetListType ->
                    convertList(targetListType, listValue);
            // Convert from schemaless structs to a struct with a schema
            case DataStruct structValue when targetType instanceof StructType targetStructType ->
                    convertStruct(targetStructType, structValue);
            // If no conversion was found suitable, then just return the object itself
            default -> value;
        };
    }

    @Nullable
    public DataObject convertStringToDataObject(DataType expected, String value) {
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
            case ListType listType -> convertStringToDataList(listType, value);
            case StructType structType -> convertStringToDataStruct(structType, value);
            case TupleType tupleType -> convertStringToDataTuple(tupleType, value);
            case UnionType unionType -> convertStringToUnionMemberType(unionType, value);
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

    private DataList convertStringToDataList(ListType type, String value) {
        if (value == null || value.isEmpty()) return null;
        final var elements = JsonNodeUtil.convertStringToList(value);
        if (elements == null) return null;
        final var result = new DataList(type.valueType());
        for (final var element : elements) {
            final var dataObject = dataObjectMapper.toDataObject(type.valueType(), element);
            if (!type.valueType().isAssignableFrom(dataObject))
                throw new DataException("Can not convert data types: expected=" + type.valueType() + ", got " + dataObject.type());
            result.add(dataObject);
        }
        return result;
    }

    @Nullable
    private DataStruct convertStringToDataStruct(StructType structType, String value) {
        if (value == null || value.isEmpty()) return null;
        final var map = JsonNodeUtil.convertStringToMap(value);
        if (map == null) return null;
        final var result = new DataStruct(structType.schema());
        for (final var entry : map.entrySet()) {
            result.put(entry.getKey(), dataObjectMapper.toDataObject(entry.getValue()));
        }
        return result;
    }

    @Nullable
    private DataTuple convertStringToDataTuple(TupleType type, String value) {
        if (value == null || value.isEmpty()) return null;
        if (value.startsWith("(") && value.endsWith(")")) {
            // Replace round brackets by square brackets and parse as list
            value = "[" + value.substring(1, value.length() - 1) + "]";
        }
        final var elements = JsonNodeUtil.convertStringToList(value);
        if (elements == null) return null;
        if (elements.size() != type.subTypeCount())
            throw new DataException("Error converting string to tuple: element count does not match");
        final var tupleElements = new DataObject[elements.size()];
        var index = 0;
        for (final var element : elements) {
            final var dataObject = dataObjectMapper.toDataObject(type.subType(index), element);
            if (!type.subType(index).isAssignableFrom(dataObject))
                throw new DataException("Can not convert data types: expected=" + type.subType(index) + ", got " + dataObject.type());
            tupleElements[index++] = dataObject;
        }
        return new DataTuple(tupleElements);
    }

    public DataObject convertStringToUnionMemberType(UnionType type, String value) {
        final var valueString = new DataString(value);
        for (final var memberType : type.memberTypes()) {
            final var dataObject = convert(memberType.type(), valueString, true);
            if (dataObject != null && memberType.type().isAssignableFrom(dataObject))
                return dataObject;
        }
        throw new DataException("Can not convert string to union: type=" + type + ", value=" + (value != null ? value : "null"));
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
            default -> throw new DataException("Can not convert NULL to " + expected);
        };
    }

    private DataObject convertList(ListType expected, DataList value) {
        // If the target list type does not have a specific value type, then simply return
        final var expectedValueType = expected.valueType();
        if (expectedValueType == null || expectedValueType == DataType.UNKNOWN) return value;

        // Create a new List with the give value type
        final var result = new DataList(expectedValueType, value.isNull());
        // Copy all list elements into the new list, possibly making sub-elements compatible
        for (int index = 0; index < value.size(); index++) {
            result.add(convertDataObject(expected.valueType(), value.get(index)));
        }
        // Return the List with made-compatible elements
        return result;
    }

    private DataObject convertStruct(StructType expected, DataStruct value) {
        // Don't recurse into Structs without a schema, just return those plainly
        final var schema = expected.schema();
        if (schema == null) return value;

        // Create a new Struct with the same schema type
        final var result = new DataStruct(expected.schema(), value.isNull());

        // Copy all struct fields into the new struct, possibly making sub-elements compatible
        for (final var entry : value.entrySet()) {
            // Determine the new value type
            final var field = schema.field(entry.getKey());
            // Only copy if the field exists in the target structure
            if (field != null) {
                final var fieldType = dataSchemaMapper.fromDataSchema(field.schema());
                // Convert to that type if necessary
                result.put(entry.getKey(), convertDataObject(fieldType, entry.getValue()));
            }
        }

        // Return the Struct with compatible fields
        return result;
    }

    private DataObject convertTuple(TupleType expected, DataTuple value) {
        if (value.elements().size() != expected.subTypeCount())
            throw new DataException("Error converting tuple to " + expected + ": element count does not match");
        final var convertedDataObjects = new DataObject[value.elements().size()];
        for (int index = 0; index < value.elements().size(); index++) {
            convertedDataObjects[index] = convertDataObject(expected.subType(index), value.elements().get(index));
        }
        return new DataTuple(convertedDataObjects);
    }
}
