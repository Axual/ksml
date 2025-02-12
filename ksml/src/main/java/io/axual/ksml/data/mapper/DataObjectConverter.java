package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.UserTupleType;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.*;
import io.axual.ksml.execution.ExecutionContext;

import static io.axual.ksml.data.notation.UserType.DEFAULT_NOTATION;

// This DataObjectConverter makes expected data types compatible with the actual data that was
// created. It does so by converting numbers to strings, and vice versa. It converts complex
// data objects like Enums, Lists and Structs too, recursively going through sub-elements if
// necessary. When no standard conversion is possible, the mechanism reverts to using the
// source or target notation's converter to respectively export or import the data type to
// the desired type.
public class DataObjectConverter {
    private static final DataTypeSchemaMapper SCHEMA_MAPPER = new DataTypeFlattener();

    public DataObject convert(String sourceNotation, DataObject value, UserType targetType) {
        return convert(sourceNotation, value, targetType, false);
    }

    private DataObject convert(String sourceNotation, DataObject value, UserType targetType, boolean allowFail) {
        // If no conversion is possible or necessary, or the value type is already compatible with
        // the expected type, then just return the value object
        if (targetType == null || value == null || targetType.dataType().isAssignableFrom(value.type()))
            return value;

        // If the value represents a NULL, then convert it directly
        if (value == DataNull.INSTANCE) return NativeDataObjectMapper.convertFromNull(targetType.dataType());

        // Perform type conversions recursively, going into complex types if necessary

        // Recurse into union types
        if (targetType.dataType() instanceof UnionType targetUnionType) {
            for (int index = 0; index < targetUnionType.valueTypes().length; index++) {
                var convertedValue = convert(sourceNotation, value, new UserType(targetUnionType.valueTypes()[index].type()), true);
                if (convertedValue != null) return convertedValue;
            }
        }

        // Recurse into lists
        if (targetType.dataType() instanceof ListType targetListType && value instanceof DataList valueList) {
            return convertList(targetListType, valueList);
        }

        // Recurse into structs
        if (targetType.dataType() instanceof StructType targetStructType && value instanceof DataStruct valueStruct) {
            return convertStruct(targetStructType, valueStruct);
        }

        // Recurse into tuples
        if (targetType.dataType() instanceof TupleType targetTupleType
                && value instanceof DataTuple valueTuple
                && targetTupleType.subTypeCount() == valueTuple.size()) {
            return convertTuple(targetTupleType, valueTuple);
        }

        // When we reach this point, real data conversion needs to happen

        // First step is to use the converters from the notations to convert the type into the desired target type
        var convertedValue = applyNotationConverters(sourceNotation, value, targetType);

        // If the notation conversion was good enough, then return that result
        if (targetType.dataType().isAssignableFrom(convertedValue)) return convertedValue;

        // As a final attempt to convert to the right type, run it through the compatibility converter
        convertedValue = convert(convertedValue, targetType.dataType());
        if (convertedValue != null) return convertedValue;

        // If we are okay with failing a conversion, then return null
        if (allowFail) return null;

        // We can't perform the conversion, so report a fatal error
        throw new DataException("Can not convert value to " + targetType + ": " + ExecutionContext.INSTANCE.maskData(value));
    }

    private DataObject applyNotationConverters(String sourceNotation, DataObject value, UserType targetType) {
        // If the value is a union, then dig down to its real value
        while (value instanceof DataUnion valueUnion) {
            value = valueUnion.value();
        }

        // First we see if the target notation is able to interpret the source value
        var targetConverter = NotationLibrary.get(targetType.notation()).converter();
        if (targetConverter != null) {
            final var target = targetConverter.convert(value, targetType);
            if (target != null && targetType.dataType().isAssignableFrom(target.type())) return target;
        }

        // If the target notation was not able to convert, then try the source notation
        var sourceConverter = NotationLibrary.get(sourceNotation).converter();
        if (sourceConverter != null) {
            final var target = sourceConverter.convert(value, targetType);
            if (target != null && targetType.dataType().isAssignableFrom(target.type())) return target;
        }

        return value;
    }

    public DataObject convert(DataObject value, DataType targetType) {
        // If we're already compatible with the target type, then return the value itself. This line is here mainly
        // for recursive calls from lists and maps.
        if (targetType.isAssignableFrom(value)) return value;

        // Come up with default values if we convert from Null
        if (value == null || value instanceof DataNull)
            return NativeDataObjectMapper.convertFromNull(targetType);

        // Convert numbers to their proper specific type
        if (value instanceof DataByte val) {
            if (targetType == DataShort.DATATYPE) return new DataShort(val.value().shortValue());
            if (targetType == DataInteger.DATATYPE) return new DataInteger(val.value().intValue());
            if (targetType == DataLong.DATATYPE) return new DataLong(val.value().longValue());
            return val;
        }
        if (value instanceof DataShort val) {
            if (targetType == DataByte.DATATYPE) return new DataByte(val.value().byteValue());
            if (targetType == DataInteger.DATATYPE) return new DataInteger(val.value().intValue());
            if (targetType == DataLong.DATATYPE) return new DataLong(val.value().longValue());
            return val;
        }
        if (value instanceof DataInteger val) {
            if (targetType == DataByte.DATATYPE) return new DataByte(val.value().byteValue());
            if (targetType == DataShort.DATATYPE) return new DataShort(val.value().shortValue());
            if (targetType == DataLong.DATATYPE) return new DataLong(val.value().longValue());
            return val;
        }
        if (value instanceof DataLong val) {
            if (targetType == DataByte.DATATYPE) return new DataByte(val.value().byteValue());
            if (targetType == DataShort.DATATYPE) return new DataShort(val.value().shortValue());
            if (targetType == DataInteger.DATATYPE) return new DataInteger(val.value().intValue());
            return val;
        }
        if (value instanceof DataDouble val) {
            if (targetType == DataFloat.DATATYPE) return new DataFloat(val.value().floatValue());
            return val;
        }
        if (value instanceof DataFloat val) {
            if (targetType == DataDouble.DATATYPE) return new DataDouble(val.value().doubleValue());
            return val;
        }

        // Convert from anything to String
        if (targetType == DataString.DATATYPE) return convertToString(value);

        // Convert from Strings to anything
        if (value instanceof DataString str) {
            return convertFromString(targetType, str.value());
        }

        // Convert list without a value type to a list with a specific value type
        if (value instanceof DataList list && targetType instanceof ListType listType) {
            return convertList(listType, list);
        }

        // Convert from schemaless structs to a struct with a schema
        if (value instanceof DataStruct struct && targetType instanceof StructType structType) {
            return convertStruct(structType, struct);
        }

        // If no conversion was found suitable, then just return the object itself
        return value;
    }

    private DataString convertToString(DataObject value) {
        if (value instanceof DataNull) return new DataString(null);
        if (value instanceof DataByte val) return new DataString("" + val);
        if (value instanceof DataShort val) return new DataString("" + val);
        if (value instanceof DataInteger val) return new DataString("" + val);
        if (value instanceof DataLong val) return new DataString("" + val);
        if (value instanceof DataDouble val) return new DataString("" + val);
        if (value instanceof DataFloat val) return new DataString("" + val);
        return new DataString(value.toString());
    }

    private DataObject convertFromString(DataType expected, String value) {
        if (expected == DataNull.DATATYPE && value == null) return DataNull.INSTANCE;
        if (expected == DataByte.DATATYPE) return new DataByte(Byte.parseByte(value));
        if (expected == DataShort.DATATYPE) return new DataShort(Short.parseShort(value));
        if (expected == DataInteger.DATATYPE) return new DataInteger(Integer.parseInt(value));
        if (expected == DataLong.DATATYPE) return new DataLong(Long.parseLong(value));
        if (expected == DataFloat.DATATYPE) return new DataFloat(Float.parseFloat(value));
        if (expected == DataDouble.DATATYPE) return new DataDouble(Double.parseDouble(value));
        return null;
    }

    private DataObject convertList(ListType expected, DataList value) {
        // If the target list type does not have a specific value type, then simply return
        final var valueType = expected.valueType();
        if (valueType == null || valueType == DataType.UNKNOWN) return value;

        // Create a new List with the give value type
        final var result = new DataList(valueType, value.isNull());
        final var valueUserType = new UserType(valueType);
        // Copy all list elements into the new list, possibly making sub-elements compatible
        for (int index = 0; index < value.size(); index++) {
            result.add(convert(valueUserType.notation(), value.get(index), valueUserType));
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
                final var fieldType = new UserType(SCHEMA_MAPPER.fromDataSchema(field.schema()));
                // Convert to that type if necessary
                result.put(entry.getKey(), convert(fieldType.notation(), entry.getValue(), fieldType));
            }
        }

        // Return the Struct with compatible fields
        return result;
    }

    private DataObject convertTuple(TupleType expected, DataTuple value) {
        final var convertedDataObjects = new DataObject[value.size()];
        for (int index = 0; index < value.size(); index++) {
            // If the tuple type contains the notation, then use that notation for conversion, otherwise use the
            // default notation
            final var elementType = expected instanceof UserTupleType targetUserTupleType
                    ? targetUserTupleType.getUserType(index)
                    : new UserType(expected.subType(index));
            convertedDataObjects[index] = convert(DEFAULT_NOTATION, value.get(index), elementType);
        }
        return new DataTuple(convertedDataObjects);
    }
}
