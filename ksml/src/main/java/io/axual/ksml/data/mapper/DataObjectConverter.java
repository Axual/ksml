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

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.SchemaUtil;
import io.axual.ksml.data.type.*;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.NotationLibrary;

import static io.axual.ksml.data.type.UserType.DEFAULT_NOTATION;

// This DataObjectConverter makes expected data types compatible with the actual data that was
// created. It does so by converting numbers to strings, and vice versa. It can convert complex
// data objects like Enums, Lists and Structs too, recursively going through sub-elements if
// necessary.
public class DataObjectConverter {
    private final NotationLibrary notationLibrary;

    public DataObjectConverter(NotationLibrary notationLibrary) {
        this.notationLibrary = notationLibrary;
    }

    public DataObject convert(String sourceNotation, DataObject value, UserType targetType) {
        return convert(sourceNotation, value, targetType, false);
    }

    private DataObject convert(String sourceNotation, DataObject value, UserType targetType, boolean allowFail) {
        // If no conversion is possible or necessary, or the value type is already compatible with
        // the expected type, then just return the value object
        if (targetType == null || value == null || targetType.dataType().isAssignableFrom(value.type()))
            return value;

        // Perform type conversions recursively, going into complex types if necessary

        // Recurse into union types
        if (targetType.dataType() instanceof UnionType targetUnionType) {
            for (int index = 0; index < targetUnionType.possibleTypes().length; index++) {
                var convertedValue = convert(sourceNotation, value, targetUnionType.possibleTypes()[index], true);
                if (convertedValue != null) return convertedValue;
            }
        }

        // Recurse into lists
        if (targetType.dataType() instanceof ListType targetListType
                && value instanceof DataList valueList) {
            var result = new DataList(targetListType.valueType());
            var expectedValueType = new UserType(targetListType.valueType());
            for (int index = 0; index < valueList.size(); index++) {
                result.add(convert(DEFAULT_NOTATION, valueList.get(index), expectedValueType));
            }
            return result;
        }

        // Recurse into structs
        if (targetType.dataType() instanceof StructType targetStructType
                && value instanceof DataStruct valueStruct) {
            // Don't recurse into structs without a schema, just return those plainly
            if (targetStructType.schema() == null) return value;
            // Recurse and convert struct entries
            DataStruct result = new DataStruct(targetStructType.schema());
            for (var entry : valueStruct.entrySet()) {
                // Determine the new value type
                var field = targetStructType.schema().field(entry.getKey());
                // Only copy if the field exists in the target structure
                if (field != null) {
                    var newValueType = new UserType(SchemaUtil.schemaToDataType(field.schema()));
                    // Convert to that type if necessary
                    result.put(entry.getKey(), convert(DEFAULT_NOTATION, entry.getValue(), newValueType));
                }
            }
            return result;
        }

        // Recurse into tuples
        if (targetType.dataType() instanceof TupleType targetTupleType
                && value instanceof DataTuple valueTuple
                && targetTupleType.subTypeCount() == valueTuple.size()) {
            var convertedDataObjects = new DataObject[valueTuple.size()];
            for (int index = 0; index < valueTuple.size(); index++) {
                // If the tuple type contains the notation, then use that notation for conversion, otherwise use the
                // default notation
                var elementType = targetTupleType instanceof UserTupleType targetUserTupleType
                        ? targetUserTupleType.getUserType(index)
                        : new UserType(targetTupleType.subType(index));
                convertedDataObjects[index] = convert(DEFAULT_NOTATION, valueTuple.get(index), elementType);
            }
            return new DataTuple(convertedDataObjects);
        }

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
        throw FatalError.dataError("Can not convert value to " + targetType + ": " + ExecutionContext.INSTANCE.maskData(value));
    }

    private DataObject applyNotationConverters(String sourceNotation, DataObject value, UserType targetType) {
        // If we have a notation library, then we can check if the notations have converters to translate the value
        if (notationLibrary != null) {
            // If the value is a union, then dig down to its real value
            while (value instanceof DataUnion valueUnion) {
                value = valueUnion.value();
            }

            // First we see if the target notation is able to interpret the source value
            var targetConverter = notationLibrary.converter(targetType.notation());
            if (targetConverter != null) {
                var target = targetConverter.convert(value, targetType);
                if (target != null && targetType.dataType().isAssignableFrom(target.type())) return target;
            }

            // If the target notation was not able to convert, then try the source notation
            var sourceConverter = notationLibrary.converter(sourceNotation);
            if (sourceConverter != null) {
                var target = sourceConverter.convert(value, targetType);
                if (target != null && targetType.dataType().isAssignableFrom(target.type())) return target;
            }
        }

        return value;
    }

    public DataObject convert(DataObject value, DataType targetType) {
        // If we're already compatible with the target type, then return the value itself. This line is here mainly
        // for recursive calls from lists and maps.
        if (targetType.isAssignableFrom(value)) return value;

        // Come up with default values if we convert from Null
        if (value == null || value instanceof DataNull)
            return convertFromNull(targetType);

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

    private DataObject convertFromNull(DataType expected) {
        if (expected == null || expected == DataNull.DATATYPE) return DataNull.INSTANCE;
        if (expected == DataByte.DATATYPE) return new DataByte();
        if (expected == DataShort.DATATYPE) return new DataShort();
        if (expected == DataInteger.DATATYPE) return new DataInteger();
        if (expected == DataLong.DATATYPE) return new DataLong();
        if (expected == DataFloat.DATATYPE) return new DataFloat();
        if (expected == DataDouble.DATATYPE) return new DataDouble();
        if (expected == DataBytes.DATATYPE) return new DataBytes();
        if (expected == DataString.DATATYPE) return new DataString();
        if (expected instanceof ListType listType) return new DataList(listType.valueType());
        if (expected instanceof StructType structType) return new DataStruct(structType.schema());
        if (expected instanceof TupleType tupleType) return createEmptyTuple(tupleType);
        if (expected instanceof UnionType unionType)
            return new DataUnion(unionType, DataNull.INSTANCE);
        throw new KSMLExecutionException("Can not convert NULL to " + expected);
    }

    private DataTuple createEmptyTuple(TupleType tupleType) {
        // Create a tuple with given type using default values for all tuple elements
        var elements = new DataObject[tupleType.subTypeCount()];
        for (int index = 0; index < elements.length; index++)
            elements[index] = convertFromNull(tupleType.subType(index));
        return new DataTuple(elements);
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
        var valueType = expected.valueType();

        // If the target list type does not have a specific value type, then simply return
        if (valueType == null || valueType == DataType.UNKNOWN) return value;

        // Create a new List with the give value type
        var result = new DataList(valueType);

        // Copy all list elements into the new list, possibly making sub-elements compatible
        for (var element : value) {
            result.add(convert(element, valueType));
        }

        // Return the List with made-compatible elements
        return result;
    }

    private DataObject convertStruct(StructType expected, DataStruct value) {
        var schema = expected.schema();
        if (schema == null) return value;

        // Create a new Struct with the given schema type
        var result = new DataStruct(expected.schema());

        // Copy all struct fields into the new struct, possibly making sub-elements compatible
        for (var entry : value.entrySet()) {
            var fieldName = entry.getKey();
            var fieldType = SchemaUtil.schemaToDataType(schema.field(fieldName).schema());
            result.put(entry.getKey(), convert(entry.getValue(), fieldType));
        }

        // Return the Struct with compatible fields
        return result;
    }
}
