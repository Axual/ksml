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

import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.object.DataUnion;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.notation.binary.NativeDataObjectMapper;
import io.axual.ksml.notation.json.JsonDataObjectMapper;
import io.axual.ksml.schema.SchemaUtil;

// This DataObjectMapper makes expected data types compatible with the actual data that was
// created. It does so by converting numbers to strings, and vice versa. It can convert complex
// data objects like Enums, Lists and Structs too, recursively going through sub-elements if
// necessary.
public class CompatibilityMapper {
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final JsonDataObjectMapper JSON_MAPPER = new JsonDataObjectMapper();
    private final NotationLibrary notationLibrary;

    public CompatibilityMapper(NotationLibrary notationLibrary) {
        this.notationLibrary = notationLibrary;
    }

    public DataObject toDataObject(UserType expected, DataObject value) {
        // If no conversion is possible or necessary, or the value type is already compatible with
        // the expected type, then just return the value object
        if (expected == null || value == null || expected.dataType().isAssignableFrom(value.type()))
            return value;

        // If we have a notation library, then first run the object through the default parser.
        // This enables amongst others String to XML parsing for automatic interpretation of
        // complex types.
        if (notationLibrary != null) {
            var parser = notationLibrary.getDefaultParser(expected.notation());
            if (parser!=null) {
                var nativeValue = NATIVE_MAPPER.fromDataObject(value);
                var newValue = parser.parse(nativeValue);
                if (newValue != null) value = newValue;
            }
        }

        // Now that we have a parsed type, run it through the compatibility mapper
        return toDataObject(expected.dataType(), value);
    }

    public DataObject toDataObject(DataType expected, DataObject value) {
        // Come up with default values if we convert from Null
        if (value == null || value instanceof DataNull)
            return convertFromNull(expected);

        // Convert from anything to String
        if (expected == DataString.DATATYPE) return convertToString(value);

        // Convert from Strings to anything
        if (value instanceof DataString str) {
            var converted = convertFromString(expected, str.value());
            return converted != null ? converted : value;
        }

        // Convert list without a value type to a list with a specific value type
        if (value instanceof DataList list && expected instanceof ListType listType) {
            return convertList(listType, list);
        }

        // Convert from schemaless structs to a struct with a schema
        if (value instanceof DataStruct struct && expected instanceof StructType structType) {
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
        if (value instanceof DataList || value instanceof DataStruct)
            return new DataString(JSON_MAPPER.fromDataObject(value));
        return new DataString(value.toString());
    }

    private DataObject convertFromString(DataType expected, String value) {
        if (expected == DataNull.DATATYPE) return DataNull.INSTANCE;
        if (expected == DataByte.DATATYPE) return new DataByte(Byte.parseByte(value));
        if (expected == DataShort.DATATYPE) return new DataShort(Short.parseShort(value));
        if (expected == DataInteger.DATATYPE) return new DataInteger(Integer.parseInt(value));
        if (expected == DataLong.DATATYPE) return new DataLong(Long.parseLong(value));
        if (expected == DataFloat.DATATYPE) return new DataFloat(Float.parseFloat(value));
        if (expected == DataDouble.DATATYPE) return new DataDouble(Double.parseDouble(value));
        if (expected instanceof ListType || expected instanceof StructType)
            return toDataObject(expected, JSON_MAPPER.toDataObject(value));
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
            result.add(toDataObject(valueType, element));
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
            result.put(entry.getKey(), toDataObject(fieldType, entry.getValue()));
        }

        // Return the Struct with compatible fields
        return result;
    }
}
