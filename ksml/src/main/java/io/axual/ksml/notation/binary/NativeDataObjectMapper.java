package io.axual.ksml.notation.binary;

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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataEnum;
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
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.data.schema.SchemaUtil;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// Maps DataObjects to/from native Java structures
public class NativeDataObjectMapper implements DataObjectMapper<Object> {
    public static final String STRUCT_SCHEMA_FIELD = DataStruct.META_ATTRIBUTE_CHAR + "schema";
    public static final String STRUCT_TYPE_FIELD = DataStruct.META_ATTRIBUTE_CHAR + "type";
    private static final NativeDataSchemaMapper SCHEMA_MAPPER = new NativeDataSchemaMapper();
    private boolean includeTypeInfo = true;

    public void setIncludeTypeInfo(boolean includeTypeInfo) {
        this.includeTypeInfo = includeTypeInfo;
    }

    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) return DataNull.INSTANCE;
        if (value instanceof DataObject val) return val;
        if (value instanceof Boolean val) return new DataBoolean(val);
        if (value instanceof Byte val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val);
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataByte(val);
        }
        if (value instanceof Short val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val);
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataShort(val);
        }
        if (value instanceof Integer val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val);
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataInteger(val);
        }
        if (value instanceof Long val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val);
            return new DataLong(val);
        }
        if (value instanceof Double val) {
            if (expected == DataDouble.DATATYPE) return new DataDouble(val);
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            return new DataDouble(val);
        }
        if (value instanceof Float val) {
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
            if (expected == DataFloat.DATATYPE) return new DataFloat(val);
            return new DataFloat(val);
        }
        if (value instanceof byte[] val) return new DataBytes(val);
        if (value instanceof String val) return new DataString(val);
        if (value instanceof List<?> val)
            return nativeToDataList((List<Object>) val, expected instanceof ListType expectedList ? expectedList.valueType() : DataType.UNKNOWN);
        if (value instanceof Map<?, ?> val)
            return nativeToDataStruct((Map<String, Object>) val, expected instanceof StructType expectedStruct ? expectedStruct.schema() : null);
        if (value instanceof Tuple<?> val) return toDataTuple((Tuple<Object>) val);
        throw new KSMLExecutionException("Can not convert to DataObject: " + value.getClass().getSimpleName());
    }

    private DataType inferType(Object value) {
        if (value == null) return DataNull.DATATYPE;
        if (value instanceof Boolean) return DataBoolean.DATATYPE;

        if (value instanceof Byte) return DataByte.DATATYPE;
        if (value instanceof Short) return DataShort.DATATYPE;
        if (value instanceof Integer) return DataInteger.DATATYPE;
        if (value instanceof Long) return DataLong.DATATYPE;

        if (value instanceof Double) return DataDouble.DATATYPE;
        if (value instanceof Float) return DataFloat.DATATYPE;

        if (value instanceof byte[]) return DataBytes.DATATYPE;

        if (value instanceof String) return DataString.DATATYPE;

        if (value.getClass().isEnum()) return inferEnumType(value);
        if (value instanceof List<?> val) return inferListType(val);
        if (value instanceof Map<?, ?> val) return inferStructType(val);
        if (value instanceof Tuple<?> val) return inferTupleType(val);

        return DataType.UNKNOWN;
    }

    private EnumType inferEnumType(Object value) {
        var enumConstants = value.getClass().getEnumConstants();
        var symbols = new String[enumConstants.length];
        for (int index = 0; index < symbols.length; index++) {
            symbols[index] = enumConstants[index].toString();
        }
        return new EnumType(symbols);
    }

    private ListType inferListType(List<?> list) {
        // Assume the list contains all elements of the same dataType. If not validation will fail
        // later. We infer the valueType by looking at the first element of the list. If the list
        // is empty, then use dataType UNKNOWN.
        if (list.isEmpty()) return new ListType(DataType.UNKNOWN);
        return new ListType(inferType(list.get(0)));
    }

    private StructType inferStructType(Map<?, ?> map) {
        return inferStructType(map, null);
    }

    private StructType inferStructType(Map<?, ?> map, DataSchema expected) {
        var schema = inferStructSchema(map, expected);
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
        if (schema != null)
            throw new KSMLDataException("Map can not be converted to " + schema);
        return new StructType();
    }

    private DataSchema inferStructSchema(Map<?, ?> map, DataSchema expected) {
        // Find out the schema of the struct by looking at the fields. If there are no meta fields
        // to override the expected schema, then return the expected schema.

        // The "@schema" field overrides the entire schema library. If this field is filled, then
        // we assume the entire schema is contained within the map itself. Therefore we do not
        // consult the schema library, but instead directly decode the schema from the field.
        if (map.containsKey(STRUCT_SCHEMA_FIELD)) {
            var nativeSchema = map.get(STRUCT_SCHEMA_FIELD);
            return SCHEMA_MAPPER.toDataSchema("dummy", nativeSchema);
        }

        // The "@type" field indicates a type that is assumed to be contained in the schema
        // library. If this field is set, then look up the schema by name in the library.
        if (map.containsKey(STRUCT_TYPE_FIELD)) {
            var typeName = map.get(STRUCT_TYPE_FIELD).toString();
            return SchemaLibrary.getSchema(typeName, false);
        }

        // No fields found to override the expected schema, so simply return that.
        return expected;
    }

    private DataType inferTupleType(Tuple<?> tuple) {
        // Infer all subtypes
        DataType[] subTypes = new DataType[tuple.size()];
        for (int index = 0; index < tuple.size(); index++) {
            subTypes[index] = inferType(tuple.get(index));
        }
        return new TupleType(subTypes);
    }

    protected DataList nativeToDataList(List<Object> list, DataType valueType) {
        DataList result = new DataList(valueType);
        list.forEach(element -> result.add(toDataObject(valueType, element)));
        return result;
    }

    protected DataObject nativeToDataStruct(Map<String, Object> map, DataSchema schema) {
        var type = inferStructType(map, schema);
        map.remove(STRUCT_SCHEMA_FIELD);
        map.remove(STRUCT_TYPE_FIELD);
        DataStruct result = new DataStruct(type.schema());
        map.forEach((key, value) -> {
            var field = type.schema() != null ? type.schema().field(key) : null;
            var fieldSchema = field != null ? field.schema() : null;
            var subType = SchemaUtil.schemaToDataType(fieldSchema);
            result.put(key, toDataObject(subType, value));
        });
        return result;
    }

    protected DataTuple toDataTuple(Tuple<Object> tuple) {
        DataObject[] elements = new DataObject[tuple.size()];
        for (var index = 0; index < tuple.size(); index++) {
            elements[index] = toDataObject(tuple.get(index));
        }
        return new DataTuple(elements);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull val) return val.value();

        if (value instanceof DataBoolean val) return val.value();

        if (value instanceof DataByte val) return val.value();
        if (value instanceof DataShort val) return val.value();
        if (value instanceof DataInteger val) return val.value();
        if (value instanceof DataLong val) return val.value();

        if (value instanceof DataDouble val) return val.value();
        if (value instanceof DataFloat val) return val.value();

        if (value instanceof DataBytes val) return val.value();

        if (value instanceof DataString val) return val.value();

        if (value instanceof DataEnum val) return val.value();
        if (value instanceof DataList val) return fromDataList(val);
        if (value instanceof DataStruct val) return fromDataStruct(val);
        if (value instanceof DataTuple val) return fromDataTuple(val);

        if (value instanceof DataUnion val) return val.value();

        throw new KSMLExecutionException("Can not convert DataObject to native dataType: " + value.getClass().getSimpleName());
    }

    public List<Object> fromDataList(DataList list) {
        List<Object> result = new ArrayList<>();
        list.forEach(element -> result.add(fromDataObject(element)));
        return result;
    }

    public Map<String, Object> fromDataStruct(DataStruct struct) {
        Map<String, Object> result = new TreeMap<>(DataStruct.COMPARATOR);
        struct.forEach((key, value) -> result.put(key, fromDataObject(value)));

        // Convert schema to native format by encoding it in meta fields
        var schema = struct.type().schema();
        if (schema != null && includeTypeInfo) {
            result.put(STRUCT_TYPE_FIELD, schema.name());
            result.put(STRUCT_SCHEMA_FIELD, SCHEMA_MAPPER.fromDataSchema(schema));
        }

        // Return the native representation as Map
        return result;
    }

    public Tuple<Object> fromDataTuple(DataTuple value) {
        var elements = new Object[value.size()];
        for (int index = 0; index < value.size(); index++) {
            elements[index] = fromDataObject(value.get(index));
        }

        return new Tuple<>(elements);
    }
}
