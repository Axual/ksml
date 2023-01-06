package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.axual.ksml.data.object.DataBoolean;
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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.StructSchema;

abstract class BaseDataObjectMapper implements DataObjectMapper {
    private static final NativeDataSchemaMapper SCHEMA_MAPPER = new NativeDataSchemaMapper();
    private static final AttributeComparator COMPARATOR = new AttributeComparator();

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
        // Find out the schema of the struct by looking at the fields. If there are no fields to
        // override the expected schema, then return the expected schema.
        if (map.containsKey(STRUCT_TYPE_FIELD)) {
            var typeName = map.get(STRUCT_TYPE_FIELD).toString();
            return SchemaLibrary.getSchema(typeName);
        } else if (map.containsKey(STRUCT_SCHEMA_FIELD)) {
            var nativeSchema = map.get(STRUCT_SCHEMA_FIELD);
            return SCHEMA_MAPPER.toDataSchema(nativeSchema);
        }
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

    protected DataList toDataList(List<Object> list) {
        DataList result = new DataList(list.isEmpty() ? DataType.UNKNOWN : inferType(list.get(0)), list.size());
        list.forEach(element -> result.add(toDataObject(element)));
        return result;
    }

    protected DataObject toDataStruct(Map<String, Object> map, DataSchema schema) {
        StructType type = inferStructType(map, schema);
        map.remove(STRUCT_SCHEMA_FIELD);
        map.remove(STRUCT_TYPE_FIELD);
        DataStruct result = new DataStruct(type);
        map.forEach((key, value) -> result.put(key, toDataObject(value)));
        return result;
    }

    protected DataTuple toDataTuple(Tuple<Object> tuple) {
        DataObject[] elements = new DataObject[tuple.size()];
        for (var index = 0; index < tuple.size(); index++) {
            elements[index] = toDataObject(tuple.get(index));
        }
        return new DataTuple(elements);
    }

    public List<Object> fromDataList(DataList list) {
        List<Object> result = new ArrayList<>();
        list.forEach(element -> result.add(fromDataObject(element)));
        return result;
    }

    public Map<String, Object> fromDataStruct(DataStruct struct) {
        // To make external representations look nice, we return a sorted map. Sorting is done
        // based on keys, where "normal" keys are always sorted before "meta" keys.
        Map<String, Object> result = new TreeMap<>(COMPARATOR);
        struct.forEach((key, value) -> result.put(key, fromDataObject(value)));

        // Convert schema to native format by encoding it in meta fields
        var schema = struct.type().schema();
        if (schema != null) {
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
