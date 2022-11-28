package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import io.axual.ksml.data.type.*;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.StructSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class NativeDataObjectMapper implements DataObjectMapper<Object> {
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
            throw new KSMLParseException("Map can not be converted to " + schema);
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

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) return new DataNull();
        if (value instanceof DataObject val) return val;
        if (value instanceof Boolean val) return new DataBoolean(val);
        if (value instanceof Byte val) return new DataByte(val);
        if (value instanceof Short val) return new DataShort(val);
        if (value instanceof Integer val) return new DataInteger(val);
        if (value instanceof Long val) return new DataLong(val);
        if (value instanceof Double val) return new DataDouble(val);
        if (value instanceof Float val) return new DataFloat(val);
        if (value instanceof byte[] val) return new DataBytes(val);
        if (value instanceof String val) return new DataString(val);
        if (value instanceof List<?> val) return listToDataList(val);
        if (value instanceof Map<?, ?> val) return mapToDataStruct(val, null);
        if (value instanceof Tuple<?> val) return tupleToDataTuple(val);
        throw new KSMLExecutionException("Can not convert to DataObject: " + value.getClass().getSimpleName());
    }

    private DataList listToDataList(List<?> list) {
        DataList result = new DataList(list.isEmpty() ? DataType.UNKNOWN : inferType(list.get(0)), list.size());
        for (Object element : list) {
            result.add(toDataObject(element));
        }
        return result;
    }

    protected DataObject mapToDataStruct(Map<?, ?> map, DataSchema schema) {
        StructType type = inferStructType(map, schema);
        map.remove(STRUCT_SCHEMA_FIELD);
        map.remove(STRUCT_TYPE_FIELD);
        DataStruct result = new DataStruct(type);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(entry.getKey().toString(), toDataObject(entry.getValue()));
        }
        return result;
    }

    private DataTuple tupleToDataTuple(Tuple<?> tuple) {
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
        if (value instanceof DataList val) return dataListToList(val);
        if (value instanceof DataStruct val) return dataStructToMap(val);
        if (value instanceof DataTuple val) return dataTupleToTuple(val);

        if (value instanceof DataUnion val) return val.value();

        throw new KSMLExecutionException("Can not convert DataObject to native dataType: " + value.getClass().getSimpleName());
    }

    public List<Object> dataListToList(DataList value) {
        List<Object> result = new ArrayList<>();
        for (DataObject element : value) {
            result.add(fromDataObject(element));
        }
        return result;
    }

    public Map<String, Object> dataStructToMap(DataStruct value) {
        // To make external representations look nice, we return a sorted map. Sorting is done
        // based on keys, where "normal" keys are always sorted before "meta" keys.
        Map<String, Object> result = new TreeMap<>(COMPARATOR);
        for (Map.Entry<String, DataObject> entry : value.entrySet()) {
            result.put(entry.getKey(), fromDataObject(entry.getValue()));
        }
        var schema = value.type().schema();
        if (schema != null) {
            result.put(STRUCT_TYPE_FIELD, schema.name());
            result.put(STRUCT_SCHEMA_FIELD, SCHEMA_MAPPER.fromDataSchema(schema));
        }
        return result;
    }

    public Tuple<Object> dataTupleToTuple(DataTuple value) {
        var elements = new Object[value.size()];
        for (int index = 0; index < value.size(); index++) {
            elements[index] = fromDataObject(value.get(index));
        }

        return new Tuple<>(elements);
    }
}
