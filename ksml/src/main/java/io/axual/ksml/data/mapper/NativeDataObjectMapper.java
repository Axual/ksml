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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.object.DataUnion;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.RecordSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.SchemaWriter;

public class NativeDataObjectMapper implements DataObjectMapper<Object> {

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
        if (value instanceof Map<?, ?> val) return new RecordType();
        if (value instanceof Tuple<?> val) return inferTupleType(val);

        return DataType.UNKNOWN;
    }

    private EnumType inferEnumType(Object value) {
        var enumConstants = value.getClass().getEnumConstants();
        var possibleValues = new String[enumConstants.length];
        for (int index = 0; index < possibleValues.length; index++) {
            possibleValues[index] = enumConstants[index].toString();
        }
        return new EnumType(value.getClass().getSimpleName(), possibleValues);
    }

    private ListType inferListType(List<?> list) {
        // Assume the list contains all elements of the same dataType. If not validation will fail
        // later. We infer the valueType by looking at the first element of the list. If the list
        // is empty, then use dataType UNKNOWN.
        if (list.isEmpty()) return new ListType(DataType.UNKNOWN);
        return new ListType(inferType(list.get(0)));
    }

    private RecordType inferRecordType(Map<?, ?> map, DataSchema expected) {
        var schema = inferRecordSchema(map, expected);
        if (schema instanceof RecordSchema recordSchema) return new RecordType(recordSchema);
        if (schema != null)
            throw new KSMLParseException("Map can not be converted to " + schema);
        return new RecordType();
    }

    private DataSchema inferRecordSchema(Map<?, ?> map, DataSchema expected) {
        // Find out the schema of the record by looking at the fields. If there are no fields to
        // override the expected schema, then return the expected schema.
        if (map.containsKey(RECORD_TYPE_FIELD)) {
            var typeName = map.get(RECORD_TYPE_FIELD).toString();
            return SchemaLibrary.getSchema(typeName);
        } else if (map.containsKey(RECORD_SCHEMA_FIELD)) {
            var schemaStr = map.get(RECORD_SCHEMA_FIELD).toString();
            return SchemaWriter.read(schemaStr);
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
        if (value instanceof Map<?, ?> val) return mapToDataRecord(val, null);
        if (value instanceof Tuple<?> val) return tupleToDataTuple(val);
        throw new KSMLExecutionException("Can not convert to UserObject: " + value.getClass().getSimpleName());
    }

    public DataList listToDataList(List<?> list) {
        DataList result = new DataList(list.isEmpty() ? DataType.UNKNOWN : inferType(list.get(0)), list.size());
        for (Object element : list) {
            result.add(toDataObject(element));
        }
        return result;
    }

    public DataObject mapToDataRecord(Map<?, ?> map, DataSchema schema) {
        RecordType type = inferRecordType(map, schema);
        map.remove(RECORD_SCHEMA_FIELD);
        map.remove(RECORD_TYPE_FIELD);
        DataRecord result = new DataRecord(type);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(entry.getKey().toString(), toDataObject(entry.getValue()));
        }
        return result;
    }

    public DataTuple tupleToDataTuple(Tuple<?> tuple) {
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
        if (value instanceof DataRecord val) return dataRecordToMap(val);
        if (value instanceof DataTuple val) return dataTupleToTuple(val);

        if (value instanceof DataUnion val) return val.value();

        throw new KSMLExecutionException("Can not convert UserObject to native dataType: " + value.getClass().getSimpleName());
    }

    public List<Object> dataListToList(DataList value) {
        List<Object> result = new ArrayList<>();
        for (DataObject element : value) {
            result.add(fromDataObject(element));
        }
        return result;
    }

    public Map<String, Object> dataRecordToMap(DataRecord value) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, DataObject> entry : value.entrySet()) {
            result.put(entry.getKey(), fromDataObject(entry.getValue()));
        }
        var schema = value.type().schema();
        if (schema != null) {
            result.put(RECORD_TYPE_FIELD, schema.name());
            result.put(RECORD_SCHEMA_FIELD, schema.toString());
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
