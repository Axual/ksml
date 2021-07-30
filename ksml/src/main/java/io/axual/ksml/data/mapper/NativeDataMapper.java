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
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.data.type.DataType;

public class NativeDataMapper implements DataMapper<Object> {
    public DataType inferType(Object value) {
        if (value instanceof Boolean) return DataBoolean.TYPE;
        if (value instanceof Byte) return DataByte.TYPE;
        if (value instanceof Short) return DataShort.TYPE;
        if (value instanceof Integer) return DataInteger.TYPE;
        if (value instanceof Long) return DataLong.TYPE;
        if (value instanceof Float) return DataFloat.TYPE;
        if (value instanceof Double) return DataDouble.TYPE;
        if (value instanceof byte[]) return DataBytes.TYPE;
        if (value instanceof String) return DataString.TYPE;
        return DataType.UNKNOWN;
    }

    @Override
    public DataObject toDataObject(Object value, DataType expected) {
        if (value instanceof Boolean) return new DataBoolean((Boolean) value);
        if (value instanceof Byte) return new DataByte((Byte) value);
        if (value instanceof Short) return new DataShort((Short) value);
        if (value instanceof Integer) return new DataInteger((Integer) value);
        if (value instanceof Long) return new DataLong((Long) value);
        if (value instanceof Float) return new DataFloat((Float) value);
        if (value instanceof Double) return new DataDouble((Double) value);
        if (value instanceof byte[]) return new DataBytes((byte[]) value);
        if (value instanceof String) return new DataString((String) value);
        if (value instanceof List) return listToDataList((List<?>) value);
        if (value instanceof Map) return mapToDataRecord((Map<?, ?>) value, null);
        throw new KSMLExecutionException("Can not wrap type in DataObject: " + value.getClass().getSimpleName());
    }

    public DataList listToDataList(List<?> list) {
        DataList result = new DataList(list.isEmpty() ? DataType.UNKNOWN : inferType(list.get(0)), list.size());
        for (Object element : list) {
            result.add(toDataObject(element));
        }
        return result;
    }

    public DataRecord mapToDataRecord(Map<?, ?> map, DataSchema schema) {
        DataRecord result = new DataRecord(schema);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(entry.getKey().toString(), toDataObject(entry.getValue()));
        }
        return result;
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataBoolean) return ((DataBoolean) value).value();
        if (value instanceof DataByte) return ((DataByte) value).value();
        if (value instanceof DataShort) return ((DataShort) value).value();
        if (value instanceof DataInteger) return ((DataInteger) value).value();
        if (value instanceof DataLong) return ((DataLong) value).value();
        if (value instanceof DataFloat) return ((DataFloat) value).value();
        if (value instanceof DataDouble) return ((DataDouble) value).value();
        if (value instanceof DataBytes) return ((DataBytes) value).value();
        if (value instanceof DataString) return ((DataString) value).value();
        if (value instanceof DataList) return dataListToList((DataList) value);
        if (value instanceof DataRecord) return dataRecordToMap((DataRecord) value);
        throw new KSMLExecutionException("Can not unwrap DataObject type: " + value.getClass().getSimpleName());
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
        return result;
    }
}
