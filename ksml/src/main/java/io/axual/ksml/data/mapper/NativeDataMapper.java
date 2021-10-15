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

import io.axual.ksml.data.object.UserBoolean;
import io.axual.ksml.data.object.UserByte;
import io.axual.ksml.data.object.UserBytes;
import io.axual.ksml.data.object.UserDouble;
import io.axual.ksml.data.object.UserFloat;
import io.axual.ksml.data.object.UserInteger;
import io.axual.ksml.data.object.UserList;
import io.axual.ksml.data.object.UserLong;
import io.axual.ksml.data.object.UserObject;
import io.axual.ksml.data.object.UserRecord;
import io.axual.ksml.data.object.UserShort;
import io.axual.ksml.data.object.UserString;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class NativeDataMapper implements DataMapper<Object> {
    public DataType inferType(Object value) {
        if (value instanceof Boolean) return UserBoolean.TYPE;
        if (value instanceof Byte) return UserByte.TYPE;
        if (value instanceof Short) return UserShort.TYPE;
        if (value instanceof Integer) return UserInteger.TYPE;
        if (value instanceof Long) return UserLong.TYPE;
        if (value instanceof Float) return UserFloat.TYPE;
        if (value instanceof Double) return UserDouble.TYPE;
        if (value instanceof byte[]) return UserBytes.TYPE;
        if (value instanceof String) return UserString.TYPE;
        return DataType.UNKNOWN;
    }

    @Override
    public UserObject toDataObject(UserType expected, Object value) {
        final String resultNotation = expected != null ? expected.notation() : DEFAULT_NOTATION;
        if (value instanceof Boolean) return new UserBoolean(resultNotation, (Boolean) value);
        if (value instanceof Byte) return new UserByte(resultNotation, (Byte) value);
        if (value instanceof Short) return new UserShort(resultNotation, (Short) value);
        if (value instanceof Integer) return new UserInteger(resultNotation, (Integer) value);
        if (value instanceof Long) return new UserLong(resultNotation, (Long) value);
        if (value instanceof Float) return new UserFloat(resultNotation, (Float) value);
        if (value instanceof Double) return new UserDouble(resultNotation, (Double) value);
        if (value instanceof byte[]) return new UserBytes(resultNotation, (byte[]) value);
        if (value instanceof String) return new UserString(resultNotation, (String) value);
        if (value instanceof List) return listToDataList(resultNotation, (List<?>) value);
        if (value instanceof Map) return mapToDataRecord(resultNotation, (Map<?, ?>) value, null);
        throw new KSMLExecutionException("Can not wrap type in DataObject: " + value.getClass().getSimpleName());
    }

    public UserList listToDataList(String notation, List<?> list) {
        UserList result = new UserList(notation, list.isEmpty() ? new StaticUserType(DataType.UNKNOWN, notation) : new StaticUserType(inferType(list.get(0)), notation), list.size());
        for (Object element : list) {
            result.add(toDataObject(notation, element));
        }
        return result;
    }

    public UserRecord mapToDataRecord(String notation, Map<?, ?> map, DataSchema schema) {
        UserRecord result = new UserRecord(schema);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(entry.getKey().toString(), toDataObject(notation, entry.getValue()));
        }
        return result;
    }

    @Override
    public Object fromDataObject(UserObject value) {
        if (value instanceof UserBoolean) return ((UserBoolean) value).value();
        if (value instanceof UserByte) return ((UserByte) value).value();
        if (value instanceof UserShort) return ((UserShort) value).value();
        if (value instanceof UserInteger) return ((UserInteger) value).value();
        if (value instanceof UserLong) return ((UserLong) value).value();
        if (value instanceof UserFloat) return ((UserFloat) value).value();
        if (value instanceof UserDouble) return ((UserDouble) value).value();
        if (value instanceof UserBytes) return ((UserBytes) value).value();
        if (value instanceof UserString) return ((UserString) value).value();
        if (value instanceof UserList) return dataListToList((UserList) value);
        if (value instanceof UserRecord) return dataRecordToMap((UserRecord) value);
        throw new KSMLExecutionException("Can not unwrap DataObject type: " + value.getClass().getSimpleName());
    }

    public List<Object> dataListToList(UserList value) {
        List<Object> result = new ArrayList<>();
        for (UserObject element : value) {
            result.add(fromDataObject(element));
        }
        return result;
    }

    public Map<String, Object> dataRecordToMap(UserRecord value) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, UserObject> entry : value.entrySet()) {
            result.put(entry.getKey(), fromDataObject(entry.getValue()));
        }
        return result;
    }
}
