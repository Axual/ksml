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

import io.axual.ksml.data.object.base.Tuple;
import io.axual.ksml.data.object.user.UserBoolean;
import io.axual.ksml.data.object.user.UserByte;
import io.axual.ksml.data.object.user.UserBytes;
import io.axual.ksml.data.object.user.UserDouble;
import io.axual.ksml.data.object.user.UserFloat;
import io.axual.ksml.data.object.user.UserInteger;
import io.axual.ksml.data.object.user.UserList;
import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.object.user.UserNone;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.object.user.UserShort;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.object.user.UserTuple;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class NativeUserObjectMapper implements UserObjectMapper<Object> {

    private DataType inferType(Object value) {
        if (value == null) return UserNone.DATATYPE;
        if (value instanceof Boolean) return UserBoolean.DATATYPE;
        if (value instanceof Byte) return UserByte.DATATYPE;
        if (value instanceof Short) return UserShort.DATATYPE;
        if (value instanceof Integer) return UserInteger.DATATYPE;
        if (value instanceof Long) return UserLong.DATATYPE;
        if (value instanceof Float) return UserFloat.DATATYPE;
        if (value instanceof Double) return UserDouble.DATATYPE;
        if (value instanceof byte[]) return UserBytes.DATATYPE;
        if (value instanceof String) return UserString.DATATYPE;
        return DataType.UNKNOWN;
    }

    @Override
    public UserObject toUserObject(UserType expected, Object value) {
        final String resultNotation = expected != null ? expected.notation() : DEFAULT_NOTATION;
        if (value == null) return new UserNone(resultNotation);
        if (value instanceof Boolean) return new UserBoolean(resultNotation, (Boolean) value);
        if (value instanceof Byte) return new UserByte(resultNotation, (Byte) value);
        if (value instanceof Short) return new UserShort(resultNotation, (Short) value);
        if (value instanceof Integer) return new UserInteger(resultNotation, (Integer) value);
        if (value instanceof Long) return new UserLong(resultNotation, (Long) value);
        if (value instanceof Float) return new UserFloat(resultNotation, (Float) value);
        if (value instanceof Double) return new UserDouble(resultNotation, (Double) value);
        if (value instanceof byte[]) return new UserBytes(resultNotation, (byte[]) value);
        if (value instanceof String) return new UserString(resultNotation, (String) value);
        if (value instanceof List) return listToUserList(resultNotation, (List<?>) value);
        if (value instanceof Map) return mapToUserRecord(resultNotation, (Map<?, ?>) value, null);
        if (value instanceof Tuple) return tupleToUserTuple(resultNotation, (Tuple<?>) value);
        throw new KSMLExecutionException("Can not convert to UserObject: " + value.getClass().getSimpleName());
    }

    public UserList listToUserList(String notation, List<?> list) {
        UserList result = new UserList(notation, list.isEmpty() ? new StaticUserType(DataType.UNKNOWN, notation) : new StaticUserType(inferType(list.get(0)), notation), list.size());
        for (Object element : list) {
            result.add(toUserObject(notation, element));
        }
        return result;
    }

    public UserRecord mapToUserRecord(String notation, Map<?, ?> map, DataSchema schema) {
        UserRecord result = new UserRecord(schema);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(entry.getKey().toString(), toUserObject(notation, entry.getValue()));
        }
        return result;
    }

    public UserTuple tupleToUserTuple(String notation, Tuple<?> tuple) {
        UserObject[] elements = new UserObject[tuple.size()];
        for (var index = 0; index < tuple.size(); index++) {
            elements[index] = toUserObject(notation, tuple.get(index));
        }
        return new UserTuple(notation, elements);
    }

    @Override
    public Object fromUserObject(UserObject value) {
        if (value instanceof UserNone) return ((UserNone) value).value();
        if (value instanceof UserBoolean) return ((UserBoolean) value).value();
        if (value instanceof UserByte) return ((UserByte) value).value();
        if (value instanceof UserShort) return ((UserShort) value).value();
        if (value instanceof UserInteger) return ((UserInteger) value).value();
        if (value instanceof UserLong) return ((UserLong) value).value();
        if (value instanceof UserFloat) return ((UserFloat) value).value();
        if (value instanceof UserDouble) return ((UserDouble) value).value();
        if (value instanceof UserBytes) return ((UserBytes) value).value();
        if (value instanceof UserString) return ((UserString) value).value();
        if (value instanceof UserList) return userListToList((UserList) value);
        if (value instanceof UserRecord) return userRecordToMap((UserRecord) value);
        throw new KSMLExecutionException("Can not convert UserObject to type: " + value.getClass().getSimpleName());
    }

    public List<Object> userListToList(UserList value) {
        List<Object> result = new ArrayList<>();
        for (UserObject element : value) {
            result.add(fromUserObject(element));
        }
        return result;
    }

    public Map<String, Object> userRecordToMap(UserRecord value) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, UserObject> entry : value.entrySet()) {
            result.put(entry.getKey(), fromUserObject(entry.getValue()));
        }
        return result;
    }
}
