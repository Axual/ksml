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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.object.user.UserBoolean;
import io.axual.ksml.data.object.user.UserByte;
import io.axual.ksml.data.object.user.UserBytes;
import io.axual.ksml.data.object.user.UserDouble;
import io.axual.ksml.data.object.user.UserFloat;
import io.axual.ksml.data.object.user.UserInteger;
import io.axual.ksml.data.object.user.UserList;
import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.object.user.UserShort;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.object.user.UserTuple;
import io.axual.ksml.data.type.user.UserListType;
import io.axual.ksml.data.type.user.UserRecordType;
import io.axual.ksml.data.type.user.UserTupleType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.SchemaUtil;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class PythonUserObjectMapper implements UserObjectMapper<Value> {
    private static final String RECORD_SCHEMA_FIELD = "@schema";
    private static final String RECORD_TYPE_FIELD = "@type";
    private final NativeUserObjectMapper nativeDataMapper = new NativeUserObjectMapper();
    private final Context context;

    public PythonUserObjectMapper(Context context) {
        this.context = context;
    }

    public UserObject toUserObject(UserType expected, Value object) {
        final String resultNotation = expected != null ? expected.notation() : DEFAULT_NOTATION;
        if (object.isBoolean() && (expected == null || expected.type() == UserBoolean.TYPE))
            return new UserBoolean(resultNotation, object.asBoolean());

        if (object.isNumber()) {
            if (expected != null) {
                if (expected.type() == UserByte.TYPE)
                    return new UserByte(resultNotation, object.asByte());
                if (expected.type() == UserShort.TYPE)
                    return new UserShort(resultNotation, object.asShort());
                if (expected.type() == UserInteger.TYPE)
                    return new UserInteger(resultNotation, object.asInt());
                if (expected.type() == UserLong.TYPE)
                    return new UserLong(resultNotation, object.asLong());
                if (expected.type() == UserFloat.TYPE)
                    return new UserFloat(resultNotation, object.asFloat());
                if (expected.type() == UserDouble.TYPE)
                    return new UserDouble(resultNotation, object.asDouble());
            }
            // Return a long by default
            return new UserLong(resultNotation, object.asLong());
        }

        if (object.isString()) return new UserString(resultNotation, object.asString());

        if (object.hasArrayElements()) {
            if (expected instanceof UserTupleType) {
                var elements = new UserObject[(int) object.getArraySize()];
                for (var index = 0; index < object.getArraySize(); index++) {
                    var subType = ((UserTupleType) expected).subType(index);
                    elements[index] = toUserObject(subType, object.getArrayElement(index));
                }
                return new UserTuple(resultNotation, elements);
            }
            if (expected == null || expected instanceof UserListType) {
                var valueType = expected != null ? ((UserListType) expected).valueType() : UserType.UNKNOWN;
                var result = new UserList(resultNotation, valueType);
                for (var index = 0; index < object.getArraySize(); index++) {
                    result.add(toUserObject(valueType, object.getArrayElement(index)));
                }
                return result;
            }
        }

        if (expected == null || expected instanceof UserRecordType) {
            // Try to cash the value to a HashMap. If that works, then we received a dict value
            // back from Python.
            try {
                HashMap<?, ?> map = object.as(HashMap.class);
                final DataSchema schema;
                if (map.containsKey(RECORD_TYPE_FIELD)) {
                    var typeName = map.get(RECORD_TYPE_FIELD).toString();
                    schema = SchemaLibrary.getSchema(typeName);
                } else if (map.containsKey(RECORD_SCHEMA_FIELD)) {
                    var schemaStr = map.get(RECORD_SCHEMA_FIELD).toString();
                    schema = SchemaUtil.parse(schemaStr);
                } else if (expected != null) {
                    schema = ((UserRecordType) expected).schema();
                } else {
                    schema = null;
                }
                map.remove(RECORD_TYPE_FIELD);
                map.remove(RECORD_SCHEMA_FIELD);
                return nativeDataMapper.mapToUserRecord(resultNotation, map, schema);
            } catch (Exception e) {
                // Ignore all cast exceptions
            }
        }

        throw new KSMLExecutionException("Can not wrap type in UserObject: " + object.getClass().getSimpleName());
    }

    @Override
    public UserObject toUserObject(String notation, Value object) {
        throw new KSMLExecutionException("Use PythonDataMapper::toUserObject(value, expectedType)");
    }

    @Override
    public Value fromUserObject(UserObject object) {
        if (object instanceof UserBoolean) return Value.asValue(((UserBoolean) object).value());
        if (object instanceof UserByte) return Value.asValue(((UserByte) object).value());
        if (object instanceof UserShort) return Value.asValue(((UserShort) object).value());
        if (object instanceof UserInteger) return Value.asValue(((UserInteger) object).value());
        if (object instanceof UserLong) return Value.asValue(((UserLong) object).value());
        if (object instanceof UserFloat) return Value.asValue(((UserFloat) object).value());
        if (object instanceof UserDouble) return Value.asValue(((UserDouble) object).value());
        if (object instanceof UserBytes) return Value.asValue(((UserBytes) object).value());
        if (object instanceof UserString) return Value.asValue(((UserString) object).value());
        if (object instanceof UserList)
            return Value.asValue(nativeDataMapper.userListToList((UserList) object));
        if (object instanceof UserRecord) {
            return context.eval("python", userRecordToJsonString((UserRecord) object));
        }
        throw new KSMLExecutionException("Can not unwrap UserObject type: " + object.getClass().getSimpleName());
    }

    private String userRecordToJsonString(UserRecord object) {
        var builder = new StringBuilder("{");
        var first = true;
        for (Map.Entry<String, UserObject> entry : object.entrySet()) {
            if (!first) builder.append(",");
            builder.append("\"").append(entry.getKey()).append("\":");
            if (entry.getValue() instanceof UserString) builder.append("\"");
            builder.append(entry.getValue().toString());
            if (entry.getValue() instanceof UserString) builder.append("\"");
            first = false;
        }
        if (object.type.schema() != null) {
            if (!first) builder.append(",");
            builder.append("\"").append(RECORD_TYPE_FIELD).append("\":\"").append(object.type.schema().name()).append("\"");
            builder.append(",");
            var schemaString = object.type.schema().toString().replace("\"", "\\\"");
            builder.append("\"").append(RECORD_SCHEMA_FIELD).append("\":\"").append(schemaString).append("\"");
        }
        return builder.append("}").toString();
    }
}
