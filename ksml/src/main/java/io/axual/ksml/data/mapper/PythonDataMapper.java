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
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataListType;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.SchemaUtil;

public class PythonDataMapper implements DataMapper<Value> {
    private static final String RECORD_SCHEMA_FIELD = "@schema";
    private static final String RECORD_TYPE_FIELD = "@type";
    private final NativeDataMapper nativeDataMapper = new NativeDataMapper();
    private final Context context;

    public PythonDataMapper(Context context) {
        this.context = context;
    }

    public DataObject toDataObject(Value object, DataType expected) {
        if (object.isBoolean() && (expected == null || expected == DataBoolean.TYPE))
            return new DataBoolean(object.asBoolean());

        if (object.isNumber()) {
            if (expected == DataByte.TYPE) return new DataByte(object.asByte());
            if (expected == DataShort.TYPE) return new DataShort(object.asShort());
            if (expected == DataInteger.TYPE) return new DataInteger(object.asInt());
            if (expected == DataLong.TYPE) return new DataLong(object.asLong());
            if (expected == DataFloat.TYPE) return new DataFloat(object.asFloat());
            if (expected == DataDouble.TYPE) return new DataDouble(object.asDouble());
            // Return a long by default
            return new DataLong(object.asLong());
        }

        if (object.isString()) return new DataString(object.asString());

        if (object.hasArrayElements()) {
            if (expected instanceof TupleType) {
                var elements = new DataObject[(int) object.getArraySize()];
                for (var index = 0; index < object.getArraySize(); index++) {
                    var subtype = ((TupleType) expected).subTypes[index];
                    elements[index] = toDataObject(object.getArrayElement(index), subtype);
                }
                return new DataTuple(elements);
            }
            if (expected == null || expected instanceof DataListType) {
                var valueType = expected != null ? ((DataListType) expected).valueType() : DataType.UNKNOWN;
                var result = new DataList(valueType);
                for (var index = 0; index < object.getArraySize(); index++) {
                    result.add(toDataObject(object.getArrayElement(index), valueType));
                }
                return result;
            }
        }

        if (expected == null || expected instanceof RecordType) {
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
                    schema = ((RecordType) expected).schema();
                } else {
                    schema = null;
                }
                map.remove(RECORD_TYPE_FIELD);
                map.remove(RECORD_SCHEMA_FIELD);
                return nativeDataMapper.mapToDataRecord(map, schema);
            } catch (Exception e) {
                // Ignore all cast exceptions
            }
        }

        throw new KSMLExecutionException("Can not wrap type in DataObject: " + object.getClass().getSimpleName());
    }

    @Override
    public DataObject toDataObject(Value object) {
        throw new KSMLExecutionException("Use PythonDataMapper::toDataObject(value, expectedType)");
    }

    @Override
    public Value fromDataObject(DataObject object) {
        if (object instanceof DataBoolean) return Value.asValue(((DataBoolean) object).value());
        if (object instanceof DataByte) return Value.asValue(((DataByte) object).value());
        if (object instanceof DataShort) return Value.asValue(((DataShort) object).value());
        if (object instanceof DataInteger) return Value.asValue(((DataInteger) object).value());
        if (object instanceof DataLong) return Value.asValue(((DataLong) object).value());
        if (object instanceof DataFloat) return Value.asValue(((DataFloat) object).value());
        if (object instanceof DataDouble) return Value.asValue(((DataDouble) object).value());
        if (object instanceof DataBytes) return Value.asValue(((DataBytes) object).value());
        if (object instanceof DataString) return Value.asValue(((DataString) object).value());
        if (object instanceof DataList)
            return Value.asValue(nativeDataMapper.dataListToList((DataList) object));
        if (object instanceof DataRecord) {
            return context.eval("python", recordToString((DataRecord) object));
        }
        throw new KSMLExecutionException("Can not unwrap DataObject type: " + object.getClass().getSimpleName());
    }

    private String recordToString(DataRecord object) {
        var builder = new StringBuilder("{");
        var first = true;
        for (Map.Entry<String, DataObject> entry : object.entrySet()) {
            if (!first) builder.append(",");
            builder.append("\"").append(entry.getKey()).append("\":");
            if (entry.getValue() instanceof DataString) builder.append("\"");
            builder.append(entry.getValue().toString());
            if (entry.getValue() instanceof DataString) builder.append("\"");
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
