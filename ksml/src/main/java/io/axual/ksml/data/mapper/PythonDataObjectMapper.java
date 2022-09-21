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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNone;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.RecordSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.SchemaUtil;

public class PythonDataObjectMapper implements DataObjectMapper<Value> {
    private static final String RECORD_SCHEMA_FIELD = "@schema";
    private static final String RECORD_TYPE_FIELD = "@type";
    private final JsonDataObjectMapper jsonDataMapper = new JsonDataObjectMapper();
    private final NativeDataObjectMapper nativeDataMapper = new NativeDataObjectMapper();
    private final Context context;

    public PythonDataObjectMapper(Context context) {
        this.context = context;
    }

    @Override
    public DataObject toDataObject(DataType expected, Value object) {
        if (object.isNull()) return new DataNone();
        if (object.isBoolean() && (expected == null || expected == DataBoolean.DATATYPE))
            return new DataBoolean(object.asBoolean());

        if (object.isNumber()) {
            return toDataNumber(expected, object);
        }

        if (object.isString()) return new DataString(object.asString());

        if (object.hasArrayElements()) {
            var result = toDataArray(expected, object);
            if (result != null) return result;
        }

        if (expected == null || expected instanceof RecordType) {
            var result = toUserRecord(expected, object);
            if (result != null) return result;
        }

        throw new KSMLExecutionException("Can not convert type to UserObject: " + object.getClass().getSimpleName());
    }

    private DataObject toDataNumber(DataType expected, Value object) {
        if (expected != null) {
            if (expected == DataByte.DATATYPE)
                return new DataByte(object.asByte());
            if (expected == DataShort.DATATYPE)
                return new DataShort(object.asShort());
            if (expected == DataInteger.DATATYPE)
                return new DataInteger(object.asInt());
            if (expected == DataLong.DATATYPE)
                return new DataLong(object.asLong());
            if (expected == DataFloat.DATATYPE)
                return new DataFloat(object.asFloat());
            if (expected == DataDouble.DATATYPE)
                return new DataDouble(object.asDouble());
        }
        // Return a long by default
        return new DataLong(object.asLong());
    }

    private DataObject toDataArray(DataType expected, Value object) {
        if (expected instanceof TupleType expectedTuple) {
            var elements = new DataObject[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                var subType = expectedTuple.subType(index);
                elements[index] = toDataObject(subType, object.getArrayElement(index));
            }
            return new DataTuple(elements);
        }
        if (expected == null || expected instanceof ListType expectedList) {
            var valueType = expected != null ? ((ListType) expected).valueType() : DataType.UNKNOWN;
            var result = new DataList(valueType);
            for (var index = 0; index < object.getArraySize(); index++) {
                result.add(toDataObject(valueType, object.getArrayElement(index)));
            }
            return result;
        }
        return null;
    }

    private DataObject toUserRecord(DataType expected, Value object) {
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
            return nativeDataMapper.mapToDataRecord(map, schema instanceof RecordSchema recordSchema ? recordSchema : null);
        } catch (
                Exception e) {
            // Ignore all cast exceptions
        }

        return null;
    }

    @Override
    public DataObject toDataObject(Value object) {
        throw new KSMLExecutionException("Use PythonDataMapper::toUserObject(value, expectedType)");
    }

    @Override
    public Value fromDataObject(DataObject object) {
        if (object instanceof DataNone) return Value.asValue(((DataNone) object).value());
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
            return context.eval("python", jsonDataMapper.fromDataObject(object));
        }
        throw new KSMLExecutionException("Can not convert UserObject to Python type: " + object.getClass().getSimpleName());
    }
}
