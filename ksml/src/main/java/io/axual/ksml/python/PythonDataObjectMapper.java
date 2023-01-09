package io.axual.ksml.python;

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

import org.graalvm.polyglot.Value;

import java.util.Map;

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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.notation.binary.NativeDataObjectMapper;

public class PythonDataObjectMapper extends NativeDataObjectMapper {
    @Override
    public DataObject toDataObject(DataType expected, Object object) {
        // If we got a Value object, then convert it to native format first
        if (object instanceof Value value) {
            object = valueToNative(expected, value);
        }

        // If we expect a union dataType, then check its possible types, else convert by value.
        if (expected instanceof UnionType unionType)
            return unionToDataObject(unionType, object);

        // Finally convert all native types to DataObjects
        return super.toDataObject(expected, object);
    }

    private DataObject unionToDataObject(UnionType unionType, Object object) {
        for (UserType possibleType : unionType.possibleTypes()) {
            try {
                var result = toDataObject(possibleType.dataType(), object);
                if (result != null) return result;
            } catch (Exception e) {
                // Ignore exception and move to next possible dataType
            }
        }

        throw new KSMLExecutionException("Can not convert Python dataType to Union value: " + object.getClass().getSimpleName());
    }

    private Object valueToNative(DataType expected, Value object) {
        if (object.isNull()) return DataNull.INSTANCE;
        if (object.isBoolean() && (expected == null || expected == DataBoolean.DATATYPE))
            return object.asBoolean();

        if (object.isNumber()) return numberToNative(expected, object);

        if (object.isString()) return object.asString();

        if (object.hasArrayElements()) {
            var result = arrayToNative(expected, object);
            if (result != null) return result;
        }

        // By default, try to decode a dict as a struct
        if (expected == null || expected instanceof MapType) {
            var result = mapToNative(expected, object);
            if (result != null) return result;
        }

        throw new KSMLExecutionException("Can not convert Python dataType to DataObject: " + object.getClass().getSimpleName());
    }

    private Object numberToNative(DataType expected, Value object) {
        if (expected != null) {
            if (expected == DataByte.DATATYPE) return object.asByte();
            if (expected == DataShort.DATATYPE) return object.asShort();
            if (expected == DataInteger.DATATYPE) return object.asInt();
            if (expected == DataLong.DATATYPE) return object.asLong();
            if (expected == DataFloat.DATATYPE) return object.asFloat();
            if (expected == DataDouble.DATATYPE) return object.asDouble();
        }
        // Return a long by default
        return object.asLong();
    }

    private Object arrayToNative(DataType expected, Value object) {
        if (expected == DataBytes.DATATYPE) {
            var bytes = new byte[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                bytes[index] = object.getArrayElement(index).asByte();
            }
            return bytes;
        }
        if (expected instanceof TupleType expectedTuple) {
            var elements = new DataObject[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                var subType = expectedTuple.subType(index);
                elements[index] = toDataObject(subType, object.getArrayElement(index));
            }
            return new DataTuple(elements);
        }
        if (expected == null || expected instanceof ListType) {
            var valueType = expected != null ? ((ListType) expected).valueType() : DataType.UNKNOWN;
            var result = new DataList(valueType);
            for (var index = 0; index < object.getArraySize(); index++) {
                result.add(toDataObject(valueType, object.getArrayElement(index)));
            }
            return result;
        }
        return null;
    }

    private DataObject mapToNative(DataType expected, Value object) {
        // Try to cast the value to a HashMap. If that works, then we received a dict value
        // back from Python.
        try {
            Map<?, ?> map = object.as(Map.class);
            return nativeToDataStruct((Map<String, Object>) map, expected instanceof StructType rec ? rec.schema() : null);
        } catch (Exception e) {
            // Ignore all cast exceptions
        }

        return null;
    }

    @Override
    public Value fromDataObject(DataObject object) {
        if (object instanceof DataNull) return Value.asValue(null);
        if (object instanceof DataBoolean val) return Value.asValue(val.value());
        if (object instanceof DataByte val) return Value.asValue(val.value());
        if (object instanceof DataShort val) return Value.asValue(val.value());
        if (object instanceof DataInteger val) return Value.asValue(val.value());
        if (object instanceof DataLong val) return Value.asValue(val.value());
        if (object instanceof DataFloat val) return Value.asValue(val.value());
        if (object instanceof DataDouble val) return Value.asValue(val.value());
        if (object instanceof DataBytes val) return Value.asValue(val.value());
        if (object instanceof DataString val) return Value.asValue(val.value());
        if (object instanceof DataList val) return Value.asValue(fromDataList(val));
        if (object instanceof DataStruct val) return Value.asValue(fromDataStruct(val));
        throw new KSMLExecutionException("Can not convert DataObject to Python dataType: " + object.getClass().getSimpleName());
    }
}
