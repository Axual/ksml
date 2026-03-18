package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.util.MapUtil;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.proxy.base.AbstractProxy;
import io.axual.ksml.util.ExecutionUtil;
import org.graalvm.polyglot.Value;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PythonNativeMapper {
    public Object fromPython(Object object) {
        return fromPython(null, object);
    }

    public Object fromPython(DataType expected, Object object) {
        if (object instanceof Value value) {
            // If we got a polyglot Value object, then convert it below before letting the remainder be
            // handled by the superclass
            object = polyglotValueToNative(expected, value);
        }
        return object;
    }

    private Object polyglotValueToNative(DataType expected, Value object) {
        if (object.isNull()) return null;
        if (object.isBoolean() && (expected == null || expected == DataBoolean.DATATYPE))
            return object.asBoolean();

        if (object.isNumber()) return polyglotNumberToNative(expected, object);

        if (object.isString()) return object.asString();

        if (object.hasArrayElements()) {
            final var result = polyglotArrayToNative(expected, object);
            if (result != null) return result;
        }

        // By default, try to decode a dict as a struct
        if (object.hasHashEntries()) {
            final var result = polyglotMapToNative(object);
            if (result != null) return result;
        }

        throw new DataException("Can not convert Python dataType to DataObject: "
                + object.getClass().getSimpleName()
                + (expected != null ? ", expected: " + expected : ""));
    }

    private Object polyglotNumberToNative(DataType expected, Value object) {
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

    private Object polyglotArrayToNative(DataType expected, Value object) {
        if (expected == DataBytes.DATATYPE) {
            final var bytes = new byte[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                bytes[index] = (byte) object.getArrayElement(index).asInt();
            }
            return bytes;
        }
        if (expected instanceof TupleType expectedTuple) {
            final var elements = new Object[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                var subType = expectedTuple.subType(index);
                elements[index] = fromPython(subType, object.getArrayElement(index));
            }
            return new Tuple<>(elements);
        }
        if (expected == null || expected == DataType.UNKNOWN || expected instanceof ListType) {
            final var valueType = expected != null && expected != DataType.UNKNOWN ? ((ListType) expected).valueType() : DataType.UNKNOWN;
            final var result = new ArrayList<>();
            for (var index = 0; index < object.getArraySize(); index++) {
                result.add(fromPython(valueType, object.getArrayElement(index)));
            }
            return result;
        }
        return null;
    }

    @Nullable
    private Map<String, Object> polyglotMapToNative(Value object) {
        return ExecutionUtil.tryThis(() -> MapUtil.stringKeys(object.as(Map.class)));
    }

    public Object toPython(Object object) {
        // Copy all KSML proxy objects without further translation or wrapping
        if (object instanceof AbstractProxy value) return value;
        return toPythonValue(object);
    }

    public Value toPythonValue(Object object) {
        return switch (object) {
            // Value remains untranslated
            case Value value -> value;
            // Below we convert all we can to Value types
            case null -> Value.asValue(null);
            case Boolean value -> Value.asValue(value);
            case Byte value -> Value.asValue(value);
            case Short value -> Value.asValue(value);
            case Integer value -> Value.asValue(value);
            case Long value -> Value.asValue(value);
            case Float value -> Value.asValue(value);
            case Double value -> Value.asValue(value);
            case String value -> Value.asValue(value);
            case byte[] value -> {
                // Convert the contained byte array to a list of unsigned bytes (as short)
                final var values = new ArrayList<Short>(value.length);
                for (byte b : value) values.add(b >= 0 ? (short) b : (short) (256 + b));
                yield Value.asValue(new PythonList(values));
            }
            case List<?> value -> Value.asValue(new PythonList(value));
            case Map<?, ?> value -> Value.asValue(new PythonDict(value));
            default ->
                    throw new DataException("Can not convert native value to Python dataType: " + object.getClass().getSimpleName());
        };
    }
}
