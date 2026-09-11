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
import io.axual.ksml.data.util.NumericRangeChecker;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.proxy.base.AbstractProxy;
import io.axual.ksml.util.ExecutionUtil;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PythonNativeMapper {
    private final Map<Context, PythonTypes> typesByContext = new ConcurrentHashMap<>();

    private record PythonTypes(Value dict, Value list, Value none) {
        static PythonTypes of(Context context) {
            return new PythonTypes(
                    context.eval(PythonContext.PYTHON, "dict"),
                    context.eval(PythonContext.PYTHON, "list"),
                    context.eval(PythonContext.PYTHON, "type(None)").execute());
        }
    }

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
        // GraalVM's Value.asByte()/asShort()/asInt()/asLong()/asFloat()/asDouble() throw when the
        // Python value does not fit in the requested Java primitive (e.g. asByte() on Python int 300).
        // The library raises ClassCastException with a generic message ("Invalid or lossy primitive
        // coercion") that does not name the expected KSML type, and on some paths
        // UnsupportedOperationException is raised instead. Catch both and rethrow as DataException
        // so the producer sees a pointed, actionable error.
        try {
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
        } catch (ClassCastException | UnsupportedOperationException e) {
            throw new DataException("Python value " + object + " does not fit in expected type "
                    + (expected != null ? expected : DataLong.DATATYPE), e);
        }
    }

    private Object polyglotArrayToNative(DataType expected, Value object) {
        if (expected == DataBytes.DATATYPE) {
            final var bytes = new byte[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                // Raw byte arrays are conventionally written using either signed (-128..127) or
                // unsigned (0..255) representation in Python, so we accept both. Out-of-range values
                // (e.g. 300) would otherwise silently truncate to a wrong byte (300 → 44).
                final var element = object.getArrayElement(index).asInt();
                if (element < Byte.MIN_VALUE || element > NumericRangeChecker.UNSIGNED_BYTE_MAX_VALUE) {
                    throw new DataException("Python value " + element + " at index " + index
                            + " does not fit in a byte (allowed range: " + (int) Byte.MIN_VALUE + ".."
                            + NumericRangeChecker.UNSIGNED_BYTE_MAX_VALUE + ")");
                }
                bytes[index] = (byte) element;
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

    /** Converts a scalar; returns null otherwise (null included, since None needs the context). */
    private static Value scalarToPythonValue(Object object) {
        return switch (object) {
            case Value value -> value;
            case null -> null;
            case Boolean value -> Value.asValue(value);
            case Byte value -> Value.asValue(value);
            case Short value -> Value.asValue(value);
            case Integer value -> Value.asValue(value);
            case Long value -> Value.asValue(value);
            case Float value -> Value.asValue(value);
            case Double value -> Value.asValue(value);
            case String value -> Value.asValue(value);
            default -> null;
        };
    }

    private static DataException unsupportedType(Object object) {
        return new DataException("Can not convert native value to Python dataType: " + object.getClass().getSimpleName());
    }

    /** Like {@link #toRealPythonValue(Context, Object)}, but looks the current context up lazily. */
    public Value toRealPythonValue(Object object) {
        if (object instanceof AbstractProxy proxy) return Value.asValue(proxy);
        final var scalar = scalarToPythonValue(object);
        if (scalar != null) return scalar;
        return toRealPythonValue(currentContext(), object);
    }

    private static Context currentContext() {
        try {
            return Context.getCurrent();
        } catch (IllegalStateException e) {
            throw new DataException("Converting a value to Python needs an entered Python context", e);
        }
    }

    /** Builds a genuine Python dict/list/None, by filling in Python's own types. Never exposes a Java object to Python. */
    public Value toRealPythonValue(Context context, Object object) {
        if (object instanceof AbstractProxy proxy) return Value.asValue(proxy);
        final var scalar = scalarToPythonValue(object);
        if (scalar != null) return scalar;
        final var types = typesByContext.computeIfAbsent(context, PythonTypes::of);
        if (object == null) return types.none();
        return switch (object) {
            case byte[] value -> {
                final var pyList = types.list().execute();
                for (byte b : value) pyList.invokeMember("append", b >= 0 ? (short) b : (short) (256 + b));
                yield pyList;
            }
            case List<?> value -> {
                final var pyList = types.list().execute();
                for (var element : value) pyList.invokeMember("append", toRealPythonValue(context, element));
                yield pyList;
            }
            case Map<?, ?> value -> {
                final var pyDict = types.dict().execute();
                // KSML map keys are always strings; force it
                value.forEach((k, v) -> pyDict.putHashEntry(String.valueOf(k), toRealPythonValue(context, v)));
                yield pyDict;
            }
            default -> throw unsupportedType(object);
        };
    }
}
