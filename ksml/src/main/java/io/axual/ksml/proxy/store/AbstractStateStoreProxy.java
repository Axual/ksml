package io.axual.ksml.proxy.store;

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
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.proxy.base.AbstractProxy;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonErrors;
import io.axual.ksml.python.PythonNativeMapper;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.graalvm.polyglot.HostAccess;

import java.math.BigInteger;

/**
 * Base class for state store proxy implementations.
 * This class exposes the base state store methods for use in KSML Python code. All methods are
 * forwarded to the delegate, except for query and getPosition, which are not supported yet in any proxy.
 */
public abstract class AbstractStateStoreProxy<T extends StateStore> implements StateStore, AbstractProxy {
    protected static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    protected static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    protected static final NativeDataObjectMapper JAVA_MAPPER = new NativeDataObjectMapper();
    protected final T delegate;
    /** The store's declared key type, or {@code null} when unknown. */
    protected final DataType keyType;
    /** The store's declared value type, or {@code null} when unknown. */
    protected final DataType valueType;

    protected AbstractStateStoreProxy(T delegate) {
        this(delegate, null, null);
    }

    protected AbstractStateStoreProxy(T delegate, DataType keyType, DataType valueType) {
        this.delegate = delegate;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /** Converts a key to the store's declared key type; see {@link #toStoreType}. */
    protected Object toKey(Object key) {
        return toStoreType(keyType, key, "key");
    }

    /** Converts a value to the store's declared value type; see {@link #toStoreType}. */
    protected Object toValue(Object value) {
        return toStoreType(valueType, value, "value");
    }

    /**
     * Converts a Python key or value to the store's declared simple type, so that, for example, a Python int fits a
     * long store whatever its size. Only a value of the same kind converts: an int to any number type, a float to
     * float or double, a str to string, a bool to boolean and bytes to bytes. Another kind raises TypeError, a number
     * out of range ValueError. {@code None}, and keys or values of stores without a declared simple type, are
     * returned unchanged.
     */
    private Object toStoreType(DataType type, Object object, String role) {
        if (!(type instanceof SimpleType simpleType) || type instanceof EnumType || object == null) return object;
        final var container = simpleType.containerClass();
        // The expected type only helps to read Python bytes; for other types it could coerce a value of another kind
        final var value = object instanceof DataObject dataObject
                ? JAVA_MAPPER.fromDataObject(dataObject)
                : NATIVE_MAPPER.fromPython(container == byte[].class ? type : null, object);
        if (value == null) return null;
        final boolean isInteger = isInteger(value);
        final boolean fits;
        if (container == Byte.class || container == Short.class || container == Integer.class || container == Long.class) {
            fits = isInteger;
        } else if (container == Float.class || container == Double.class) {
            fits = isInteger || value instanceof Float || value instanceof Double;
        } else if (container == String.class) {
            fits = value instanceof String;
        } else if (container == Boolean.class) {
            fits = value instanceof Boolean;
        } else if (container == byte[].class) {
            fits = value instanceof byte[];
        } else {
            return object;
        }
        if (!fits)
            throw PythonErrors.typeError(storeError(role) + " must be " + type + ", got " + typeName(value));
        try {
            if (value instanceof BigInteger bigInteger) {
                if (container == Float.class || container == Double.class)
                    return DATA_OBJECT_MAPPER.toDataObject(type, bigInteger.doubleValue());
                return DATA_OBJECT_MAPPER.toDataObject(type, bigInteger.longValueExact());
            }
            return DATA_OBJECT_MAPPER.toDataObject(type, value);
        } catch (ArithmeticException | DataException e) {
            throw PythonErrors.valueError(storeError(role) + " " + typeName(value) + " is out of range for " + type, e);
        }
    }

    /** Whether a value is a Python int: GraalVM passes it as Byte, Short, Integer, Long or BigInteger by its size. */
    protected static boolean isInteger(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Long || value instanceof BigInteger;
    }

    /** The Python type name; a large int arrives as a BigInteger, which Python would call ForeignNumber. */
    protected static String typeName(Object value) {
        return isInteger(value) ? "int" : PythonErrors.typeName(value);
    }

    /** The start of an error message about this store, followed by what is wrong. */
    protected String storeError(String subject) {
        return "Store " + name() + ": " + subject;
    }

    @HostAccess.Export
    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(StateStoreContext stateStoreContext, StateStore root) {
        delegate.init(stateStoreContext, root);
    }

    @HostAccess.Export
    @Override
    // StateStore.flush() is deprecated in Kafka Streams 4.x, but this proxy exposes it to KSML's
    // Python API (@HostAccess.Export) and must keep delegating to the wrapped store for compatibility.
    @SuppressWarnings("deprecation")
    public void flush() {
        delegate.flush();
    }

    @HostAccess.Export
    @Override
    public void close() {
        delegate.close();
    }

    @HostAccess.Export
    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    @HostAccess.Export
    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @HostAccess.Export
    @Override
    public String toString() {
        return getClass().getSimpleName() + " [name=\"" + name() + "\"]";
    }
}
