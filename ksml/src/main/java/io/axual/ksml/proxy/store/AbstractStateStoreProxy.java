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
import org.graalvm.polyglot.Value;

import java.math.BigInteger;
import java.util.Set;

/**
 * Base class for state store proxy implementations.
 * This class exposes the base state store methods for use in KSML Python code. All methods are
 * forwarded to the delegate, except for query and getPosition, which are not supported yet in any proxy.
 */
public abstract class AbstractStateStoreProxy<T extends StateStore> implements StateStore, AbstractProxy {
    protected static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    protected static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    protected static final NativeDataObjectMapper DATA_OBJECT_TO_JAVA = new NativeDataObjectMapper();
    private static final Set<Class<?>> INTEGER_CONTAINERS = Set.of(Byte.class, Short.class, Integer.class, Long.class);
    private static final Set<Class<?>> FLOAT_CONTAINERS = Set.of(Float.class, Double.class);
    protected final T delegate;
    /** The store's declared key type, or {@code null} when unknown. */
    protected final DataType keyType;
    /** The store's declared value type, or {@code null} when unknown. */
    protected final DataType valueType;

    protected AbstractStateStoreProxy(T delegate, DataType keyType, DataType valueType) {
        this.delegate = delegate;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /** Converts a Python key to the store's declared key type (see {@link #toStoreType}), as a Java value for Kafka. */
    protected Object toKey(Object key) {
        return toKey(key, "key");
    }

    /** Like {@link #toKey(Object)}; {@code role} names the key in error messages, for example "putAll entry 3 key". */
    protected Object toKey(Object key, String role) {
        return NATIVE_MAPPER.fromPython(toStoreType(keyType, key, role));
    }

    /** Like {@link #toKey(Object)}, for a write: Kafka rejects a {@code None} key, so it raises a Python ValueError. */
    protected Object toWriteKey(Object key) {
        return toWriteKey(key, "key");
    }

    /** Like {@link #toWriteKey(Object)}; {@code role} names the key in error messages. */
    protected Object toWriteKey(Object key, String role) {
        if (key == null) throw PythonErrors.valueError(storeError(role) + " must not be None");
        return toKey(key, role);
    }

    /** Converts a Python value to the store's declared value type (see {@link #toStoreType}), as a Java value for Kafka. */
    protected Object toValue(Object value) {
        return toValue(value, "value");
    }

    /** Like {@link #toValue(Object)}; {@code role} names the value in error messages, for example "putAll entry 3 value". */
    protected Object toValue(Object value, String role) {
        return NATIVE_MAPPER.fromPython(toStoreType(valueType, value, role));
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
        final Object value;
        try {
            value = readValue(type, container, object);
        } catch (DataException e) {
            // Only bytes, a list or a tuple for a bytes store is read element by element; there an int is out of range
            if (isByteElements(container, object))
                throw PythonErrors.valueError(storeError(role) + " has an int out of range for " + type + " (-128 to 255)", e);
            throw kindError(role, type, typeName(object));
        } catch (ClassCastException _) {
            throw kindError(role, type, typeName(object));
        }
        if (value == null) return null;
        if (!isConvertible(container)) return object;
        if (!fits(container, value))
            throw kindError(role, type, typeName(object, value));
        try {
            return convert(type, container, value);
        } catch (ArithmeticException | DataException e) {
            throw PythonErrors.valueError(storeError(role) + " " + typeName(object, value) + " is out of range for " + type, e);
        }
    }

    /** Reads a Python key or value, or a DataObject, as a Java value. */
    private static Object readValue(DataType type, Class<?> container, Object object) {
        // The expected type only helps to read Python bytes; for other types it could coerce a value of another kind.
        // GraalVM passes Python bytes as a Java object, so it is read back as a polyglot value to get a byte[].
        if (object instanceof DataObject dataObject) return DATA_OBJECT_TO_JAVA.fromDataObject(dataObject);
        if (isByteElements(container, object)) {
            final var elements = Value.asValue(object);
            // Only ints are bytes; the mapper would read a float or a bool element as an int. toStoreType makes the
            // ClassCastException a TypeError, as for any value of another kind.
            for (long index = 0; index < elements.getArraySize(); index++)
                if (!"int".equals(PythonErrors.typeName(elements.getArrayElement(index))))
                    throw new ClassCastException("bytes element is not an int");
            return NATIVE_MAPPER.fromPython(type, elements);
        }
        return NATIVE_MAPPER.fromPython(object);
    }

    /** Whether a value for a bytes store is read element by element: Python bytes, a list or a tuple. */
    private static boolean isByteElements(Class<?> container, Object object) {
        return container == byte[].class && !(object instanceof DataObject) && Value.asValue(object).hasArrayElements();
    }

    /** Whether {@link #toStoreType} converts values to this container class. */
    private static boolean isConvertible(Class<?> container) {
        return INTEGER_CONTAINERS.contains(container) || FLOAT_CONTAINERS.contains(container)
                || container == String.class || container == Boolean.class || container == byte[].class;
    }

    /** Whether a value is of the same kind as a convertible container class. */
    private static boolean fits(Class<?> container, Object value) {
        if (INTEGER_CONTAINERS.contains(container)) return isInteger(value);
        if (FLOAT_CONTAINERS.contains(container)) return isInteger(value) || value instanceof Float || value instanceof Double;
        if (container == String.class) return value instanceof String;
        if (container == Boolean.class) return value instanceof Boolean;
        return value instanceof byte[];
    }

    /**
     * Converts a fitting value to the declared type; a too large int throws ArithmeticException or DataException, also
     * for a float type, as Python's own {@code float(10**400)} fails.
     */
    private static Object convert(DataType type, Class<?> container, Object value) {
        if (value instanceof BigInteger bigInteger) {
            if (FLOAT_CONTAINERS.contains(container)) {
                final var asDouble = bigInteger.doubleValue();
                if (Double.isInfinite(asDouble)) throw new ArithmeticException("int too large for a float");
                return DATA_OBJECT_MAPPER.toDataObject(type, asDouble);
            }
            return DATA_OBJECT_MAPPER.toDataObject(type, bigInteger.longValueExact());
        }
        return DATA_OBJECT_MAPPER.toDataObject(type, value);
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

    /**
     * The Python type name of a key or value as Python passed it. A {@code DataObject} (from a test runner assert
     * block) is named by its Java value; for Python bytes stores the read value is a polyglot wrapper.
     */
    private static String typeName(Object object, Object value) {
        return typeName(object instanceof DataObject ? value : object);
    }

    /** The start of an error message about this store, followed by what is wrong. */
    protected String storeError(String subject) {
        return "Store " + name() + ": " + subject;
    }

    /** The TypeError for a key or value of another kind than the declared type, for example "must be long, got float". */
    private RuntimeException kindError(String role, DataType type, String actual) {
        return PythonErrors.typeError(storeError(role) + " must be " + type + ", got " + actual);
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
