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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonErrors;
import io.axual.ksml.python.PythonNativeMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for creating proxy objects and converted values returned to Python
 */
public class ProxyUtil {
    private static final String KEY_FIELD = "key";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String VALID_TO_FIELD = "validTo";
    private static final String VALUE_FIELD = "value";
    private static final DataObjectFlattener FLATTENER = new DataObjectFlattener();
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    private static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);

    private ProxyUtil() {
    }

    /**
     * Converts a value for Python. Always builds a real dict/list, so {@code copy.deepcopy()} works.
     *
     * @param object the Object to convert to polyglot / Python
     * @return a proxy object, a genuine Python value, or another wrapper that Python can directly use
     */
    public static Object toPython(Object object) {
        if (object == null) return null;
        if (object instanceof ValueAndTimestamp<?> vat) return resultFrom(vat);
        if (object instanceof VersionedRecord<?> vr) return resultFrom(vr);
        if (object instanceof KeyValue<?, ?> kv) return resultFrom(kv);
        if (object instanceof KeyValueIterator<?, ?> kvi) return new StoreIteratorProxy(kvi);

        // Normal processing, ensuring flattened data structures
        if (object instanceof Windowed<?> windowed)
            object = FLATTENER.toDataObject(windowed);
        if (object instanceof DataObject dataObject)
            return DATA_OBJECT_MAPPER.fromDataObject(dataObject);
        return NATIVE_MAPPER.toRealPythonValue(object);
    }

    /** Converts a ValueAndTimestamp to a Python value. */
    private static Object resultFrom(ValueAndTimestamp<?> vat) {
        if (vat == null) return null;
        final var converted = new Struct<>();
        converted.put(VALUE_FIELD, toPython(vat.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vat.timestamp()));
        return NATIVE_MAPPER.toRealPythonValue(converted);
    }

    /** Converts a VersionedRecord to a Python value. */
    private static Object resultFrom(VersionedRecord<?> vr) {
        if (vr == null) return null;
        final var converted = new Struct<>();
        converted.put(VALUE_FIELD, toPython(vr.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vr.timestamp()));
        vr.validTo().ifPresent(validTo -> converted.put(VALID_TO_FIELD, toPython(validTo)));
        return NATIVE_MAPPER.toRealPythonValue(converted);
    }

    /** Converts a KeyValue to a Python value. */
    private static Object resultFrom(KeyValue<?, ?> kv) {
        if (kv == null) return null;
        final var converted = new Struct<>();
        converted.put(KEY_FIELD, toPython(kv.key));
        converted.put(VALUE_FIELD, toPython(kv.value));
        return NATIVE_MAPPER.toRealPythonValue(converted);
    }

    /**
     * Converts the Python argument of {@code putAll} to Kafka entries. The argument is a list or an iterator of
     * (key, value) pairs or dicts with key and value. A bad entry raises a Python {@code TypeError} or
     * {@code ValueError} that names its position, before anything is written.
     */
    public static List<KeyValue<Object, Object>> toKeyValueList(Object entries) {
        final var value = Value.asValue(entries);
        final Value iterator;
        if (value.hasHashEntries())
            throw PythonErrors.typeError("putAll needs a list or an iterator of entries; for a dict use putAll(d.items())");
        if (value.hasIterator()) iterator = value.getIterator();
        else if (value.isIterator()) iterator = value;
        else throw PythonErrors.typeError("putAll needs a list or an iterator of entries, got: " + value);

        final var result = new ArrayList<KeyValue<Object, Object>>();
        while (iterator.hasIteratorNextElement()) {
            final var entry = iterator.getIteratorNextElement();
            result.add(toKeyValue(result.size(), entry));
        }
        return result;
    }

    private static KeyValue<Object, Object> toKeyValue(int position, Value entry) {
        final var name = "putAll entry " + position;
        if (entry.hasArrayElements()) {
            if (entry.getArraySize() != 2)
                throw PythonErrors.valueError(name + " must have 2 elements (key, value), got " + entry.getArraySize() + ": " + entry);
            return KeyValue.pair(
                    fromPython(entry.getArrayElement(0), name + " key"),
                    fromPython(entry.getArrayElement(1), name + " value"));
        }
        if (entry.hasHashEntries()) {
            if (!entry.hasHashEntry(KEY_FIELD) || !entry.hasHashEntry(VALUE_FIELD))
                throw PythonErrors.valueError(name + " must have \"key\" and \"value\", got: " + entry);
            return KeyValue.pair(
                    fromPython(entry.getHashValue(KEY_FIELD), name + " key"),
                    fromPython(entry.getHashValue(VALUE_FIELD), name + " value"));
        }
        throw PythonErrors.typeError(name + " must be a (key, value) pair or a dict with \"key\" and \"value\", got: " + entry);
    }

    /** Converts the Python argument of {@code prefixScan} to a string prefix. */
    public static String toStringPrefix(Object prefix) {
        if (fromPython(prefix, "prefixScan prefix") instanceof String result) return result;
        throw PythonErrors.typeError("prefixScan needs a string prefix, got: " + prefix);
    }

    /**
     * Converts a Python argument to a Java value. A value that cannot be converted raises a Python
     * {@code ValueError}, because it is a caller error.
     */
    public static Object fromPython(Object object, String name) {
        try {
            return NATIVE_MAPPER.fromPython(object);
        } catch (DataException e) {
            throw PythonErrors.valueError(name + ": " + e.getMessage(), e);
        }
    }

    /**
     * Static method for creating proxy wrappers around Kafka Streams state stores.
     * The proxies delegate all operations to the underlying store while providing
     * a controlled interface that can be safely exposed to user code (e.g., Python functions).
     *
     * @param store the state store to wrap
     * @return a proxy wrapper around the store, or the original store if no proxy is available
     */
    @SuppressWarnings("unchecked")
    public static StateStore wrapStateStore(StateStore store) {
        if (store instanceof VersionedKeyValueStore<?, ?> versionedStore) {
            return new VersionedKeyValueStoreProxy((VersionedKeyValueStore<Object, Object>) versionedStore);
        } else if (store instanceof TimestampedKeyValueStore<?, ?> timestampedKeyValueStore) {
            return new TimestampedKeyValueStoreProxy((TimestampedKeyValueStore<Object, Object>) timestampedKeyValueStore);
        } else if (store instanceof KeyValueStore<?, ?> kvStore) {
            return new KeyValueStoreProxy((KeyValueStore<Object, Object>) kvStore);
        } else if (store instanceof SessionStore<?, ?> sessionStore) {
            return new SessionStoreProxy((SessionStore<Object, Object>) sessionStore);
        } else if (store instanceof TimestampedWindowStore<?, ?> timestampedWindowStore) {
            return new TimestampedWindowStoreProxy((TimestampedWindowStore<Object, Object>) timestampedWindowStore);
        } else if (store instanceof WindowStore<?, ?> windowStore) {
            return new WindowStoreProxy((WindowStore<Object, Object>) windowStore);
        }
        // Return unwrapped for unknown store types
        return store;
    }
}
