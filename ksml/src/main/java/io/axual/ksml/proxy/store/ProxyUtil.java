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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonErrors;
import io.axual.ksml.python.PythonNativeMapper;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

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
    /** Serializes {@code prefixScan} prefixes as UTF-8. */
    static final Serializer<String> PREFIX_SERIALIZER = new StringSerializer();

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
     * Wraps an iterator of a timestamped key-value store for Python. Without timestamps, entries are
     * {@code {key, value}} like on a plain key-value store; with timestamps, they are {@code {key, value, timestamp}}.
     */
    public static Object toPythonTimestamped(KeyValueIterator<?, ? extends ValueAndTimestamp<?>> iterator, boolean withTimestamp) {
        return new StoreIteratorProxy(iterator, entry -> timestampedEntry(entry, withTimestamp));
    }

    private static Object timestampedEntry(KeyValue<?, ?> kv, boolean withTimestamp) {
        final var vat = (ValueAndTimestamp<?>) kv.value;
        final var converted = new Struct<>();
        converted.put(KEY_FIELD, toPython(kv.key));
        converted.put(VALUE_FIELD, toPython(valueOf(vat)));
        if (withTimestamp && vat != null) converted.put(TIMESTAMP_FIELD, toPython(vat.timestamp()));
        return NATIVE_MAPPER.toRealPythonValue(converted);
    }

    /** Returns the value of a timestamped store result, or {@code null} when there is none. */
    public static Object valueOf(ValueAndTimestamp<?> vat) {
        return vat != null ? vat.value() : null;
    }

    /**
     * Converts the {@code putAll} argument: a list or an iterator of (key, value) pairs or dicts with key and value.
     * A bad entry raises TypeError or ValueError with its position, before anything is written. A dict
     * {@code timestamp} field is ignored. Keys and values get the same conversion as in the store's put.
     */
    public static List<KeyValue<Object, Object>> toKeyValueList(Object entries, UnaryOperator<Object> convertKey, UnaryOperator<Object> convertValue) {
        return toEntries(entries, false, convertKey, convertValue).stream().map(e -> KeyValue.pair(e.key(), e.value())).toList();
    }

    /** Like {@link #toKeyValueList}, but uses a dict {@code timestamp} field; other entries get the record timestamp. */
    public static List<KeyValue<Object, ValueAndTimestamp<Object>>> toTimestampedKeyValueList(Object entries, LongSupplier recordTimestamp, UnaryOperator<Object> convertKey, UnaryOperator<Object> convertValue) {
        final var result = new ArrayList<KeyValue<Object, ValueAndTimestamp<Object>>>();
        for (final var e : toEntries(entries, true, convertKey, convertValue)) {
            final long timestamp = e.timestamp() != null ? e.timestamp() : recordTimestamp.getAsLong();
            result.add(KeyValue.pair(e.key(), ValueAndTimestamp.make(e.value(), timestamp)));
        }
        return result;
    }

    private record Entry(Object key, Object value, Long timestamp) {
    }

    private static List<Entry> toEntries(Object entries, boolean readTimestamp, UnaryOperator<Object> convertKey, UnaryOperator<Object> convertValue) {
        final var value = Value.asValue(entries);
        final Value iterator;
        if (value.hasIterator()) iterator = value.getIterator();
        else if (value.isIterator()) iterator = value;
        else throw PythonErrors.typeError("putAll needs a list or an iterator of entries, got " + PythonErrors.typeName(value));

        final var result = new ArrayList<Entry>();
        while (iterator.hasIteratorNextElement()) {
            final var entry = iterator.getIteratorNextElement();
            result.add(toEntry(result.size(), entry, readTimestamp, convertKey, convertValue));
        }
        return result;
    }

    private static Entry toEntry(int position, Value entry, boolean readTimestamp, UnaryOperator<Object> convertKey, UnaryOperator<Object> convertValue) {
        final var name = "putAll entry " + position;
        if (entry.hasArrayElements()) {
            if (entry.getArraySize() != 2)
                throw PythonErrors.valueError(name + " must have 2 elements (key, value), got " + entry.getArraySize());
            return new Entry(toStoreObject(entry.getArrayElement(0), convertKey), toStoreObject(entry.getArrayElement(1), convertValue), null);
        }
        if (entry.hasHashEntries()) {
            if (!entry.hasHashEntry(KEY_FIELD) || !entry.hasHashEntry(VALUE_FIELD))
                throw PythonErrors.valueError(name + " must have \"key\" and \"value\", got " + PythonErrors.typeName(entry));
            return new Entry(
                    toStoreObject(entry.getHashValue(KEY_FIELD), convertKey),
                    toStoreObject(entry.getHashValue(VALUE_FIELD), convertValue),
                    readTimestamp ? toTimestamp(entry, name) : null);
        }
        throw PythonErrors.typeError(name + " must be a (key, value) pair or a dict with \"key\" and \"value\", got " + PythonErrors.typeName(entry));
    }

    /** {@code as(Object.class)} gives the same Java object that GraalVM passes to an {@code Object} parameter of put. */
    private static Object toStoreObject(Value value, UnaryOperator<Object> convert) {
        return convert.apply(value.as(Object.class));
    }

    /** Reads the optional timestamp field of a dict entry. A missing field or {@code None} means "no timestamp". */
    private static Long toTimestamp(Value entry, String name) {
        if (!entry.hasHashEntry(TIMESTAMP_FIELD)) return null;
        final var timestamp = entry.getHashValue(TIMESTAMP_FIELD);
        if (timestamp.isNull()) return null;
        if (timestamp.isNumber() && timestamp.fitsInLong()) return timestamp.asLong();
        throw PythonErrors.typeError(name + " timestamp must be an int, got " + PythonErrors.typeName(timestamp));
    }

    /** Converts the Python argument of {@code prefixScan} to a string prefix. */
    public static String toStringPrefix(Object prefix) {
        if (NATIVE_MAPPER.fromPython(prefix) instanceof String result) return result;
        throw PythonErrors.typeError("prefixScan needs a string prefix, got " + PythonErrors.typeName(prefix));
    }

    /**
     * Wraps a state store outside record processing. A write without a timestamp to a timestamped store then raises a
     * Python {@code ValueError}, because there is no record timestamp to use.
     */
    public static StateStore wrapStateStore(StateStore store) {
        return wrapStateStore(store, (StateStoreDefinition) null);
    }

    /** Like {@link #wrapStateStore(StateStore)}, and converts keys and values to the types the definition declares. */
    public static StateStore wrapStateStore(StateStore store, StateStoreDefinition definition) {
        return wrapStateStore(store, definition, ProxyUtil::noRecordTimestamp);
    }

    private static long noRecordTimestamp() {
        throw PythonErrors.valueError("No record is being processed, so a write to a timestamped store needs a timestamp");
    }

    /** Wraps a state store whose declared types are unknown; see {@link #wrapStateStore(StateStore, StateStoreDefinition, LongSupplier)}. */
    public static StateStore wrapStateStore(StateStore store, LongSupplier recordTimestamp) {
        return wrapStateStore(store, null, recordTimestamp);
    }

    /**
     * Static method for creating proxy wrappers around Kafka Streams state stores.
     * The proxies delegate all operations to the underlying store while providing
     * a controlled interface that can be safely exposed to user code (e.g., Python functions).
     *
     * @param store           the state store to wrap
     * @param definition      the store's definition, which declares its key and value types, or {@code null}
     * @param recordTimestamp the timestamp of the record that is being processed, used by timestamped stores when
     *                        Python writes without a timestamp
     * @return a proxy wrapper around the store, or the original store if no proxy is available
     */
    @SuppressWarnings("unchecked")
    public static StateStore wrapStateStore(StateStore store, StateStoreDefinition definition, LongSupplier recordTimestamp) {
        final var keyType = definition != null ? dataTypeOf(definition.keyType()) : null;
        final var valueType = definition != null ? dataTypeOf(definition.valueType()) : null;
        if (store instanceof VersionedKeyValueStore<?, ?> versionedStore) {
            return new VersionedKeyValueStoreProxy((VersionedKeyValueStore<Object, Object>) versionedStore, keyType, valueType);
        } else if (store instanceof TimestampedKeyValueStore<?, ?> timestampedKeyValueStore) {
            return new TimestampedKeyValueStoreProxy((TimestampedKeyValueStore<Object, Object>) timestampedKeyValueStore, keyType, valueType, recordTimestamp);
        } else if (store instanceof KeyValueStore<?, ?> kvStore) {
            return new KeyValueStoreProxy((KeyValueStore<Object, Object>) kvStore, keyType, valueType);
        } else if (store instanceof SessionStore<?, ?> sessionStore) {
            return new SessionStoreProxy((SessionStore<Object, Object>) sessionStore, keyType, valueType);
        } else if (store instanceof TimestampedWindowStore<?, ?> timestampedWindowStore) {
            return new TimestampedWindowStoreProxy((TimestampedWindowStore<Object, Object>) timestampedWindowStore, keyType, valueType, recordTimestamp);
        } else if (store instanceof WindowStore<?, ?> windowStore) {
            return new WindowStoreProxy((WindowStore<Object, Object>) windowStore, keyType, valueType);
        }
        // Return unwrapped for unknown store types
        return store;
    }

    private static DataType dataTypeOf(UserType type) {
        return type != null ? type.dataType() : null;
    }
}
