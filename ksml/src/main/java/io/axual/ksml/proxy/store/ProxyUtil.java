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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonDict;
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
import org.apache.kafka.streams.state.WindowStoreIterator;

public class ProxyUtil {
    private static final String KEY_FIELD = "key";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String VALID_TO_FIELD = "validTo";
    private static final String VALUE_FIELD = "value";
    private static final DataObjectFlattener FLATTENER = new DataObjectFlattener();
    private static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();

    private ProxyUtil() {
    }

    public static Object resultFrom(ValueAndTimestamp<Object> vat) {
        if (vat == null) return null;
        final var converted = new Struct<>();
        converted.put(VALUE_FIELD, toPython(vat.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vat.timestamp()));
        return new PythonDict(converted);
    }

    public static Object resultFrom(VersionedRecord<Object> vr) {
        if (vr == null) return null;
        final var converted = new Struct<>();
        converted.put(VALUE_FIELD, toPython(vr.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vr.timestamp()));
        vr.validTo().ifPresent(validTo -> converted.put(VALID_TO_FIELD, toPython(validTo)));
        return new PythonDict(converted);
    }

    public static Object resultFrom(KeyValue<?, ?> kv) {
        if (kv == null) return null;
        final var converted = new Struct<>();
        converted.put(KEY_FIELD, toPython(kv.key));
        converted.put(VALUE_FIELD, toPython(kv.value));
        return new PythonDict(converted);
    }

    public static Object toPython(Object object) {
        if (object == null) return null;
        if (object instanceof WindowStoreIterator<?> wis) return new WindowStoreIteratorProxy(wis);
        if (object instanceof KeyValueIterator<?, ?> kvi) return new KeyValueIteratorProxy(kvi);
        if (object instanceof Windowed<?> windowed)
            object = FLATTENER.toDataObject(windowed);
        if (object instanceof DataObject dataObject)
            return DATA_OBJECT_MAPPER.fromDataObject(dataObject);
        return NATIVE_MAPPER.toPython(object);
    }

    /**
     * Wraps a state store in an appropriate proxy based on its type.
     *
     * @param store the state store to wrap
     * @return a proxy wrapper around the store, or the original store if no proxy is available
     */
    /**
     * Factory class for creating proxy wrappers around Kafka Streams state stores.
     * The proxies delegate all operations to the underlying store while providing
     * a controlled interface that can be safely exposed to user code (e.g., Python functions).
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
