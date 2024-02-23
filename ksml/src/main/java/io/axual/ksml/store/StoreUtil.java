package io.axual.ksml.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.serde.StoreSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;

public class StoreUtil {
    private StoreUtil() {
    }

    public static KeyValueBytesStoreSupplier getStoreSupplier(KeyValueStateStoreDefinition store) {
        return store.persistent()
                ? (store.versioned()
                ? Stores.persistentVersionedKeyValueStore(store.name(), store.historyRetention(), store.segmentInterval())
                : (store.timestamped()
                ? Stores.persistentTimestampedKeyValueStore(store.name())
                : Stores.persistentKeyValueStore(store.name())))
                : Stores.inMemoryKeyValueStore(store.name());
    }

    public static StoreBuilder<?> getStoreBuilder(KeyValueStateStoreDefinition store) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        StoreBuilder<?> storeBuilder;
        if (store.persistent()) {
            if (store.versioned()) {
                final var supplier = Stores.persistentVersionedKeyValueStore(store.name(), store.historyRetention(), store.segmentInterval());
                storeBuilder = Stores.versionedKeyValueStoreBuilder(supplier, keyType.serde(), valueType.serde());
            } else {
                final var supplier = store.timestamped()
                        ? Stores.persistentTimestampedKeyValueStore(store.name())
                        : Stores.persistentKeyValueStore(store.name());
                storeBuilder = Stores.keyValueStoreBuilder(supplier, keyType.serde(), valueType.serde());
            }
        } else {
            final var supplier = Stores.inMemoryKeyValueStore(store.name());
            storeBuilder = Stores.keyValueStoreBuilder(supplier, keyType.serde(), valueType.serde());
        }
        storeBuilder = store.caching() ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    public static SessionBytesStoreSupplier getStoreSupplier(SessionStateStoreDefinition store) {
        return store.persistent()
                ? Stores.persistentSessionStore(store.name(), store.retention())
                : Stores.inMemorySessionStore(store.name(), store.retention());
    }

    public static StoreBuilder<?> getStoreBuilder(SessionStateStoreDefinition store) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        final var supplier = store.persistent()
                ? Stores.persistentSessionStore(store.name(), store.retention())
                : Stores.inMemorySessionStore(store.name(), store.retention());
        var storeBuilder = Stores.sessionStoreBuilder(supplier, keyType.serde(), valueType.serde());
        storeBuilder = store.caching() ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    public static WindowBytesStoreSupplier getStoreSupplier(WindowStateStoreDefinition store) {
        return store.persistent()
                ? (store.timestamped()
                ? Stores.persistentTimestampedWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates())
                : Stores.persistentWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates()))
                : Stores.inMemoryWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
    }

    public static StoreBuilder<?> getStoreBuilder(WindowStateStoreDefinition store) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        var supplier = store.persistent()
                ? (store.timestamped()
                ? Stores.persistentTimestampedWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates())
                : Stores.persistentWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates()))
                : Stores.inMemoryWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
        var storeBuilder = Stores.windowStoreBuilder(supplier, keyType.serde(), valueType.serde());
        storeBuilder = store.caching() ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    public record MaterializedStore<V, S extends StateStore>(Materialized<Object, V, S> materialized,
                                                             Serde<Object> keySerde, Serde<V> valueSerde) {
    }

    public static <V> MaterializedStore<V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store, String topicName) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        return materialize(mat, store, topicName);
    }

    public static <V> MaterializedStore<V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store, String topicName) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, topicName);
    }

    public static <V> MaterializedStore<V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store, String topicName) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, topicName);
    }

    private static <V, S extends StateStore> MaterializedStore<V, S> materialize(Materialized<Object, V, S> mat, StateStoreDefinition store, String topicName) {
        final var keySerde = getStoreSerde(store.keyType(), true, topicName);
        final var valueSerde = (Serde<V>) getStoreSerde(store.valueType(), false, store.name());
        mat = mat.withKeySerde(keySerde).withValueSerde(valueSerde);
        mat = store.caching() ? mat.withCachingEnabled() : mat.withCachingDisabled();
        mat = store.logging() ? mat.withLoggingEnabled(new HashMap<>()) : mat.withLoggingDisabled();
        return new MaterializedStore<>(mat, keySerde, valueSerde);
    }

    public static Serde<Object> getStoreSerde(UserType type, boolean isKey, final String topicName) {
        // Here we determine the serde for the state store. The rule is: if we are serializing to a Kafka topic, then we
        // follow the explicitly provided type and use the corresponding serde. If we are serializing to an in memory or
        // RocksDB store, then we use an internal serde, which prevents external side effects, such as registering an
        // AVRO schema for a non-existing topic. The internal serde is capable of serializing an in-memory DataObject
        // to a byte[] and restoring it from the byte[] completely, including its schema, without any unwanted SR side
        // effects.
        final var defaultSerde = new StreamDataType(type, isKey).serde();
        return new StoreSerde(type.dataType(), defaultSerde, topicName);
    }
}
