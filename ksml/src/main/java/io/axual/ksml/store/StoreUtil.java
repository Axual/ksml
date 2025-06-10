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

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.*;

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

    public static <V> MaterializedStore<V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        return materialize(mat, store);
    }

    public static <V> MaterializedStore<V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store);
    }

    public static <V> MaterializedStore<V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(getStoreSupplier(store));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store);
    }

    private static <V, S extends StateStore> MaterializedStore<V, S> materialize(Materialized<Object, V, S> mat, StateStoreDefinition store) {
        final var keySerde = new StreamDataType(store.keyType(), true).serde();
        @SuppressWarnings("unchecked")
        final var valueSerde = (Serde<V>) new StreamDataType(store.valueType(), false).serde();
        mat = mat.withKeySerde(keySerde).withValueSerde(valueSerde);
        mat = store.caching() ? mat.withCachingEnabled() : mat.withCachingDisabled();
        mat = store.logging() ? mat.withLoggingEnabled(new HashMap<>()) : mat.withLoggingDisabled();
        return new MaterializedStore<>(mat, keySerde, valueSerde);
    }

    public static KeyValueStateStoreDefinition validateKeyValueStore(BaseOperation operation, StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateKeyValueStore(operation, store, keyType.userType(), valueType.userType());
    }

    public static KeyValueStateStoreDefinition validateKeyValueStore(BaseOperation operation, StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof KeyValueStateStoreDefinition keyValueStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : keyValueStore.keyType();
            final var storeValueType = valueType != null ? valueType : keyValueStore.valueType();
            return new KeyValueStateStoreDefinition(
                    keyValueStore.name(),
                    keyValueStore.persistent(),
                    keyValueStore.timestamped(),
                    keyValueStore.versioned(),
                    keyValueStore.historyRetention(),
                    keyValueStore.segmentInterval(),
                    storeKeyType,
                    storeValueType,
                    keyValueStore.caching(),
                    keyValueStore.logging());
        }
        throw new ExecutionException(operation + " requires a  state store of type 'keyValue'");
    }

    public static SessionStateStoreDefinition validateSessionStore(BaseOperation operation, StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateSessionStore(operation, store, keyType.userType(), valueType.userType());
    }

    public static SessionStateStoreDefinition validateSessionStore(BaseOperation operation, StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof SessionStateStoreDefinition sessionStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : sessionStore.keyType();
            final var storeValueType = valueType != null ? valueType : sessionStore.valueType();
            return new SessionStateStoreDefinition(
                    sessionStore.name(),
                    sessionStore.persistent(),
                    sessionStore.timestamped(),
                    sessionStore.retention(),
                    storeKeyType,
                    storeValueType,
                    sessionStore.caching(),
                    sessionStore.logging());
        }
        throw new ExecutionException(operation + " requires a  state store of type 'session'");
    }

    public static WindowStateStoreDefinition validateWindowStore(BaseOperation operation, StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateWindowStore(operation, store, keyType.userType(), valueType.userType());
    }

    public static WindowStateStoreDefinition validateWindowStore(BaseOperation operation, StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof WindowStateStoreDefinition windowStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : windowStore.keyType();
            final var storeValueType = valueType != null ? valueType : windowStore.valueType();
            return new WindowStateStoreDefinition(
                    windowStore.name(),
                    windowStore.persistent(),
                    windowStore.timestamped(),
                    windowStore.retention(),
                    windowStore.windowSize(),
                    windowStore.retainDuplicates(),
                    storeKeyType,
                    storeValueType,
                    windowStore.caching(),
                    windowStore.logging());
        }
        throw new ExecutionException(operation + " requires a  state store of type 'window'");
    }

    private static void validateStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        validateStoreTypeWithStreamType(store, "key", store.keyType(), keyType);
        validateStoreTypeWithStreamType(store, "value", store.valueType(), valueType);
    }

    private static void validateStoreTypeWithStreamType(StateStoreDefinition store, String keyOrValue, UserType storeKeyOrValueType, UserType streamKeyOrValueType) {
        if (streamKeyOrValueType == null) {
            if (storeKeyOrValueType == null) {
                throw new ExecutionException("State store '" + store.name() + "' does not have a defined " + keyOrValue + " type");
            }
            return;
        }

        if (storeKeyOrValueType != null && !storeKeyOrValueType.dataType().isAssignableFrom(streamKeyOrValueType.dataType())) {
            throw new ExecutionException("Incompatible " + keyOrValue + " types for state store '" + store.name() + "': " + storeKeyOrValueType + " and " + streamKeyOrValueType);
        }
    }
}
