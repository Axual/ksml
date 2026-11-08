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
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;

public class StoreUtil {

    private StoreUtil() {
    }

    /**
     * Create a store supplier for a windowed state store and validate it.
     * The validations are copied from the Kafka Streams internal
     * {@link org.apache.kafka.streams.kstream.internals.KStreamImplJoin#assertWindowSettings(WindowBytesStoreSupplier, JoinWindows)}
     * and are duplicated here to provide less cryptic error messages.
     * @param store a {@link WindowStateStoreDefinition}.
     * @param joinWindows the {@link JoinWindows} settings.
     * @return a validated window store.
     */
    public static WindowBytesStoreSupplier validatedWindowStore(WindowStateStoreDefinition store, JoinWindows joinWindows) {
        return validatedWindowStore(store, joinWindows, false);
    }

    /**
     * Like {@link #validatedWindowStore(WindowStateStoreDefinition, JoinWindows)}, in the headers-aware format when
     * {@code headersStoreFormat} is set and the store supports it.
     */
    public static WindowBytesStoreSupplier validatedWindowStore(WindowStateStoreDefinition store, JoinWindows joinWindows, boolean headersStoreFormat) {
        final var result = getWindowStoreSupplier(store, headersStoreFormat);
        if (!result.retainDuplicates()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'retainDuplicates' set to 'true'.");
        }
        if (result.windowSize() != joinWindows.size()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'windowSize' equal to '2*timeDifference'.");
        }
        if (result.retentionPeriod() != joinWindows.size() + joinWindows.gracePeriodMs()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'retention' equal to '2*timeDifference + grace'.");
        }
        return result;
    }

    // Like Kafka Streams, stores built for processors are not cached unless the definition asks for it
    private static boolean processorStoreCaching(StateStoreDefinition store) {
        return Boolean.TRUE.equals(store.caching());
    }

    // Like Kafka Streams, stores materialized by DSL operations are cached unless the definition says otherwise
    private static boolean materializedStoreCaching(StateStoreDefinition store) {
        return !Boolean.FALSE.equals(store.caching());
    }

    // Like Kafka Streams, only persistent timestamped stores have a headers-aware format, and versioned stores have none
    private static boolean useHeaders(KeyValueStateStoreDefinition store, boolean headersStoreFormat) {
        return headersStoreFormat && store.persistent() && store.timestamped() && !store.versioned();
    }

    private static boolean useHeaders(WindowStateStoreDefinition store, boolean headersStoreFormat) {
        return headersStoreFormat && store.persistent() && store.timestamped();
    }

    // Session stores are not timestamped, so only persistence matters
    private static boolean useHeaders(SessionStateStoreDefinition store, boolean headersStoreFormat) {
        return headersStoreFormat && store.persistent();
    }

    private static KeyValueBytesStoreSupplier getKeyValueStoreSupplier(KeyValueStateStoreDefinition store, boolean headersStoreFormat) {
        if (!store.persistent()) {
            return Stores.inMemoryKeyValueStore(store.name());
        }
        if (store.versioned()) {
            return getVersionedStoreSupplier(store);
        }
        if (useHeaders(store, headersStoreFormat)) {
            return Stores.persistentTimestampedKeyValueStoreWithHeaders(store.name());
        }
        if (store.timestamped()) {
            return Stores.persistentTimestampedKeyValueStore(store.name());
        }
        return Stores.persistentKeyValueStore(store.name());
    }

    private static VersionedBytesStoreSupplier getVersionedStoreSupplier(KeyValueStateStoreDefinition store) {
        // Without a segment interval, Kafka Streams picks a default based on the history retention
        return store.segmentInterval().isZero()
                ? Stores.persistentVersionedKeyValueStore(store.name(), store.historyRetention())
                : Stores.persistentVersionedKeyValueStore(store.name(), store.historyRetention(), store.segmentInterval());
    }

    @SuppressWarnings("java:S1452") // wildcard is unavoidable: KV/Session/Window store builders have unrelated type parameters
    public static StoreBuilder<?> getStoreBuilder(StateStoreDefinition store) {
        return getStoreBuilder(store, false);
    }

    /**
     * Like {@link #getStoreBuilder(StateStoreDefinition)}, in the headers-aware format when
     * {@code headersStoreFormat} is set and the store supports it.
     */
    @SuppressWarnings("java:S1452") // wildcard is unavoidable: KV/Session/Window store builders have unrelated type parameters
    public static StoreBuilder<?> getStoreBuilder(StateStoreDefinition store, boolean headersStoreFormat) {
        if (store instanceof KeyValueStateStoreDefinition kvStore) {
            return getKeyValueStateStoreBuilder(kvStore, headersStoreFormat);
        } else if (store instanceof SessionStateStoreDefinition sessionStore) {
            return getSessionStateStoreBuilder(sessionStore, headersStoreFormat);
        } else if (store instanceof WindowStateStoreDefinition windowStore) {
            return getWindowStateStoreBuilder(windowStore, headersStoreFormat);
        }
        throw new IllegalArgumentException("Unknown store type: " + store.getClass().getSimpleName());
    }

    private static StoreBuilder<?> getKeyValueStateStoreBuilder(KeyValueStateStoreDefinition store, boolean headersStoreFormat) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        StoreBuilder<?> storeBuilder;
        if (store.persistent() && store.versioned()) {
            final var supplier = getVersionedStoreSupplier(store);
            storeBuilder = Stores.versionedKeyValueStoreBuilder(supplier, keyType.serde(), valueType.serde());
        } else if (useHeaders(store, headersStoreFormat)) {
            final var supplier = getKeyValueStoreSupplier(store, true);
            storeBuilder = Stores.timestampedKeyValueStoreWithHeadersBuilder(supplier, keyType.serde(), valueType.serde());
        } else {
            final var supplier = getKeyValueStoreSupplier(store, false);
            storeBuilder = store.timestamped()
                    ? Stores.timestampedKeyValueStoreBuilder(supplier, keyType.serde(), valueType.serde())
                    : Stores.keyValueStoreBuilder(supplier, keyType.serde(), valueType.serde());
        }
        storeBuilder = processorStoreCaching(store) ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    private static SessionBytesStoreSupplier getSessionStoreSupplier(SessionStateStoreDefinition store, boolean headersStoreFormat) {
        if (!store.persistent()) {
            return Stores.inMemorySessionStore(store.name(), store.retention());
        }
        return useHeaders(store, headersStoreFormat)
                ? Stores.persistentSessionStoreWithHeaders(store.name(), store.retention())
                : Stores.persistentSessionStore(store.name(), store.retention());
    }

    private static StoreBuilder<?> getSessionStateStoreBuilder(SessionStateStoreDefinition store, boolean headersStoreFormat) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        final var supplier = getSessionStoreSupplier(store, headersStoreFormat);
        StoreBuilder<?> storeBuilder = useHeaders(store, headersStoreFormat)
                ? Stores.sessionStoreWithHeadersBuilder(supplier, keyType.serde(), valueType.serde())
                : Stores.sessionStoreBuilder(supplier, keyType.serde(), valueType.serde());
        storeBuilder = processorStoreCaching(store) ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    private static WindowBytesStoreSupplier getWindowStoreSupplier(WindowStateStoreDefinition store, boolean headersStoreFormat) {
        if (!store.persistent()) {
            return Stores.inMemoryWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
        }
        if (useHeaders(store, headersStoreFormat)) {
            return Stores.persistentTimestampedWindowStoreWithHeaders(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
        }
        if (store.timestamped()) {
            return Stores.persistentTimestampedWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
        }
        return Stores.persistentWindowStore(store.name(), store.retention(), store.windowSize(), store.retainDuplicates());
    }

    private static StoreBuilder<?> getWindowStateStoreBuilder(WindowStateStoreDefinition store, boolean headersStoreFormat) {
        final var keyType = new StreamDataType(store.keyType(), true);
        final var valueType = new StreamDataType(store.valueType(), false);
        final var supplier = getWindowStoreSupplier(store, headersStoreFormat);
        StoreBuilder<?> storeBuilder = useHeaders(store, headersStoreFormat)
                ? Stores.timestampedWindowStoreWithHeadersBuilder(supplier, keyType.serde(), valueType.serde())
                : Stores.windowStoreBuilder(supplier, keyType.serde(), valueType.serde());
        storeBuilder = processorStoreCaching(store) ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled();
        storeBuilder = store.logging() ? storeBuilder.withLoggingEnabled(new HashMap<>()) : storeBuilder.withLoggingDisabled();
        return storeBuilder;
    }

    public record MaterializedStore<V, S extends StateStore>(Materialized<Object, V, S> materialized,
                                                             Serde<Object> keySerde, Serde<V> valueSerde) {
    }

    public static <V> MaterializedStore<V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store) {
        return materialize(store, false);
    }

    public static <V> MaterializedStore<V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store, boolean headersStoreFormat) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> mat = Materialized.as(getKeyValueStoreSupplier(store, headersStoreFormat));
        return materialize(mat, store);
    }

    public static <V> MaterializedStore<V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store) {
        return materialize(store, false);
    }

    public static <V> MaterializedStore<V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store, boolean headersStoreFormat) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(getSessionStoreSupplier(store, headersStoreFormat));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store);
    }

    public static <V> MaterializedStore<V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store) {
        return materialize(store, false);
    }

    public static <V> MaterializedStore<V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store, boolean headersStoreFormat) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(getWindowStoreSupplier(store, headersStoreFormat));
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store);
    }

    private static <V, S extends StateStore> MaterializedStore<V, S> materialize(Materialized<Object, V, S> mat, StateStoreDefinition store) {
        final var keySerde = new StreamDataType(store.keyType(), true).serde();
        @SuppressWarnings("unchecked") final var valueSerde = (Serde<V>) new StreamDataType(store.valueType(), false).serde();
        mat = mat.withKeySerde(keySerde).withValueSerde(valueSerde);
        mat = materializedStoreCaching(store) ? mat.withCachingEnabled() : mat.withCachingDisabled();
        mat = store.logging() ? mat.withLoggingEnabled(new HashMap<>()) : mat.withLoggingDisabled();
        return new MaterializedStore<>(mat, keySerde, valueSerde);
    }
}
