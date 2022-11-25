package io.axual.ksml.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

import io.axual.ksml.definition.StoreDefinition;
import io.axual.ksml.generator.StreamDataType;

public class StoreUtil {
    private StoreUtil() {
    }

    public static <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> createKeyValueStore(StoreDefinition storeDefinition, StreamDataType keyType, StreamDataType valueType) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> mat = Materialized.as(storeDefinition.name);
        return configureStore(mat, storeDefinition.retention, keyType, valueType, storeDefinition.caching);
    }

    public static <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> createSessionStore(StoreDefinition storeDefinition, StreamDataType keyType, StreamDataType valueType) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(storeDefinition.name);
        return configureStore(mat, storeDefinition.retention, keyType, valueType, storeDefinition.caching);
    }

    public static <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> createWindowStore(StoreDefinition storeDefinition, StreamDataType keyType, StreamDataType valueType) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(storeDefinition.name);
        return configureStore(mat, storeDefinition.retention, keyType, valueType, storeDefinition.caching);
    }

    private static <V, S extends StateStore> Materialized<Object, V, S> configureStore(Materialized<Object, V, S> mat, Duration retention, StreamDataType keyType, StreamDataType valueType, Boolean cachingEnabled) {
        mat = mat.withKeySerde(keyType.getSerde());
        mat = mat.withValueSerde((Serde<V>) valueType.getSerde());
        if (retention != null) {
            mat = mat.withRetention(retention);
        }
        return cachingEnabled != null && cachingEnabled ? mat.withCachingEnabled() : mat.withCachingDisabled();
    }
}
