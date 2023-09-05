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
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;

public class StoreUtil {
    private StoreUtil() {
    }

    public static <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> result = Materialized.as(store.name());
        return materialize(result, store, notationLibrary);
    }

    public static <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(store.name());
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, notationLibrary);
    }

    public static <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(store.name());
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, notationLibrary);
    }

    private static <V, S extends StateStore> Materialized<Object, V, S> materialize(Materialized<Object, V, S> mat, StateStoreDefinition store, NotationLibrary notationLibrary) {
        var keyType = new StreamDataType(notationLibrary, store.keyType(), true);
        var valueType = new StreamDataType(notationLibrary, store.valueType(), false);
        mat = mat.withKeySerde(keyType.getSerde()).withValueSerde((Serde<V>) valueType.getSerde());
        mat = store.caching() ? mat.withCachingEnabled() : mat.withCachingDisabled();
        mat = store.logging() ? mat.withLoggingEnabled(new HashMap<>()) : mat.withLoggingDisabled();
        return mat;
    }
}
