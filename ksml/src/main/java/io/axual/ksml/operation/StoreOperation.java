package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.store.StoreType;

public class StoreOperation extends BaseOperation {
    protected final String storeName;
    protected final StoreOperationConfig.GroupedRegistry groupedRegistry;
    protected final StoreOperationConfig.StoreRegistry storeRegistry;

    public StoreOperation(StoreOperationConfig config) {
        super(config);
        this.storeName = config.storeName == null ? name : config.storeName;
        this.groupedRegistry = config.groupedRegistry;
        this.storeRegistry = config.storeRegistry;
    }

    @Override
    public String toString() {
        return super.toString() + " [storeName=\"" + storeName + "\"]";
    }

    protected <K, V> Grouped<K, V> registerGrouped(Grouped<K, V> grouped) {
        groupedRegistry.registerGrouped(grouped);
        return grouped;
    }

    protected <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> registerKeyValueStore(String storeName, StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.KEYVALUE_STORE, storeName, keyType, valueType);
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> mat = Materialized.as(storeName);
        mat = mat.withKeySerde(keyType.getSerde());
        mat = mat.withValueSerde((Serde<V>) valueType.getSerde());
        return mat;
    }

    protected <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> registerSessionStore(String storeName, StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.SESSION_STORE, storeName, keyType, valueType);
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(storeName);
        mat = mat.withKeySerde(keyType.getSerde());
        mat = mat.withValueSerde((Serde<V>) valueType.getSerde());
        return mat;
    }

    protected <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> registerWindowStore(String storeName, StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.WINDOW_STORE, storeName, keyType, valueType);
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(storeName);
        mat = mat.withKeySerde(keyType.getSerde());
        mat = mat.withValueSerde((Serde<V>) valueType.getSerde());
        return mat;
    }

    protected StreamDataType windowedTypeOf(StreamDataType type) {
        return streamDataTypeOf(new StaticUserType(new WindowedType(type.type()), type.notation().name()), true);
    }
}
