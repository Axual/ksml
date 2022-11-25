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


import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.definition.StoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.schema.mapper.WindowedSchemaMapper;
import io.axual.ksml.store.GroupedRegistry;
import io.axual.ksml.store.StoreRegistry;
import io.axual.ksml.store.StoreType;
import io.axual.ksml.store.StoreUtil;

public class StoreOperation extends BaseOperation {
    private static final WindowedSchemaMapper mapper = new WindowedSchemaMapper();
    protected final StoreDefinition store;
    protected final GroupedRegistry groupedRegistry;
    protected final StoreRegistry storeRegistry;

    public StoreOperation(StoreOperationConfig config) {
        super(config);
        store = new StoreDefinition(
                config.store.name == null ? name : config.store.name,
                config.store.retention,
                config.store.caching
        );
        this.groupedRegistry = config.groupedRegistry;
        this.storeRegistry = config.storeRegistry;
    }

    @Override
    public String toString() {
        return super.toString() + " [storeName=\"" + store.name + "\"]";
    }

    protected <K, V> Grouped<K, V> registerGrouped(Grouped<K, V> grouped) {
        groupedRegistry.registerGrouped(grouped);
        return grouped;
    }

    protected <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> registerKeyValueStore(StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.KEYVALUE_STORE, store, keyType, valueType);
        return StoreUtil.createKeyValueStore(store, keyType, valueType);
    }

    protected <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> registerSessionStore(StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.SESSION_STORE, store, keyType, valueType);
        return StoreUtil.createSessionStore(store, keyType, valueType);
    }

    protected <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> registerWindowStore(StreamDataType keyType, StreamDataType valueType) {
        storeRegistry.registerStore(StoreType.WINDOW_STORE, store, keyType, valueType);
        return StoreUtil.createWindowStore(store, keyType, valueType);
    }

    protected StreamDataType windowedTypeOf(StreamDataType type) {
        var windowedType = new WindowedType(type.userType().dataType());
        return streamDataTypeOf(type.userType().notation(), windowedType, type.isKey());
    }
}
