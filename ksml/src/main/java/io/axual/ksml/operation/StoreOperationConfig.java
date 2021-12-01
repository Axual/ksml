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

import org.apache.kafka.streams.kstream.Grouped;

import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.store.StoreType;

public class StoreOperationConfig extends OperationConfig {
    public final String storeName;
    public final GroupedRegistry groupedRegistry;
    public final StoreRegistry storeRegistry;

    public interface GroupedRegistry {
        <K, V> void registerGrouped(Grouped<K, V> grouped);
    }

    public interface StoreRegistry {
        void registerStore(StoreType type, String storeName, StreamDataType keyType, StreamDataType valueType);
    }

    public StoreOperationConfig(String name, NotationLibrary notationLibrary, String storeName, GroupedRegistry groupedRegistry, StoreRegistry storeRegistry) {
        super(name, notationLibrary);
        this.storeName = storeName;
        this.groupedRegistry = groupedRegistry;
        this.storeRegistry = storeRegistry;
    }
}
