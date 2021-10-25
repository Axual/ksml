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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

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

    protected <K, V, S extends StateStore> Materialized<K, V, S> registerStore(Materialized<K, V, S> materialized) {
        storeRegistry.registerStore(materialized);
        return materialized;
    }
}
