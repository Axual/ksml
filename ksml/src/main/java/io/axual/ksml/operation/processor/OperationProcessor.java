package io.axual.ksml.operation.processor;

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

import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.store.StateStoreProxyFactory;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public abstract class OperationProcessor implements Processor<Object, Object, Object, Object> {
    protected ProcessorContext<Object, Object> context;
    private final String name;
    private final String[] storeNames;
    protected final StateStores stores = new StateStores();

    protected OperationProcessor(String name, String[] storeNames) {
        this.name = name;
        this.storeNames = storeNames;
    }

    @Override
    public void init(ProcessorContext<Object, Object> context) {
        this.context = context;
        stores.clear();
        for (String storeName : storeNames) {
            StateStore store = context.getStateStore(storeName);
            if (store == null) {
                throw new ExecutionException("Could not connect processor '" + name + "' to state store '" + storeName + "'");
            }
            // Wrap the store in a proxy for security
            stores.put(storeName, StateStoreProxyFactory.wrap(store));
        }
    }
}
