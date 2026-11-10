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

import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.proxy.store.ProxyUtil;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.Function;

public abstract class OperationProcessor implements Processor<Object, Object, Object, Object> {
    protected ProcessorContext<Object, Object> context;
    private final String name;
    private final String[] storeNames;
    protected final StateStores stores = new StateStores();
    // The timestamp of the record that is being processed, null between records
    private Long recordTimestamp;
    private Function<String, StateStoreDefinition> storeDefinitions = storeName -> null;

    protected OperationProcessor(String name, String[] storeNames) {
        this.name = name;
        this.storeNames = storeNames;
    }

    /** Sets where {@link #init} looks up the definitions, and so the declared types, of the stores it wraps. */
    void storeDefinitions(Function<String, StateStoreDefinition> storeDefinitions) {
        this.storeDefinitions = storeDefinitions;
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
            stores.put(storeName, ProxyUtil.wrapStateStore(store, storeDefinitions.apply(storeName), () -> recordTimestamp != null ? recordTimestamp : ProxyUtil.noRecordTimestamp()));
        }
    }

    /** Remembers the timestamp of the record, so Python writes to timestamped stores can use it, then processes it. */
    @Override
    public final void process(Record<Object, Object> rec) {
        try {
            recordTimestamp = rec.timestamp();
            processRecord(rec);
        } finally {
            recordTimestamp = null;
        }
    }

    protected abstract void processRecord(Record<Object, Object> rec);
}
