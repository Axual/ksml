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

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Record;

public class TransformKeyValueToKeyValueListProcessor extends OperationProcessor {
    public interface TransformKeyValueToKeyValueListAction {
        Iterable<KeyValue<Object, Object>> apply(StateStores stores, Record<Object, Object> rec);
    }

    private final TransformKeyValueToKeyValueListAction action;

    public TransformKeyValueToKeyValueListProcessor(String name, TransformKeyValueToKeyValueListAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> rec) {
        var keyValues = action.apply(stores, rec);
        if (keyValues != null)
            for (var kv : keyValues) context.forward(rec.withKey(kv.key).withValue(kv.value));
    }
}
