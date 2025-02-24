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

import io.axual.ksml.data.type.RecordMetadata;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public class TransformMetadataProcessor extends FixedKeyOperationProcessor {
    public interface TransformMetadataAction {
        RecordMetadata apply(StateStores stores, FixedKeyRecord<Object, Object> rec);
    }

    private final TransformMetadataAction action;

    public TransformMetadataProcessor(String name, TransformMetadataAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(FixedKeyRecord<Object, Object> rec) {
        var result = action.apply(stores, rec);
        if (result.timestamp() != null) rec = rec.withTimestamp(result.timestamp());
        if (result.headers() != null) rec = rec.withHeaders(result.headers());
        context.forward(rec);
    }
}
