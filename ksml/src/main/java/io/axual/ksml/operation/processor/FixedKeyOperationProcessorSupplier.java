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

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

public class FixedKeyOperationProcessorSupplier<T> implements FixedKeyProcessorSupplier<Object, Object, Object> {
    public interface ProcessorFactory<T> {
        FixedKeyProcessor<Object, Object, Object> create(String name, T action, String[] storeNames);
    }

    protected final String name;
    protected final ProcessorFactory<T> factory;
    protected final T action;
    protected final String[] storeNames;

    public FixedKeyOperationProcessorSupplier(String name, ProcessorFactory<T> factory, T action, String[] storeNames) {
        this.name = name;
        this.factory = factory;
        this.action = action;
        this.storeNames = storeNames;
    }

    @Override
    public FixedKeyProcessor<Object, Object, Object> get() {
        return factory.create(name, action, storeNames);
    }
}
