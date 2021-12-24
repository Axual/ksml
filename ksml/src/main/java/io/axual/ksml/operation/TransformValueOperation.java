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


import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueTransformer;

public class TransformValueOperation extends StoreOperation {
    private final UserFunction transformer;

    public TransformValueOperation(StoreOperationConfig config, UserFunction transformer) {
        super(config);
        this.transformer = transformer;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.mapValues(new UserValueTransformer(transformer), Named.as(name)),
                input.keyType(),
                streamDataTypeOf(transformer.resultType, false));
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(transformer.resultType, false);

        return new KTableWrapper(
                input.table.mapValues(
                        new UserValueTransformer(transformer),
                        Named.as(name),
                        registerKeyValueStore(storeName, input.keyType(), resultValueType)),
                input.keyType(),
                resultValueType);
    }
}
