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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.KeyValueType;
import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserKeyValueTransformer;

public class GroupByOperation extends StoreOperation {
    private final UserFunction transformer;

    public GroupByOperation(StoreOperationConfig config, UserFunction transformer) {
        super(config);
        this.transformer = transformer;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        if (transformer.resultType.dataType() == DataType.UNKNOWN) {
            throw new KSMLExecutionException("groupBy mapper resultType not specified");
        }
        final StreamDataType resultKeyType = streamDataTypeOf(transformer.resultType, true);
        return new KGroupedStreamWrapper(
                input.stream.groupBy(
                        new UserKeyTransformer(transformer),
                        registerGrouped(Grouped.with(store.name, resultKeyType.getSerde(), input.valueType().getSerde()))),
                resultKeyType,
                input.valueType());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        if (!(transformer.resultType.dataType() instanceof KeyValueType resultType)) {
            throw new KSMLApplyException("Can not apply given transformer to KTable.groupBy operation");
        }
        final StreamDataType resultKeyType = streamDataTypeOf(transformer.resultType.notation(), resultType.keyType(), true);
        final StreamDataType resultValueType = streamDataTypeOf(transformer.resultType.notation(), resultType.valueType(), false);

        return new KGroupedTableWrapper(
                input.table.groupBy(
                        new UserKeyValueTransformer(transformer),
                        registerGrouped(Grouped.with(store.name, resultKeyType.getSerde(), resultValueType.getSerde()))),
                resultKeyType,
                resultValueType);
    }
}
