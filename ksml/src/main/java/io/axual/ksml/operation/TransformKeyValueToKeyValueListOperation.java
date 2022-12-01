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

import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.UserTupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyValueToKeyValueListTransformer;

public class TransformKeyValueToKeyValueListOperation extends BaseOperation {
    private final UserFunction transformer;

    public TransformKeyValueToKeyValueListOperation(OperationConfig config, UserFunction transformer) {
        super(config);
        this.transformer = transformer;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        if (transformer.resultType.dataType() instanceof ListType listType &&
                listType.valueType() instanceof UserTupleType userTupleType &&
                userTupleType.subTypeCount() == 2) {
            var resultKeyType = userTupleType.subType(0);
            var resultKeyNotation = userTupleType.getUserType(0).notation();
            var resultValueType = userTupleType.subType(1);
            var resultValueNotation = userTupleType.getUserType(1).notation();

            return new KStreamWrapper(
                    input.stream.flatMap(new UserKeyValueToKeyValueListTransformer(transformer), Named.as(name)),
                    streamDataTypeOf(resultKeyNotation, resultKeyType, true),
                    streamDataTypeOf(resultValueNotation, resultValueType, false));
        }
        throw new KSMLExecutionException("ResultType of keyValueToKeyValueListTransformer not correctly specified");
    }
}
