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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UserTupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyValueTransformer;

public class TransformKeyValueOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public TransformKeyValueOperation(OperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public BaseStreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR, VR> KStream<KR, VR> map(
         *          final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var tuple = checkTuple("Mapper resultType", mapper.resultType.dataType(), DataType.UNKNOWN, DataType.UNKNOWN);
        checkFunction(MAPPER_NAME, mapper, equalTo(tuple), superOf(k), superOf(v));

        if (mapper.resultType.dataType() instanceof UserTupleType userTupleType && userTupleType.subTypeCount() == 2) {
            var resultKeyType = userTupleType.subType(0);
            var resultKeyNotation = userTupleType.getUserType(0).notation();
            var resultValueType = userTupleType.subType(1);
            var resultValueNotation = userTupleType.getUserType(1).notation();

            return new KStreamWrapper(
                    input.stream.map(new UserKeyValueTransformer(mapper), Named.as(name)),
                    streamDataTypeOf(resultKeyNotation, resultKeyType, true),
                    streamDataTypeOf(resultValueNotation, resultValueType, false));
        }
        throw new KSMLExecutionException("ResultType of keyValueTransformer not correctly specified");
    }
}
