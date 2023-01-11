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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyValueToValueListTransformer;

public class TransformKeyValueToValueListOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public TransformKeyValueToValueListOperation(OperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KStream<K, VR> flatMapValues(
         *          final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        checkFunction(MAPPER_NAME, mapper, subOf(new ListType(DataType.UNKNOWN)), superOf(k), superOf(v));

        return new KStreamWrapper(
                input.stream.flatMapValues(new UserKeyValueToValueListTransformer(mapper), Named.as(name)),
                input.keyType(),
                streamDataTypeOf(mapper.resultType, false));
    }
}
