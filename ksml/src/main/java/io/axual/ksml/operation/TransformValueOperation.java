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

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueTransformer;

public class TransformValueOperation extends StoreOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public TransformValueOperation(StoreOperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KStream<K, VR> mapValues(
         *          final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var vr = mapper.resultType.dataType();
        checkFunction(MAPPER_NAME, mapper, equalTo(vr), superOf(k), superOf(v));

        final var resultValueType = streamDataTypeOf(mapper.resultType, false);
        return new KStreamWrapper(
                input.stream.mapValues(new UserValueTransformer(this.mapper), Named.as(name)),
                input.keyType(),
                resultValueType);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> mapValues(
         *          final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
         *          final Named named,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var vr = mapper.resultType.dataType();
        checkFunction(MAPPER_NAME, mapper, equalTo(vr), superOf(k), superOf(v));

        final var resultValueType = streamDataTypeOf(mapper.resultType, false);
        return new KTableWrapper(
                input.table.mapValues(
                        new UserValueTransformer(mapper),
                        Named.as(name),
                        registerKeyValueStore(input.keyType(), resultValueType)),
                input.keyType(),
                resultValueType);
    }
}
