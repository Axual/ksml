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


import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformKeyProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import org.apache.kafka.streams.kstream.Named;

public class TransformKeyOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public TransformKeyOperation(OperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR> KStream<KR, V> selectKey(
         *          final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = streamDataTypeOf(mapper.resultType, true);
        checkFunction(MAPPER_NAME, mapper, equalTo(kr), superOf(k), superOf(v));

        final var action = new UserKeyTransformer(mapper);
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames);
        final var supplier = new OperationProcessorSupplier<>(
                name,
                TransformKeyProcessor::new,
                (stores, record) -> action.apply(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.process(supplier, Named.as(name), storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, kr, v);
    }
}
