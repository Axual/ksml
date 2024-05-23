package io.axual.ksml.operation;

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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformKeyProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyTransformer;
import org.apache.kafka.streams.kstream.KStream;

public class TransformKeyOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformKeyOperation(OperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <KR> KStream<KR, V> selectKey(
         *          final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = streamDataTypeOf(firstSpecificType(mapper, k), true);
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, kr, superOf(k), superOf(v));
        final var userMap = new UserKeyTransformer(map, tags);
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames().toArray(TEMPLATE));
        final var supplier = new OperationProcessorSupplier<>(
                name,
                TransformKeyProcessor::new,
                (stores, record) -> userMap.apply(stores, record.key(), record.value()),
                storeNames);
        final var named = namedOf();
        final KStream<Object, Object> output = named != null
                ? input.stream.process(supplier, named, storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, kr, v);
    }
}
