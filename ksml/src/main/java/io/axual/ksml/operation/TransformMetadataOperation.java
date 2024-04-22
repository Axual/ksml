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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.type.RecordMetadata;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.FixedKeyOperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformMetadataProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserMetadataTransformer;
import org.apache.kafka.streams.kstream.KStream;

public class TransformMetadataOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformMetadataOperation(OperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    There is no Kafka DSL for converting metadata, but we use the following definition:
         *    <KR, VR> KStream<KR, VR> transformMetadata(
         *          final MetadataTransformer<? super K, ? super V, RecordMetadata metadata> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var meta = new UserType(RecordMetadata.DATATYPE);
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, subOf(meta), superOf(k), superOf(v), superOf(meta));
        final var userMap = new UserMetadataTransformer(map);
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames().toArray(TEMPLATE));
        final var supplier = new FixedKeyOperationProcessorSupplier<>(
                name,
                TransformMetadataProcessor::new,
                (stores, record) -> userMap.apply(stores, record.key(), record.value(), new RecordMetadata(record.timestamp(), record.headers())),
                storeNames);
        final var named = namedOf();
        final KStream<Object, Object> output = named != null
                ? input.stream.processValues(supplier, named, storeNames)
                : input.stream.processValues(supplier, storeNames);
        return new KStreamWrapper(output, k, v);
    }
}
