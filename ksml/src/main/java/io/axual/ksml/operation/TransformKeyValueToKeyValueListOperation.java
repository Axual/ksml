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


import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.notation.UserTupleType;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformKeyValueToKeyValueListProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyValueToKeyValueListTransformer;

public class TransformKeyValueToKeyValueListOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformKeyValueToKeyValueListOperation(OperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <KR, VR> KStream<KR, VR> flatMap(
         *          final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var mapperResultType = firstSpecificType(mapper, new UserType(new ListType(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN))));
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, subOf(mapperResultType), superOf(k), superOf(v));

        if (mapperResultType.dataType() instanceof ListType mapperResultListType &&
                mapperResultListType.valueType() instanceof UserTupleType mapperResultListTupleValueType &&
                mapperResultListTupleValueType.subTypeCount() == 2) {
            final var kr = streamDataTypeOf(mapperResultListTupleValueType.getUserType(0), true);
            final var vr = streamDataTypeOf(mapperResultListTupleValueType.getUserType(1), false);
            final var userMap = new UserKeyValueToKeyValueListTransformer(map, tags);
            final var storeNames = mapper.storeNames().toArray(String[]::new);
            final var supplier = new OperationProcessorSupplier<>(
                    name,
                    TransformKeyValueToKeyValueListProcessor::new,
                    (stores, record) -> userMap.apply(stores, record.key(), record.value()),
                    storeNames);
            final var named = namedOf();
            final var output = name != null
                    ? input.stream.process(supplier, named, storeNames)
                    : input.stream.process(supplier, storeNames);
            return new KStreamWrapper(output, kr, vr);
        }
        throw new ExecutionException("ResultType of keyValueToKeyValueListTransformer not correctly specified");
    }
}
