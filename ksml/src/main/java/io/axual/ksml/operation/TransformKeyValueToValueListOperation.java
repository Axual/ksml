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


import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.FixedKeyOperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformKeyValueToValueListProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserKeyValueToValueListTransformer;

public class TransformKeyValueToValueListOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformKeyValueToValueListOperation(OperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KStream<K, VR> flatMapValues(
         *          final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
         *          final Named named)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vrs = streamDataTypeOf(firstSpecificType(mapper, new UserType(new ListType(DataType.UNKNOWN))), false);
        if (!(vrs.userType().dataType() instanceof ListType vrList)) {
            throw new TopologyException("Function should return a list of values, but currently returns " + vrs);
        }
        final var vr = streamDataTypeOf(vrList.valueType(), false);
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, vrs, superOf(k.flatten()), superOf(v.flatten()));
        final var userMap = new UserKeyValueToValueListTransformer(map, tags);
        final var storeNames = mapper.storeNames().toArray(String[]::new);
        final var supplier = new FixedKeyOperationProcessorSupplier<>(
                name,
                TransformKeyValueToValueListProcessor::new,
                (stores, rec) -> userMap.apply(stores, flattenValue(rec.key()), flattenValue(rec.value())),
                storeNames);
        final var named = namedOf();
        final var output = named != null
                ? input.stream.processValues(supplier, named, storeNames)
                : input.stream.processValues(supplier, storeNames);
        return new KStreamWrapper(output, k, vr);
    }
}
