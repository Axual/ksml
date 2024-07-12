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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.FixedKeyOperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformValueProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserValueTransformer;
import io.axual.ksml.user.UserValueTransformerWithKey;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class TransformValueOperation extends StoreOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformValueOperation(StoreOperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *     <VOut> KStream<K, VOut> processValues(
         *          final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
         *          final Named named,
         *          final String... stateStoreNames
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(mapper, v.userType()), false);
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, vr, superOf(k.flatten()), superOf(v.flatten()));
        final var userMap = new UserValueTransformer(map, tags);
        final var storeNames = mapper.storeNames().toArray(String[]::new);
        final var supplier = new FixedKeyOperationProcessorSupplier<>(
                name,
                TransformValueProcessor::new,
                (stores, record) -> userMap.apply(stores, flattenValue(record.key()), flattenValue(record.value())),
                storeNames);
        final var named = namedOf();
        final KStream<Object, Object> output = named != null
                ? input.stream.processValues(supplier, named, storeNames)
                : input.stream.processValues(supplier, storeNames);
        return new KStreamWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
         *          final Named named,
         *          final String... stateStoreNames);
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(mapper, v.userType()), false);
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, vr, superOf(k), superOf(v));
        final var userMap = new UserValueTransformerWithKey(map, tags);
        final var kvStore = validateKeyValueStore(store(), k, vr);
        final ValueTransformerWithKeySupplier<Object, Object, DataObject> supplier = () -> userMap;
        final var named = namedOf();
        final var mat = materializedOf(context, kvStore);
        final var storeNames = mapper.storeNames().toArray(String[]::new);
        final KTable<Object, Object> output = named != null
                ? mat != null
                ? input.table.transformValues(supplier, mat, named, storeNames)
                : input.table.transformValues(supplier, named, storeNames)
                : mat != null
                ? input.table.transformValues(supplier, mat, storeNames)
                : input.table.transformValues(supplier, storeNames);
        return new KTableWrapper(output, k, vr);
    }
}
