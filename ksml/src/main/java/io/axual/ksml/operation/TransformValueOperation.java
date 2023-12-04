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
import io.axual.ksml.operation.processor.FixedKeyOperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformValueProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserValueTransformer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

public class TransformValueOperation extends StoreOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public TransformValueOperation(StoreOperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(mapper, v.userType()), false);
        final var map = checkFunction(MAPPER_NAME, mapper, vr, superOf(k), superOf(v));

        final var userMap = new UserValueTransformer(context.createUserFunction(map, k.userType().dataType(), v.userType().dataType()));
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames().toArray(TEMPLATE));
        final var supplier = new FixedKeyOperationProcessorSupplier<>(
                name,
                TransformValueProcessor::new,
                (stores, record) -> userMap.apply(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.processValues(supplier, Named.as(name), storeNames)
                : input.stream.processValues(supplier, storeNames);
        return new KStreamWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> mapValues(
         *          final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
         *          final Named named,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(mapper, v.userType()), false);
        final var map = checkFunction(MAPPER_NAME, mapper, vr, superOf(k), superOf(v));
        final var kvStore = validateKeyValueStore(store(), k, vr);

        final var userMap = new UserValueTransformer(context.createUserFunction(map));
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = context.materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.table.mapValues(userMap, named, mat)
                    : (KTable) input.table.mapValues(userMap, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.table.mapValues(userMap, named)
                : (KTable) input.table.mapValues(userMap);
        return new KTableWrapper(output, k, vr);
    }
}
