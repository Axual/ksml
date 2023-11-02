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
import io.axual.ksml.operation.processor.TransformValueProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueTransformer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

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
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(mapper.resultType, false);
        checkFunction(MAPPER_NAME, mapper, equalTo(vr), superOf(k), superOf(v));

        final var action = new UserValueTransformer(mapper);
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames);
        final var supplier = new OperationProcessorSupplier<>(
                name,
                TransformValueProcessor::new,
                (stores, record) -> action.apply(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.process(supplier, Named.as(name), storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, k, vr);
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
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(mapper.resultType, false);
        checkFunction(MAPPER_NAME, mapper, equalTo(vr), superOf(k), superOf(v));
        final var kvStore = validateKeyValueStore(store, k, vr);

        final var map = new UserValueTransformer(mapper);
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.table.mapValues(map, named, mat)
                    : (KTable) input.table.mapValues(map, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.table.mapValues(map, named)
                : (KTable) input.table.mapValues(map);
        return new KTableWrapper(output, k, vr);
    }
}
