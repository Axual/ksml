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

import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.KTable;

public class ToTableOperation extends StoreOperation {
    public ToTableOperation(StoreOperationConfig config) {
        super(config);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> toTable(
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var kvStore = StoreUtil.validateKeyValueStore(this, store(), k, v);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = named != null
                ? mat != null
                ? input.stream.toTable(named, mat)
                : input.stream.toTable(named)
                : mat != null
                ? input.stream.toTable(mat)
                : input.stream.toTable();
        return new KTableWrapper(output, k, v);
    }
}
