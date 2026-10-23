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
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

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
        final var kvStore = validateKeyValueStore(store(), k, v);
        // Pass serdes also without a store: KSML sets no default serdes, and a stream can lose them (e.g. after filter)
        final Materialized<Object, Object, KeyValueStore<Bytes, byte[]>> mat = kvStore != null
                ? materializedOf(context, kvStore)
                : Materialized.with(k.serde(), v.serde());
        final var named = namedOf();
        final KTable<Object, Object> output = named != null
                ? input.stream.toTable(named, mat)
                : input.stream.toTable(mat);
        return new KTableWrapper(output, k, v);
    }
}
