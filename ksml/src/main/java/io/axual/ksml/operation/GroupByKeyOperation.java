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
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.KGroupedStream;

public class GroupByKeyOperation extends StoreOperation {
    public GroupByKeyOperation(StoreOperationConfig config) {
        super(config);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KGroupedStream<K, V> groupByKey(
         *          final Grouped<K, V> grouped)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var kvStore = validateKeyValueStore(store(), k, v);
        final var grouped = groupedOf(k, v, kvStore);
        final KGroupedStream<Object, Object> output = grouped != null
                ? input.stream.groupByKey(grouped)
                : input.stream.groupByKey();
        return new KGroupedStreamWrapper(output, k, v);
    }
}
