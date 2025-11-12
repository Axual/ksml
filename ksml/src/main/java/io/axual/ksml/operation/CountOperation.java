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


import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

public class CountOperation extends StoreOperation {
    public CountOperation(StoreOperationConfig config) {
        super(config);
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<K, Long> count(
         *                final Named named,
                          final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var vr = streamDataTypeOf(new UserType(DataLong.DATATYPE), false);
        final var kvStore = validateKeyValueStore(store(), k, vr);
        final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Long> output = named != null
                ? mat != null
                ? input.groupedStream.count(named, mat)
                : input.groupedStream.count(named)
                : mat != null
                ? input.groupedStream.count(mat)
                : input.groupedStream.count();
        return new KTableWrapper((KTable) output, k, vr);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<K, Long> count(
         *          final Named named,
         *          final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var vr = streamDataTypeOf(new UserType(DataLong.DATATYPE), false);
        final var kvStore = validateKeyValueStore(store(), k, vr);
        final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Long> output = named != null
                ? mat != null
                ? input.groupedTable.count(named, mat)
                : input.groupedTable.count(named)
                : mat != null
                ? input.groupedTable.count(mat)
                : input.groupedTable.count();
        return new KTableWrapper((KTable) output, k, vr);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, Long> count(
         *          final Named named,
         *          final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var vr = streamDataTypeOf(new UserType(DataLong.DATATYPE), false);
        final var sessionStore = validateSessionStore(store(), k, vr);
        final Materialized<Object, Long, SessionStore<Bytes, byte[]>> mat = materializedOf(context, sessionStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Long> output = named != null
                ? mat != null
                ? input.sessionWindowedKStream.count(named, mat)
                : input.sessionWindowedKStream.count(named)
                : mat != null
                ? input.sessionWindowedKStream.count(mat)
                : input.sessionWindowedKStream.count();
        return new KTableWrapper((KTable) output, windowed(k), vr);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, Long> count(
         *          final Named named,
         *          final Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var vr = streamDataTypeOf(new UserType(DataLong.DATATYPE), false);
        final var windowStore = validateWindowStore(store(), k, vr);
        final Materialized<Object, Long, WindowStore<Bytes, byte[]>> mat = materializedOf(context, windowStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Long> output = named != null
                ? mat != null
                ? input.timeWindowedKStream.count(named, mat)
                : input.timeWindowedKStream.count(named)
                : mat != null
                ? input.timeWindowedKStream.count(mat)
                : input.timeWindowedKStream.count();
        return new KTableWrapper((KTable) output, windowed(k), vr);
    }
}
