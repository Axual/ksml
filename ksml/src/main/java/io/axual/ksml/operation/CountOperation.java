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
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

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
        final var output = kvStore != null
                ? (KTable) input.groupedStream.count(
                Named.as(name),
                context.materialize(kvStore))
                : (KTable) input.groupedStream.count(
                Named.as(name));
        return new KTableWrapper(output, k, vr);
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
        final var output = kvStore != null
                ? (KTable) input.groupedTable.count(
                Named.as(name),
                context.materialize(kvStore))
                : (KTable) input.groupedTable.count(
                Named.as(name));
        return new KTableWrapper(output, k, vr);
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
        final var windowedK = windowedTypeOf(k);
        final var sessionStore = validateSessionStore(store(), k, vr);
        final var output = sessionStore != null
                ? (KTable) input.sessionWindowedKStream.count(
                Named.as(name),
                context.materialize(sessionStore))
                : (KTable) input.sessionWindowedKStream.count(
                Named.as(name));
        return new KTableWrapper(output, windowedK, vr);
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
        final var windowedK = windowedTypeOf(k);
        final var windowStore = validateWindowStore(store(), k, vr);
        final var output = windowStore != null
                ? (KTable) input.timeWindowedKStream.count(
                Named.as(name),
                context.materialize(windowStore))
                : (KTable) input.timeWindowedKStream.count(
                Named.as(name));
        return new KTableWrapper(output, windowedK, vr);
    }
}
