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


import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.HashMap;

public class OuterJoinOperation extends StoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final BaseStreamWrapper joinStream;
    private final UserFunction valueJoiner;
    private final JoinWindows joinWindows;

    public OuterJoinOperation(StoreOperationConfig config, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream instanceof KStreamWrapper otherStream) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> outerJoin(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var vo = otherStream.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", otherStream.keyType().userType().dataType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vo));
            final var windowStore = validateWindowStore(context.lookupStore(store), k, vr);
            var joined = StreamJoined.with(k.getSerde(), v.getSerde(), vo.getSerde());
            if (name != null) joined = joined.withName(name);
            if (windowStore != null) {
                if (windowStore.name() != null) joined = joined.withStoreName(windowStore.name());
                joined = windowStore.logging() ? joined.withLoggingEnabled(new HashMap<>()) : joined.withLoggingDisabled();
            }
            final var output = (KStream) input.stream.outerJoin(otherStream.stream, new UserValueJoiner(valueJoiner), joinWindows, joined);
            return new KStreamWrapper(output, k, vr);
        }

        throw new KSMLTopologyException("Can not OUTER_JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream instanceof KTableWrapper otherTable) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> outerJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            final var vo = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", otherTable.keyType().userType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(vr), vr, superOf(v), superOf(vo));
            final var kvStore = validateKeyValueStore(context.lookupStore(store), k, vr);

            if (kvStore != null) {
                final var mat = materialize(kvStore);
                final var output = name != null
                        ? input.table.outerJoin(otherTable.table, new UserValueJoiner(valueJoiner), Named.as(name), mat)
                        : input.table.outerJoin(otherTable.table, new UserValueJoiner(valueJoiner), mat);
                return new KTableWrapper(output, k, vr);
            }

            final var output = name != null
                    ? (KTable) input.table.outerJoin(otherTable.table, new UserValueJoiner(valueJoiner), Named.as(name))
                    : (KTable) input.table.outerJoin(otherTable.table, new UserValueJoiner(valueJoiner));
            return new KTableWrapper(output, k, vr);
        }
        throw new KSMLTopologyException("Can not OUTER_JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
