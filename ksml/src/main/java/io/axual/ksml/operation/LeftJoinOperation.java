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
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserValueJoiner;
import io.axual.ksml.user.UserValueJoinerWithKey;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.HashMap;

public class LeftJoinOperation extends StoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TopicDefinition joinTopic;
    private final FunctionDefinition valueJoiner;
    private final Duration joinWindowsDuration;

    public LeftJoinOperation(StoreOperationConfig config, StreamDefinition joinStream, FunctionDefinition valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinTopic = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindowsDuration = joinWindowDuration;
    }

    public LeftJoinOperation(StoreOperationConfig config, TableDefinition joinTable, FunctionDefinition valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinTopic = joinTable;
        this.valueJoiner = valueJoiner;
        this.joinWindowsDuration = joinWindowDuration;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof StreamDefinition joinStream) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> leftJoin(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var otherStream = context.getStreamWrapper(joinStream);
            final var vo = otherStream.valueType();
            final var vr = context.streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", otherStream.keyType(), equalTo(k));
            final var joiner = checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
            final var windowStore = validateWindowStore(store, k, vr);

            var joined = StreamJoined.with(k.getSerde(), v.getSerde(), vo.getSerde());
            if (name != null) joined = joined.withName(name);
            if (windowStore != null) {
                if (windowStore.name() != null) joined = joined.withStoreName(windowStore.name());
                joined = windowStore.logging() ? joined.withLoggingEnabled(new HashMap<>()) : joined.withLoggingDisabled();
            }

            final var userJoiner = new UserValueJoinerWithKey(context.createUserFunction(joiner));
            final var output = (KStream) input.stream.leftJoin(otherStream.stream, userJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowsDuration), joined);
            return new KStreamWrapper(output, k, vr);
        }

        if (joinTopic instanceof TableDefinition joinTable) {
            /*    Kafka Streams method signature:
             *    <VT, VR> KStream<K, VR> leftJoin(
             *          final KTable<K, VT> table,
             *          final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
             *          final Joined<K, V, VT> joined)
             */

            final var otherTable = context.getStreamWrapper(joinTable);
            final var vt = otherTable.valueType();
            final var vr = context.streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
            checkType("Join table keyType", otherTable.keyType(), equalTo(k));
            final var joiner = checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vt));

            var joined = Joined.with(k.getSerde(), v.getSerde(), vt.getSerde());
            if (name != null) joined = joined.withName(name);

            final var userJoiner = new UserValueJoinerWithKey(context.createUserFunction(joiner));
            final var output = (KStream) input.stream.leftJoin(otherTable.table, userJoiner, joined);
            return new KStreamWrapper(output, k, vr);
        }
        throw new KSMLTopologyException("Can not LEFT_JOIN stream with " + joinTopic.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof TableDefinition joinTable) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> leftJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var otherTable = context.getStreamWrapper(joinTable);
            final var vo = otherTable.valueType();
            final var vr = context.streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", otherTable.keyType(), equalTo(k));
            final var joiner = checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(vr), vr, superOf(v), superOf(vo));
            final var userJoiner = new UserValueJoiner(context.createUserFunction(joiner));
            final var kvStore = validateKeyValueStore(store, k, vr);
            if (kvStore != null) {
                final var mat = context.materialize(kvStore);
                final var output = name != null
                        ? input.table.leftJoin(otherTable.table, userJoiner, Named.as(name), mat)
                        : input.table.leftJoin(otherTable.table, userJoiner, mat);
                return new KTableWrapper(output, k, vr);
            }

            final var output = name != null
                    ? (KTable) input.table.leftJoin(otherTable.table, userJoiner, Named.as(name))
                    : (KTable) input.table.leftJoin(otherTable.table, userJoiner);
            return new KTableWrapper(output, k, vr);
        }

        throw new KSMLTopologyException("Can not LEFT_JOIN table with " + joinTopic.getClass().getSimpleName());
    }
}
