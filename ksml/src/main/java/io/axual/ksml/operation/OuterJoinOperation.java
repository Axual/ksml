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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;

public class OuterJoinOperation extends BaseJoinOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TopicDefinition joinTopic;
    private final FunctionDefinition valueJoiner;
    private final JoinWindows joinWindows;

    public OuterJoinOperation(StoreOperationConfig config, StreamDefinition joinStream, FunctionDefinition valueJoiner, Duration timeDifference, Duration gracePeriod) {
        super(config);
        this.joinTopic = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = joinWindowsOf(timeDifference, gracePeriod);
    }

    public OuterJoinOperation(StoreOperationConfig config, TableDefinition joinStream, FunctionDefinition valueJoiner) {
        super(config);
        this.joinTopic = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof StreamDefinition joinStream) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> outerJoin(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var otherStream = context.getStreamWrapper(joinStream);
            final var vo = otherStream.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", otherStream.keyType().userType().dataType(), equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vo));
            final var windowedK = windowedTypeOf(k);
            final var windowStore = validateWindowStore(store(), k, vr);
            final var streamJoined = streamJoinedOf(windowStore, k, v, vo);
            final var userJoiner = new UserValueJoiner(joiner);
            final KStream<Object, Object> output = streamJoined != null
                    ? input.stream.outerJoin(otherStream.stream, userJoiner, joinWindows, streamJoined)
                    : input.stream.outerJoin(otherStream.stream, userJoiner, joinWindows);
            return new KStreamWrapper(output, windowedK, vr);
        }

        throw new KSMLTopologyException("Can not OUTER_JOIN stream with " + joinTopic.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof TableDefinition joinTable) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> outerJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var otherTable = context.getStreamWrapper(joinTable);
            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            final var vo = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", otherTable.keyType().userType(), equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(vr), superOf(v), superOf(vo));
            final var userJoiner = new UserValueJoiner(joiner);
            final var kvStore = validateKeyValueStore(store(), k, vr);
            final var mat = materializedOf(context, kvStore);
            final var named = namedOf();
            final KTable<Object, Object> output = named != null
                    ? mat != null
                    ? input.table.outerJoin(otherTable.table, userJoiner, named, mat)
                    : input.table.outerJoin(otherTable.table, userJoiner, named)
                    : mat != null
                    ? input.table.outerJoin(otherTable.table, userJoiner, mat)
                    : input.table.outerJoin(otherTable.table, userJoiner);
            return new KTableWrapper(output, k, vr);
        }
        throw new KSMLTopologyException("Can not OUTER_JOIN table with " + joinTopic.getClass().getSimpleName());
    }
}
