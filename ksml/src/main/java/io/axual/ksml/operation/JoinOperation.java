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
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserValueJoiner;
import io.axual.ksml.user.UserValueJoinerWithKey;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;

public class JoinOperation extends BaseJoinOperation {
    private static final String KEYSELECTOR_NAME = "Mapper";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TopicDefinition joinTopic;
    private final FunctionDefinition keyValueMapper;
    private final FunctionDefinition valueJoiner;
    private final JoinWindows joinWindows;
    private final Duration gracePeriod;

    public JoinOperation(StoreOperationConfig config, StreamDefinition joinStream, FunctionDefinition valueJoiner, Duration timeDifference, Duration gracePeriod) {
        super(config);
        this.joinTopic = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = joinWindowsOf(timeDifference, gracePeriod);
        this.gracePeriod = null;
    }

    public JoinOperation(StoreOperationConfig config, TableDefinition joinTable, FunctionDefinition valueJoiner, Duration gracePeriod) {
        super(config);
        this.joinTopic = joinTable;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
        this.gracePeriod = gracePeriod;
    }

    public JoinOperation(StoreOperationConfig config, GlobalTableDefinition joinGlobalTable, FunctionDefinition keyValueMapper, FunctionDefinition valueJoiner) {
        super(config);
        this.joinTopic = joinGlobalTable;
        this.keyValueMapper = keyValueMapper;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
        this.gracePeriod = null;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof StreamDefinition joinStream) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> join(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var otherStream = context.getStreamWrapper(joinStream);
            final var vo = otherStream.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", vo, equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
            final var windowedK = windowedTypeOf(k);
            final var windowStore = validateWindowStore(store(), k, vr);
            final var joined = streamJoinedOf(windowStore, k, v, vo);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final var output = (KStream) input.stream.join(otherStream.stream, userJoiner, joinWindows, joined);
            return new KStreamWrapper(output, windowedK, vr);
        }

        if (joinTopic instanceof TableDefinition joinTable) {
            /*    Kafka Streams method signature:
             *    <VT, VR> KStream<K, VR> join(
             *          final KTable<K, VT> table,
             *          final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
             *          final Joined<K, V, VT> joined)
             */

            final var otherTable = context.getStreamWrapper(joinTable);
            final var vt = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
            checkType("Join table keyType", otherTable.keyType(), equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vt));
            final var joined = joinedOf(name, k, v, vt, gracePeriod);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final var output = (KStream) input.stream.join(otherTable.table, userJoiner, joined);
            return new KStreamWrapper(output, k, vr);
        }

        if (joinTopic instanceof GlobalTableDefinition joinGlobalTable) {
            /*    Kafka Streams method signature:
             *    <GK, GV, RV> KStream<K, RV> join(
             *          final GlobalKTable<GK, GV> globalTable,
             *          final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
             *          final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner,
             *          final Named named)
             */

            final var otherGlobalKTable = context.getStreamWrapper(joinGlobalTable);
            checkNotNull(keyValueMapper, KEYSELECTOR_NAME.toLowerCase());
            final var gk = otherGlobalKTable.keyType();
            final var gv = otherGlobalKTable.valueType();
            final var rv = streamDataTypeOf(firstSpecificType(valueJoiner, gv, v), false);
            checkType("Join globalKTable keyType", otherGlobalKTable.keyType(), equalTo(k));
            final var sel = userFunctionOf(context, KEYSELECTOR_NAME, keyValueMapper, subOf(gk), superOf(k), superOf(v));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(rv), superOf(k), superOf(v), superOf(gv));
            final var userSel = new UserKeyTransformer(sel);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final var named = namedOf();
            final var output = named != null
                    ? (KStream) input.stream.join(otherGlobalKTable.globalTable, userSel, userJoiner, named)
                    : (KStream) input.stream.join(otherGlobalKTable.globalTable, userSel, userJoiner);
            return new KStreamWrapper(output, k, rv);
        }

        throw new KSMLTopologyException("Can not JOIN stream with " + joinTopic.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof TableDefinition joinTable) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> join(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var otherTable = context.getStreamWrapper(joinTable);
            final var vo = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", otherTable.keyType(), equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(vr), superOf(v), superOf(vo));
            final var userJoiner = new UserValueJoiner(joiner);
            final var kvStore = validateKeyValueStore(store(), k, vr);
            final var mat = materializedOf(context, kvStore);
            final var named = namedOf();
            final KTable<Object, Object> output = named != null
                    ? mat != null
                    ? input.table.join(otherTable.table, userJoiner, named, mat)
                    : input.table.join(otherTable.table, userJoiner, named)
                    : mat != null
                    ? input.table.join(otherTable.table, userJoiner, mat)
                    : input.table.join(otherTable.table, userJoiner);
            return new KTableWrapper(output, k, vr);
        }
        throw new KSMLTopologyException("Can not JOIN table with " + joinTopic.getClass().getSimpleName());
    }
}
