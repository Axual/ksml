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


import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.*;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;

public class JoinOperation extends BaseJoinOperation {
    private static final String KEYSELECTOR_NAME = "Mapper";
    private static final String FOREIGN_KEY_EXTRACTOR_NAME = "ForeignKeyExtractor";
    private static final String PARTITIONER_NAME = "Partitioner";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TopicDefinition joinTopic;
    private final FunctionDefinition keySelector;
    private final FunctionDefinition foreignKeyExtractor;
    private final FunctionDefinition valueJoiner;
    private final JoinWindows joinWindows;
    private final Duration gracePeriod;
    private final FunctionDefinition partitioner;
    private final FunctionDefinition otherPartitioner;

    public JoinOperation(StoreOperationConfig config, StreamDefinition joinStream, FunctionDefinition valueJoiner, Duration timeDifference, Duration gracePeriod) {
        super(config);
        this.joinTopic = joinStream;
        this.keySelector = null;
        this.foreignKeyExtractor = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = joinWindowsOf(timeDifference, gracePeriod);
        this.gracePeriod = null;
        this.partitioner = null;
        this.otherPartitioner = null;
    }

    public JoinOperation(StoreOperationConfig config, TableDefinition joinTable, FunctionDefinition foreignKeyExtractor, FunctionDefinition valueJoiner, Duration gracePeriod, FunctionDefinition partitioner, FunctionDefinition otherPartitioner) {
        super(config);
        this.joinTopic = joinTable;
        this.keySelector = null;
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
        this.gracePeriod = gracePeriod;
        this.partitioner = partitioner;
        this.otherPartitioner = otherPartitioner;
    }

    public JoinOperation(StoreOperationConfig config, GlobalTableDefinition joinGlobalTable, FunctionDefinition keySelector, FunctionDefinition valueJoiner) {
        super(config);
        this.joinTopic = joinGlobalTable;
        this.keySelector = keySelector;
        this.foreignKeyExtractor = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
        this.gracePeriod = null;
        this.partitioner = null;
        this.otherPartitioner = null;
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
            final var ko = otherStream.keyType();
            final var vo = otherStream.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", ko, equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
            final var windowedK = windowedTypeOf(k);
            final var windowStore = validateWindowStore(store(), k, vr);
            final var streamJoined = streamJoinedOf(windowStore, k, v, vo);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final KStream<Object, Object> output = streamJoined != null
                    ? input.stream.join(otherStream.stream, userJoiner, joinWindows, streamJoined)
                    : input.stream.join(otherStream.stream, userJoiner, joinWindows);
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
            final var kt = otherTable.keyType();
            final var vt = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
            checkType("Join table keyType", kt, equalTo(k));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vt));
            final var joined = joinedOf(name, k, v, vt, gracePeriod);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final KStream<Object, Object> output = joined != null
                    ? input.stream.join(otherTable.table, userJoiner, joined)
                    : input.stream.join(otherTable.table, userJoiner);
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
            checkNotNull(keySelector, KEYSELECTOR_NAME.toLowerCase());
            final var gk = otherGlobalKTable.keyType();
            final var gv = otherGlobalKTable.valueType();
            final var rv = streamDataTypeOf(firstSpecificType(valueJoiner, gv, v), false);
            checkType("Join globalKTable keyType", gk, equalTo(k));
            final var sel = userFunctionOf(context, KEYSELECTOR_NAME, keySelector, subOf(gk), superOf(k), superOf(v));
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(rv), superOf(k), superOf(v), superOf(gv));
            final var userSel = new UserKeyTransformer(sel);
            final var userJoiner = new UserValueJoinerWithKey(joiner);
            final var named = namedOf();
            final KStream<Object, Object> output = named != null
                    ? input.stream.join(otherGlobalKTable.globalTable, userSel, userJoiner, named)
                    : input.stream.join(otherGlobalKTable.globalTable, userSel, userJoiner);
            return new KStreamWrapper(output, k, rv);
        }

        throw new TopologyException("Can not JOIN stream with " + joinTopic.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinTopic instanceof TableDefinition joinTable) {
            final var otherTable = context.getStreamWrapper(joinTable);
            final var ko = otherTable.keyType();
            final var vo = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", ko, equalTo(k));
            final var fkExtract = userFunctionOf(context, FOREIGN_KEY_EXTRACTOR_NAME, foreignKeyExtractor, equalTo(v), equalTo(ko));
            if (fkExtract != null) {
                /*    Kafka Streams method signature:
                 *    <VO, VR> KTable<K, VR> join(
                 *          final KTable<K, VO> other,
                 *          final Function<V, KO> foreignKeyExtractor,
                 *          final ValueJoiner<V, VO, VR> joiner,
                 *          final TableJoined<K, KO> tableJoined,
                 *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
                 */

                final var userFkExtract = new UserForeignKeyExtractor(fkExtract);
                final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, equalTo(vr), equalTo(v), equalTo(vo));
                final var userJoiner = new UserValueJoiner(joiner);
                final var part = userFunctionOf(context, PARTITIONER_NAME, partitioner, equalTo(DataInteger.DATATYPE), equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
                final var userPart = part != null ? new UserStreamPartitioner(part) : null;
                final var otherPart = userFunctionOf(context, PARTITIONER_NAME, otherPartitioner, equalTo(DataInteger.DATATYPE), equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
                final var userOtherPart = part != null ? new UserStreamPartitioner(otherPart) : null;
                final var tableJoined = tableJoinedOf(userPart, userOtherPart);
                final var kvStore = validateKeyValueStore(store(), k, vr);
                final var mat = materializedOf(context, kvStore);
                final KTable<Object, Object> output = tableJoined != null
                        ? mat != null
                        ? input.table.join(otherTable.table, userFkExtract, userJoiner, tableJoined, mat)
                        : input.table.join(otherTable.table, userFkExtract, userJoiner, tableJoined)
                        : mat != null
                        ? input.table.join(otherTable.table, userFkExtract, userJoiner, mat)
                        : input.table.join(otherTable.table, userFkExtract, userJoiner);
                return new KTableWrapper(output, k, vr);
            } else {
                /*    Kafka Streams method signature:
                 *    <VO, VR> KTable<K, VR> join(
                 *          final KTable<K, VO> other,
                 *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                 *          final Named named,
                 *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
                 */

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
        }

        throw new TopologyException("Can not JOIN table with " + joinTopic.getClass().getSimpleName());
    }
}
