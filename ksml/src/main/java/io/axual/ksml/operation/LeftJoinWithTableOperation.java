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
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserForeignKeyExtractor;
import io.axual.ksml.user.UserStreamPartitioner;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;

public class LeftJoinWithTableOperation extends StoreOperation {
    private static final String FOREIGN_KEY_EXTRACTOR_NAME = "ForeignKeyExtractor";
    private static final String PARTITIONER_NAME = "Partitioner";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TableDefinition joinTable;
    private final FunctionDefinition foreignKeyExtractor;
    private final FunctionDefinition valueJoiner;
    private final Duration gracePeriod;
    private final FunctionDefinition partitioner;
    private final FunctionDefinition otherPartitioner;

    public LeftJoinWithTableOperation(StoreOperationConfig config, TableDefinition joinTable, FunctionDefinition foreignKeyExtractor, FunctionDefinition valueJoiner, Duration gracePeriod, FunctionDefinition partitioner, FunctionDefinition otherPartitioner) {
        super(config);
        this.joinTable = joinTable;
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.valueJoiner = valueJoiner;
        this.gracePeriod = gracePeriod;
        this.partitioner = partitioner;
        this.otherPartitioner = otherPartitioner;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VT, VR> KStream<K, VR> leftJoin(
         *          final KTable<K, VT> table,
         *          final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
         *          final Joined<K, V, VT> joined)
         */

        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var otherTable = context.getStreamWrapper(joinTable);
        final var kt = otherTable.keyType();
        final var vt = otherTable.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
        checkType("Join table keyType", kt, equalTo(k));
        final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vt));
        final var joined = joinedOf(k, v, vt, gracePeriod);
        final var userJoiner = valueJoinerWithKey(joiner, tags);
        final KStream<Object, Object> output = joined != null
                ? input.stream.leftJoin(otherTable.table, userJoiner, joined)
                : input.stream.leftJoin(otherTable.table, userJoiner);
        return new KStreamWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var otherTable = context.getStreamWrapper(joinTable);
        final var ko = otherTable.keyType();
        final var vo = otherTable.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
        checkType("Join table keyType", ko, equalTo(k));
        final var fkExtract = userFunctionOf(context, FOREIGN_KEY_EXTRACTOR_NAME, foreignKeyExtractor, v, equalTo(ko));
        if (fkExtract != null) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> leftJoin(
             *          final KTable<K, VO> other,
             *          final Function<V, KO> foreignKeyExtractor,
             *          final ValueJoiner<V, VO, VR> joiner,
             *          final TableJoined<K, KO> tableJoined,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var userFkExtract = new UserForeignKeyExtractor(fkExtract, tags);
            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
            final var userJoiner = valueJoiner(joiner, tags);
            final var part = userFunctionOf(context, PARTITIONER_NAME, partitioner, UserStreamPartitioner.EXPECTED_RESULT_TYPE, equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
            final var userPart = part != null ? new UserStreamPartitioner(part, tags) : null;
            final var otherPart = userFunctionOf(context, PARTITIONER_NAME, otherPartitioner, UserStreamPartitioner.EXPECTED_RESULT_TYPE, equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
            final var userOtherPart = part != null ? new UserStreamPartitioner(otherPart, tags) : null;
            final var tableJoined = tableJoinedOf(userPart, userOtherPart);
            final var kvStore = validateKeyValueStore(store(), k, vr);
            final var mat = materializedOf(context, kvStore);
            final KTable<Object, Object> output = tableJoined != null
                    ? mat != null
                    ? input.table.leftJoin(otherTable.table, userFkExtract, userJoiner, tableJoined, mat)
                    : input.table.leftJoin(otherTable.table, userFkExtract, userJoiner, tableJoined)
                    : mat != null
                    ? input.table.leftJoin(otherTable.table, userFkExtract, userJoiner, mat)
                    : input.table.leftJoin(otherTable.table, userFkExtract, userJoiner);
            return new KTableWrapper(output, k, vr);
        } else {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> leftJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
            final var userJoiner = valueJoiner(joiner, tags);
            final var kvStore = validateKeyValueStore(store(), k, vr);
            final var mat = materializedOf(context, kvStore);
            final var named = namedOf();
            final KTable<Object, Object> output = named != null
                    ? mat != null
                    ? input.table.leftJoin(otherTable.table, userJoiner, named, mat)
                    : input.table.leftJoin(otherTable.table, userJoiner, named)
                    : mat != null
                    ? input.table.leftJoin(otherTable.table, userJoiner, mat)
                    : input.table.leftJoin(otherTable.table, userJoiner);
            return new KTableWrapper(output, k, vr);
        }
    }
}
