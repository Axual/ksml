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
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.KTable;

public class OuterJoinWithTableOperation extends StoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TableDefinition joinTable;
    private final FunctionDefinition valueJoiner;

    public OuterJoinWithTableOperation(StoreOperationConfig config, TableDefinition joinTable, FunctionDefinition valueJoiner) {
        super(config);
        this.joinTable = joinTable;
        this.valueJoiner = valueJoiner;
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VO, VR> KTable<K, VR> outerJoin(
         *          final KTable<K, VO> other,
         *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
         *          final Named named,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var otherTable = context.getStreamWrapper(joinTable);
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var ko = otherTable.keyType();
        final var vo = otherTable.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
        checkType("Join table keyType", ko, equalTo(k));
        final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(vr), superOf(k), superOf(v), superOf(vo));
        final var userJoiner = valueJoiner(joiner, tags);
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
}
