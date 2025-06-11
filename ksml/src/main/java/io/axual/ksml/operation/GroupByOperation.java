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


import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.*;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserKeyValueTransformer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;

public class GroupByOperation extends StoreOperation {
    private static final String SELECTOR_NAME = "Selector";
    private final FunctionDefinition selector;

    public GroupByOperation(StoreOperationConfig config, FunctionDefinition selector) {
        super(config);
        this.selector = selector;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <KR> KGroupedStream<KR, V> groupBy(
         *          final KeyValueMapper<? super K, ? super V, KR> keySelector,
         *          final Grouped<KR, V> grouped)
         */

        checkNotNull(selector, SELECTOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = streamDataTypeOf(firstSpecificType(selector, k.userType()), true);
        final var sel = userFunctionOf(context, SELECTOR_NAME, selector, kr, superOf(k), superOf(v));
        final var kvStore = validateKeyValueStore(store(), kr, v);
        final var userSel = new UserKeyTransformer(sel, tags);
        final var grouped = groupedOf(kr, v, kvStore);
        final KGroupedStream<Object, Object> output = grouped != null
                ? input.stream.groupBy(userSel, grouped)
                : input.stream.groupBy(userSel);
        return new KGroupedStreamWrapper(output, kr, v);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <KR, VR> KGroupedTable<KR, VR> groupBy(
         *          final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
         *          final Grouped<KR, VR> grouped)
         */

        checkNotNull(selector, SELECTOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var krAndVr = firstSpecificType(selector, new UserType(new UserTupleType(k.userType(), v.userType())));

        if (krAndVr.dataType() instanceof UserTupleType userTupleType && userTupleType.subTypeCount() == 2) {
            checkTuple(SELECTOR_NAME + " resultType", krAndVr, DataType.UNKNOWN, DataType.UNKNOWN);
            final var sel = userFunctionOf(context, SELECTOR_NAME, selector, krAndVr, superOf(k), superOf(v));
            final var kr = streamDataTypeOf(userTupleType.getUserType(0), true);
            final var vr = streamDataTypeOf(userTupleType.getUserType(1), false);
            final var kvStore = validateKeyValueStore(store(), kr, vr);
            final var userSel = new UserKeyValueTransformer(sel, tags);
            final var grouped = groupedOf(kr, vr, kvStore);
            final KGroupedTable<Object, Object> output = grouped != null
                    ? input.table.groupBy(userSel, grouped)
                    : input.table.groupBy(userSel);
            return new KGroupedTableWrapper(output, kr, vr);
        }

        throw new TopologyException("Can not apply given transformer to KTable.groupBy operation");
    }
}
