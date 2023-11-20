package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import io.axual.ksml.data.type.UserTupleType;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserKeyValueTransformer;
import org.apache.kafka.streams.kstream.Grouped;

public class GroupByOperation extends StoreOperation {
    private static final String SELECTOR_NAME = "Selector";
    private final UserFunction selector;

    public GroupByOperation(StoreOperationConfig config, UserFunction selector) {
        super(config);
        this.selector = selector;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR> KGroupedStream<KR, V> groupBy(
         *          final KeyValueMapper<? super K, ? super V, KR> keySelector,
         *          final Grouped<KR, V> grouped)
         */

        checkNotNull(selector, SELECTOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = streamDataTypeOf(selector.resultType, true);
        checkFunction(SELECTOR_NAME, selector, equalTo(kr), superOf(k), superOf(v));

        final var kvStore = validateKeyValueStore(store, kr, v);
        final var mapper = new UserKeyTransformer(selector);
        var grouped = Grouped.with(kr.getSerde(), v.getSerde());
        if (name != null) grouped = grouped.withName(name);
        if (kvStore != null) grouped = grouped.withName(name);
        final var output = input.stream.groupBy(mapper, grouped);
        return new KGroupedStreamWrapper(output, kr, v);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR, VR> KGroupedTable<KR, VR> groupBy(
         *          final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
         *          final Grouped<KR, VR> grouped)
         */

        checkNotNull(selector, SELECTOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var selectorResultType = selector.resultType;
        checkTuple(SELECTOR_NAME + " resultType", selectorResultType, DataType.UNKNOWN, DataType.UNKNOWN);
        checkFunction(SELECTOR_NAME, selector, equalTo(selectorResultType), superOf(k), superOf(v));

        if (selectorResultType.dataType() instanceof UserTupleType userTupleType && userTupleType.subTypeCount() == 2) {
            final var kr = streamDataTypeOf(userTupleType.getUserType(0), true);
            final var vr = streamDataTypeOf(userTupleType.getUserType(1), false);
            final var kvStore = validateKeyValueStore(store, kr, vr);
            final var mapper = new UserKeyValueTransformer(selector);
            var grouped = Grouped.with(kr.getSerde(), vr.getSerde());
            if (name != null) grouped = grouped.withName(name);
            if (kvStore != null) grouped = grouped.withName(kvStore.name());
            final var output = input.table.groupBy(mapper, grouped);
            return new KGroupedTableWrapper(output, kr, vr);
        }

        throw new KSMLTopologyException("Can not apply given transformer to KTable.groupBy operation");
    }
}
