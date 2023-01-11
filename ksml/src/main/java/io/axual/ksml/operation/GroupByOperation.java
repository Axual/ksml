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


import org.apache.kafka.streams.kstream.Grouped;

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UserTupleType;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserKeyValueTransformer;

public class GroupByOperation extends StoreOperation {
    private static final String SELECTOR_NAME = "Mapper";
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
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var kr = selector.resultType.dataType();
        checkFunction(SELECTOR_NAME, selector, equalTo(kr), superOf(k), superOf(v));

        final StreamDataType resultKeyType = streamDataTypeOf(selector.resultType, true);
        return new KGroupedStreamWrapper(
                input.stream.groupBy(
                        new UserKeyTransformer(selector),
                        registerGrouped(Grouped.with(store.name(), resultKeyType.getSerde(), input.valueType().getSerde()))),
                resultKeyType,
                input.valueType());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR, VR> KGroupedTable<KR, VR> groupBy(
         *          final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
         *          final Grouped<KR, VR> grouped)
         */

        checkNotNull(selector, SELECTOR_NAME.toLowerCase());
        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var resultType = checkTuple(SELECTOR_NAME + " resultType", selector.resultType.dataType(), DataType.UNKNOWN, DataType.UNKNOWN);
        checkFunction(SELECTOR_NAME, selector, equalTo(resultType), superOf(k), superOf(v));

        if (resultType instanceof UserTupleType userTupleType && userTupleType.subTypeCount() == 2) {
            final StreamDataType resultKeyType = streamDataTypeOf(userTupleType.getUserType(0).notation(), userTupleType.subType(0), true);
            final StreamDataType resultValueType = streamDataTypeOf(userTupleType.getUserType(1).notation(), userTupleType.subType(1), false);

            return new KGroupedTableWrapper(
                    input.table.groupBy(
                            new UserKeyValueTransformer(selector),
                            registerGrouped(Grouped.with(store.name(), resultKeyType.getSerde(), resultValueType.getSerde()))),
                    resultKeyType,
                    resultValueType);
        }

        throw new KSMLTopologyException("Can not apply given transformer to KTable.groupBy operation");
    }
}
