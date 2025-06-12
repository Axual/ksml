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
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyTransformer;
import org.apache.kafka.streams.kstream.KStream;

public class LeftJoinWithGlobalTableOperation extends BaseOperation {
    private static final String KEYSELECTOR_NAME = "Mapper";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final GlobalTableDefinition joinGlobalTable;
    private final FunctionDefinition keySelector;
    private final FunctionDefinition valueJoiner;

    public LeftJoinWithGlobalTableOperation(OperationConfig config, GlobalTableDefinition joinTable, FunctionDefinition keySelector, FunctionDefinition valueJoiner) {
        super(config);
        this.joinGlobalTable = joinTable;
        this.keySelector = keySelector;
        this.valueJoiner = valueJoiner;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <GK, GV, RV> KStream<K, RV> leftJoin(
         *          final GlobalKTable<GK, GV> globalTable,
         *          final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
         *          final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner,
         *          final Named named)
         */

        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var otherGlobalKTable = context.getStreamWrapper(joinGlobalTable);
        checkNotNull(keySelector, KEYSELECTOR_NAME.toLowerCase());
        final var gk = otherGlobalKTable.keyType();
        final var gv = otherGlobalKTable.valueType();
        final var rv = streamDataTypeOf(firstSpecificType(valueJoiner, gv, v), false);
        checkType("Join globalKTable keyType", gk, equalTo(k));
        final var sel = userFunctionOf(context, KEYSELECTOR_NAME, keySelector, subOf(gk), superOf(k), superOf(v));
        final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, subOf(rv), superOf(k), superOf(v), superOf(gv));
        final var userSel = new UserKeyTransformer(sel, tags);
        final var userJoiner = valueJoinerWithKey(joiner, tags);
        final var named = namedOf();
        final KStream<Object, Object> output = named != null
                ? input.stream.leftJoin(otherGlobalKTable.globalTable, userSel, userJoiner, named)
                : input.stream.leftJoin(otherGlobalKTable.globalTable, userSel, userJoiner);
        return new KStreamWrapper(output, k, rv);
    }
}
