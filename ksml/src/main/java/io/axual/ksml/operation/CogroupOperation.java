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
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserAggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;

public class CogroupOperation extends StoreOperation {
    private static final String AGGREGATOR_NAME = "Aggregator";
    private final FunctionDefinition aggregator;

    public CogroupOperation(StoreOperationConfig config, FunctionDefinition aggregator) {
        super(config);
        this.aggregator = aggregator;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VOut> CogroupedKStream<K, VOut> cogroup(
         *          final Aggregator<? super K, ? super V, VOut> aggregator)
         */

        checkNotNull(aggregator, AGGREGATOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vout = streamDataTypeOf(aggregator.resultType(), false);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vout, superOf(k), superOf(v), equalTo(vout));
        final var userAggr = new UserAggregator(aggr);
        final var kvStore = validateKeyValueStore(store(), k, vout);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final CogroupedKStream<Object, Object> output = input.groupedStream.cogroup(userAggr);
        return new CogroupedKStreamWrapper(output, k, vout);
    }


    @Override
    public StreamWrapper apply(CogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VIn> CogroupedKStream<K, VOut> cogroup(
         *          final KGroupedStream<K, VIn> groupedStream,
         *          final Aggregator<? super K, ? super VIn, VOut> aggregator)
         */

        // This is a method that we can not support, due to the pipeline nature of KSML. Therefore, throw
        // an exception with this explicit message.
        throw FatalError.topologyError("Cogrouping operation is not supported for CogroupStreams");
    }
}
