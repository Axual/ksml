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
import io.axual.ksml.exception.TopologyException;
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
        final var vOut = streamDataTypeOf(aggregator.resultType(), false);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vOut, superOf(k), superOf(v), equalTo(vOut));
        final var userAggr = new UserAggregator(aggr);
        final CogroupedKStream<Object, Object> output = input.groupedStream.cogroup(userAggr);
        return new CogroupedKStreamWrapper(output, k, vOut);
    }

    @Override
    public StreamWrapper apply(CogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VIn> CogroupedKStream<K, VOut> cogroup(
         *          final KGroupedStream<K, VIn> groupedStream,
         *          final Aggregator<? super K, ? super VIn, VOut> aggregator)
         */

        // This is a method that we do not support currently. Due to the pipeline nature of KSML, we can at most ask
        // for a reference to a KGroupedStream as parameter. That parameter could currently only come from the result
        // of another pipeline, which then happens to be of Java-type KGroupedStream. It seems a little far-fetched
        // for KSML users to grasp the technicalities under the hood well enough to use this properly. Therefore, this
        // is unsupported for now and we throw an exception with this explicit message.
        throw new TopologyException("Cogroup operations are not supported for CogroupStreams");
    }
}
