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
import lombok.Getter;
import org.apache.kafka.streams.kstream.CogroupedKStream;

import java.util.ArrayList;
import java.util.List;

public class CogroupOperation extends BaseOperation {
    private static final String AGGREGATOR_NAME = "Aggregator";
    // Name of a grouped stream, registered with "as" by another pipeline, to add to an existing cogroup
    @Getter
    private final String stream;
    private final FunctionDefinition aggregator;

    public CogroupOperation(OperationConfig config, String stream, FunctionDefinition aggregator) {
        super(config);
        this.stream = stream;
        this.aggregator = aggregator;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VOut> CogroupedKStream<K, VOut> cogroup(
         *          final Aggregator<? super K, ? super V, VOut> aggregator)
         */

        if (stream != null) {
            throw topologyError("Cogroup \"stream\" can only be used on a cogroup that follows another cogroup, found \"" + stream + "\" on the first cogroup");
        }
        checkNotNull(aggregator, AGGREGATOR_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vOut = streamDataTypeOf(aggregator.resultType(), false);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vOut, superOf(k), superOf(v), equalTo(vOut));
        final var userAggr = new UserAggregator(aggr, tags);
        final CogroupedKStream<Object, Object> output = input.groupedStream.cogroup(userAggr);
        return new CogroupedKStreamWrapper(output, List.of(input.groupedStream), k, vOut);
    }

    @Override
    public StreamWrapper apply(CogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VIn> CogroupedKStream<K, VOut> cogroup(
         *          final KGroupedStream<K, VIn> groupedStream,
         *          final Aggregator<? super K, ? super VIn, VOut> aggregator)
         */

        if (stream == null) {
            throw topologyError("Cogroup on a cogrouped stream requires \"stream\": the name of a grouped stream saved with \"as\" in an earlier pipeline");
        }
        checkNotNull(aggregator, AGGREGATOR_NAME.toLowerCase());
        final var other = lookupGroupedStream(context);
        if (input.contains(other.groupedStream)) {
            throw topologyError(streamLabel() + " is already part of this cogroup. Each grouped stream can be added only once, with one aggregator");
        }
        final var k = input.keyType();
        final var vIn = other.valueType();
        final var vOut = input.valueType();
        checkType(streamLabel() + " keyType", other.keyType(), equalTo(k));
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vOut, superOf(k), superOf(vIn), equalTo(vOut));
        final var userAggr = new UserAggregator(aggr, tags);
        final CogroupedKStream<Object, Object> output = input.cogroupedStream.cogroup(other.groupedStream, userAggr);
        final var groupedStreams = new ArrayList<>(input.groupedStreams);
        groupedStreams.add(other.groupedStream);
        return new CogroupedKStreamWrapper(output, groupedStreams, k, vOut);
    }

    // Grouped streams can only be referenced by the name they were saved under with "as", which is registered in the
    // build context when the saving pipeline is built. So that pipeline must come before this one in the definition.
    private KGroupedStreamWrapper lookupGroupedStream(TopologyBuildContext context) {
        final StreamWrapper wrapper;
        try {
            wrapper = context.getStreamWrapper(stream, null);
        } catch (TopologyException e) {
            throw topologyError(streamLabel() + " not found. Save a grouped stream with \"as: " + stream + "\" in a pipeline defined before this one, in the same definition", e);
        }
        if (wrapper instanceof KGroupedStreamWrapper grouped) return grouped;
        throw topologyError(streamLabel() + " must be a grouped stream (the result of groupBy or groupByKey), but found " + wrapper);
    }

    private String streamLabel() {
        return "Cogroup stream \"" + stream + "\"";
    }
}
