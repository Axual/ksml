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
    // "as" name of the grouped stream to add
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
            throw topologyError("Cogroup \"stream\" is not allowed on a grouped stream, found \"" + stream + "\"");
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
            throw topologyError("This cogroup needs \"stream\": the \"as\" name of a grouped stream from an earlier pipeline");
        }
        checkNotNull(aggregator, AGGREGATOR_NAME.toLowerCase());
        input.checkNotExtended();
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
        input.markExtended();
        final CogroupedKStream<Object, Object> output = input.cogroupedStream.cogroup(other.groupedStream, userAggr);
        final var groupedStreams = new ArrayList<>(input.groupedStreams);
        groupedStreams.add(other.groupedStream);
        return new CogroupedKStreamWrapper(output, groupedStreams, k, vOut);
    }

    private KGroupedStreamWrapper lookupGroupedStream(TopologyBuildContext context) {
        final var wrapper = findSavedStream(context, stream);
        if (!(wrapper instanceof KGroupedStreamWrapper) && context.isTopicDefinition(stream)) throw notGroupedTopicError();
        return requireSavedStream(wrapper, KGroupedStreamWrapper.class, streamLabel(),
                "a grouped stream (the result of groupBy or groupByKey)",
                "Save a grouped stream with \"as: " + stream + "\" " + SAVED_EARLIER);
    }

    // Unlike in merge and join, "stream" is not a topic definition, so explain the likely mistake
    private TopologyException notGroupedTopicError() {
        return topologyError(streamLabel() + " is a topic definition, but cogroup needs a grouped stream. Save it with groupByKey or groupBy and \"as\" in a pipeline before this one");
    }

    private String streamLabel() {
        return "Cogroup stream \"" + stream + "\"";
    }

    @Override
    public String toString() {
        return super.toString() + (stream != null ? " [stream=\"" + stream + "\"]" : "");
    }
}
