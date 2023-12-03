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
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.user.UserAggregator;
import io.axual.ksml.user.UserInitializer;
import io.axual.ksml.user.UserMerger;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

public class AggregateOperation extends StoreOperation {
    private static final String ADDER_NAME = "Adder";
    private static final String AGGREGATOR_NAME = "Aggregator";
    private static final String INITIALIZER_NAME = "Initializer";
    private static final String MERGER_NAME = "Merger";
    private static final String SUBTRACTOR_NAME = "Subtractor";
    private final FunctionDefinition initializer;
    private final FunctionDefinition aggregator;
    private final FunctionDefinition merger;
    private final FunctionDefinition adder;
    private final FunctionDefinition subtractor;

    public AggregateOperation(StoreOperationConfig config, FunctionDefinition initializer, FunctionDefinition aggregator, FunctionDefinition merger, FunctionDefinition adder, FunctionDefinition subtractor) {
        super(config);
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.merger = merger;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Named named,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(initializer, aggregator), false);
        final var init = checkFunction(INITIALIZER_NAME, initializer, vr);
        final var aggr = checkFunction(AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var kvStore = validateKeyValueStore(store(), k, vr);

        final var userInit = new UserInitializer(context.createUserFunction(init));
        final var userAggr = new UserAggregator(context.createUserFunction(aggr));
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = context.materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.groupedStream.aggregate(userInit, userAggr, named, mat)
                    : (KTable) input.groupedStream.aggregate(userInit, userAggr, mat);
            return new KTableWrapper(output, k, vr);
        }

        // Method signature using Named without Materialized does not exist
        final var output = (KTable) input.groupedStream.aggregate(userInit, userAggr);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> adder,
         *          final Aggregator<? super K, ? super V, VR> subtractor,
         *          final Named named,
         *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(initializer, adder, subtractor), false);
        final var init = checkFunction(INITIALIZER_NAME, initializer, vr);
        final var add = checkFunction(ADDER_NAME, adder, vr, superOf(k), superOf(v), superOf(vr));
        final var sub = checkFunction(SUBTRACTOR_NAME, subtractor, vr, superOf(k), superOf(vr), superOf(vr));
        final var kvStore = validateKeyValueStore(store(), k, vr);

        final var userInit = new UserInitializer(context.createUserFunction(init));
        final var userAdd = new UserAggregator(context.createUserFunction(add));
        final var userSub = new UserAggregator(context.createUserFunction(sub));
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = context.materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.groupedTable.aggregate(userInit, userAdd, userSub, named, mat)
                    : (KTable) input.groupedTable.aggregate(userInit, userAdd, userSub, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.groupedTable.aggregate(userInit, userAdd, userSub, named)
                : (KTable) input.groupedTable.aggregate(userInit, userAdd, userSub);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<Windowed<K>, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Merger<? super K, VR> sessionMerger,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(initializer, aggregator, merger), false);
        final var init = checkFunction(INITIALIZER_NAME, initializer, vr);
        final var aggr = checkFunction(AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var merg = checkFunction(MERGER_NAME, merger, vr, superOf(k), equalTo(vr), superOf(vr));
        final var sessionStore = validateSessionStore(store(), k, vr);

        final var userInit = new UserInitializer(context.createUserFunction(init));
        final var userAggr = new UserAggregator(context.createUserFunction(aggr));
        final var userMerg = new UserMerger(context.createUserFunction(merg));
        final var named = name != null ? Named.as(name) : null;

        if (sessionStore != null) {
            final var mat = context.materialize(sessionStore);
            final var output = named != null
                    ? (KTable) input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, named, mat)
                    : (KTable) input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, named)
                : (KTable) input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*
         *    Kafka Streams method signature:
         *    <VR> KTable<Windowed<K>, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Named named,
         *          final Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = context.streamDataTypeOf(firstSpecificType(initializer, aggregator), false);
        final var init = checkFunction(INITIALIZER_NAME, initializer, vr);
        final var aggr = checkFunction(AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var windowStore = validateWindowStore(store(), k, vr);

        final var userInit = new UserInitializer(context.createUserFunction(init));
        final var userAggr = new UserAggregator(context.createUserFunction(aggr));
        final var named = name != null ? Named.as(name) : null;

        if (windowStore != null) {
            final var mat = context.materialize(windowStore);
            final var output = named != null
                    ? (KTable) input.timeWindowedKStream.aggregate(userInit, userAggr, named, mat)
                    : (KTable) input.timeWindowedKStream.aggregate(userInit, userAggr, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.timeWindowedKStream.aggregate(userInit, userAggr, named)
                : (KTable) input.timeWindowedKStream.aggregate(userInit, userAggr);
        return new KTableWrapper(output, k, vr);
    }
}
