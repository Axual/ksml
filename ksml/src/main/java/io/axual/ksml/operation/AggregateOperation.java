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
import io.axual.ksml.stream.*;
import io.axual.ksml.user.UserAggregator;
import io.axual.ksml.user.UserInitializer;
import io.axual.ksml.user.UserMerger;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;

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
        final var vr = streamDataTypeOf(firstSpecificType(initializer, aggregator), false);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, vr);
        final var userInit = new UserInitializer(init, tags);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var userAggr = new UserAggregator(aggr, tags);
        final var kvStore = validateKeyValueStore(store(), k, vr);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = mat != null
                ? named != null
                ? input.groupedStream.aggregate(userInit, userAggr, named, mat)
                : input.groupedStream.aggregate(userInit, userAggr, mat)
                : input.groupedStream.aggregate(userInit, userAggr);
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
        final var vr = streamDataTypeOf(firstSpecificType(initializer, adder, subtractor), false);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, vr);
        final var userInit = new UserInitializer(init, tags);
        final var add = userFunctionOf(context, ADDER_NAME, adder, vr, superOf(k), superOf(v), superOf(vr));
        final var userAdd = new UserAggregator(add, tags);
        final var sub = userFunctionOf(context, SUBTRACTOR_NAME, subtractor, vr, superOf(k), superOf(vr), superOf(vr));
        final var userSub = new UserAggregator(sub, tags);
        final var kvStore = validateKeyValueStore(store(), k, vr);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = named != null
                ? mat != null
                ? input.groupedTable.aggregate(userInit, userAdd, userSub, named, mat)
                : input.groupedTable.aggregate(userInit, userAdd, userSub, named)
                : mat != null
                ? input.groupedTable.aggregate(userInit, userAdd, userSub, mat)
                : input.groupedTable.aggregate(userInit, userAdd, userSub);
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
        final var windowedK = windowedTypeOf(k);
        final var vr = streamDataTypeOf(firstSpecificType(initializer, aggregator, merger), false);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, vr);
        final var userInit = new UserInitializer(init, tags);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var userAggr = new UserAggregator(aggr, tags);
        final var merg = userFunctionOf(context, MERGER_NAME, merger, vr, superOf(k), equalTo(vr), superOf(vr));
        final var userMerg = new UserMerger(merg, tags);
        final var sessionStore = validateSessionStore(store(), k, vr);
        final var mat = materializedOf(context, sessionStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = named != null
                ? mat != null
                ? input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, named, mat)
                : input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, named)
                : mat != null
                ? input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg, mat)
                : input.sessionWindowedKStream.aggregate(userInit, userAggr, userMerg);
        return new KTableWrapper((KTable) output, windowedK, vr);
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
        final var windowedK = windowedTypeOf(k);
        final var vr = streamDataTypeOf(firstSpecificType(initializer, aggregator), false);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, vr);
        final var userInit = new UserInitializer(init, tags);
        final var aggr = userFunctionOf(context, AGGREGATOR_NAME, aggregator, vr, superOf(k), superOf(v), superOf(vr));
        final var userAggr = new UserAggregator(aggr, tags);
        final var windowStore = validateWindowStore(store(), k, vr);
        final var mat = materializedOf(context, windowStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = named != null
                ? mat != null
                ? input.timeWindowedKStream.aggregate(userInit, userAggr, named, mat)
                : input.timeWindowedKStream.aggregate(userInit, userAggr, named)
                : mat != null
                ? input.timeWindowedKStream.aggregate(userInit, userAggr, mat)
                : input.timeWindowedKStream.aggregate(userInit, userAggr);
        return new KTableWrapper((KTable) output, windowedK, vr);
    }

    @Override
    public StreamWrapper apply(CogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*
         *    Kafka Streams method signature:
         *    KTable<K, VOut> aggregate(
         *          final Initializer<VOut> initializer,
         *          final Named named,
         *          final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var vout = input.valueType();
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, vout);
        final var userInit = new UserInitializer(init, tags);
        final var kvStore = validateKeyValueStore(store(), k, vout);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = named != null
                ? mat != null
                ? input.cogroupedStream.aggregate(userInit, named, mat)
                : input.cogroupedStream.aggregate(userInit, named)
                : mat != null
                ? input.cogroupedStream.aggregate(userInit, mat)
                : input.cogroupedStream.aggregate(userInit);
        return new KTableWrapper(output, k, vout);
    }

    @Override
    public StreamWrapper apply(SessionWindowedCogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*
         *    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> aggregate(
         *          final Initializer<V> initializer,
         *          final Merger<? super K, V> sessionMerger,
         *          final Named named,
         *          final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var windowedK = windowedTypeOf(k);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, v);
        final var userInit = new UserInitializer(init, tags);
        final var merg = userFunctionOf(context, MERGER_NAME, merger, v);
        final var userMerg = new UserMerger(merg, tags);
        final var sessionStore = validateSessionStore(store(), k, v);
        final var mat = materializedOf(context, sessionStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = named != null
                ? mat != null
                ? input.sessionWindowedCogroupedKStream.aggregate(userInit, userMerg, named, mat)
                : input.sessionWindowedCogroupedKStream.aggregate(userInit, userMerg, named)
                : mat != null
                ? input.sessionWindowedCogroupedKStream.aggregate(userInit, userMerg, mat)
                : input.sessionWindowedCogroupedKStream.aggregate(userInit, userMerg);
        return new KTableWrapper((KTable) output, windowedK, v);
    }

    @Override
    public StreamWrapper apply(TimeWindowedCogroupedKStreamWrapper input, TopologyBuildContext context) {
        /*
         *    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> aggregate(
         *          final Initializer<V> initializer,
         *          final Named named,
         *          final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var windowedK = windowedTypeOf(k);
        final var init = userFunctionOf(context, INITIALIZER_NAME, initializer, v);
        final var userInit = new UserInitializer(init, tags);
        final var kvStore = validateWindowStore(store(), k, v);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = named != null
                ? mat != null
                ? input.timeWindowedCogroupedKStream.aggregate(userInit, named, mat)
                : input.timeWindowedCogroupedKStream.aggregate(userInit, named)
                : mat != null
                ? input.timeWindowedCogroupedKStream.aggregate(userInit, mat)
                : input.timeWindowedCogroupedKStream.aggregate(userInit);
        return new KTableWrapper((KTable) output, windowedK, v);
    }
}
