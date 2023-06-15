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


import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.user.UserAggregator;
import io.axual.ksml.user.UserFunction;
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
    private final UserFunction initializer;
    private final UserFunction aggregator;
    private final UserFunction merger;
    private final UserFunction adder;
    private final UserFunction subtractor;

    public AggregateOperation(StoreOperationConfig config, UserFunction initializer, UserFunction aggregator, UserFunction merger, UserFunction adder, UserFunction subtractor) {
        super(config);
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.merger = merger;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(initializer.resultType, false);
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        final var kvStore = validateKeyValueStore(store, k, vr);

        final var init = new UserInitializer(initializer);
        final var aggr = new UserAggregator(aggregator);
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.groupedStream.aggregate(init, aggr, named, mat)
                    : (KTable) input.groupedStream.aggregate(init, aggr, mat);
            return new KTableWrapper(output, k, vr);
        }

        // Method signature using Named without Materialized does not exist
        final var output = (KTable) input.groupedStream.aggregate(init, aggr);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<K, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> adder,
         *          final Aggregator<? super K, ? super V, VR> subtractor,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(initializer.resultType, false);
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(ADDER_NAME, adder, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        checkFunction(SUBTRACTOR_NAME, subtractor, equalTo(vr), superOf(k), superOf(vr), superOf(vr));
        final var kvStore = validateKeyValueStore(store, k, vr);

        final var init = new UserInitializer(initializer);
        final var add = new UserAggregator(adder);
        final var sub = new UserAggregator(subtractor);
        final var named = name != null ? Named.as(name) : null;

        if (kvStore != null) {
            final var mat = materialize(kvStore);
            final var output = named != null
                    ? (KTable) input.groupedTable.aggregate(init, add, sub, named, mat)
                    : (KTable) input.groupedTable.aggregate(init, add, sub, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.groupedTable.aggregate(init, add, sub, named)
                : (KTable) input.groupedTable.aggregate(init, add, sub);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    <VR> KTable<Windowed<K>, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Merger<? super K, VR> sessionMerger,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(initializer.resultType, false);
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        checkFunction(MERGER_NAME, merger, equalTo(vr), superOf(k), equalTo(vr), superOf(vr));
        final var sessionStore = validateSessionStore(store, k, vr);

        final var init = new UserInitializer(initializer);
        final var aggr = new UserAggregator(aggregator);
        final var merg = new UserMerger(merger);
        final var named = name != null ? Named.as(name) : null;

        if (sessionStore != null) {
            final var mat = materialize(sessionStore);
            final var output = named != null
                    ? (KTable) input.sessionWindowedKStream.aggregate(init, aggr, merg, named, mat)
                    : (KTable) input.sessionWindowedKStream.aggregate(init, aggr, merg, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.sessionWindowedKStream.aggregate(init, aggr, merg, named)
                : (KTable) input.sessionWindowedKStream.aggregate(init, aggr, merg);
        return new KTableWrapper(output, k, vr);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        /*
         *    Kafka Streams method signature:
         *    <VR> KTable<Windowed<K>, VR> aggregate(
         *          final Initializer<VR> initializer,
         *          final Aggregator<? super K, ? super V, VR> aggregator,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var vr = streamDataTypeOf(initializer.resultType, false);
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        final var windowStore = validateWindowStore(store, k, vr);

        final var init = new UserInitializer(initializer);
        final var aggr = new UserAggregator(aggregator);
        final var named = name != null ? Named.as(name) : null;

        if (windowStore != null) {
            final var mat = materialize(windowStore);
            final var output = named != null
                    ? (KTable) input.timeWindowedKStream.aggregate(init, aggr, named, mat)
                    : (KTable) input.timeWindowedKStream.aggregate(init, aggr, mat);
            return new KTableWrapper(output, k, vr);
        }

        final var output = named != null
                ? (KTable) input.timeWindowedKStream.aggregate(init, aggr, named)
                : (KTable) input.timeWindowedKStream.aggregate(init, aggr);
        return new KTableWrapper(output, k, vr);
    }
}
