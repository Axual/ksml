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


import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.generator.StreamDataType;
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
         *          final Merger<? super K, VR> sessionMerger,
         *          final Named named,
         *          final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
         */

        checkNotNull(initializer, INITIALIZER_NAME.toLowerCase());
        final var k = input.keyType().userType().dataType();
        final var v = input.valueType().userType().dataType();
        final var vr = initializer.resultType.dataType();
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        checkFunction(MERGER_NAME, merger, equalTo(vr), superOf(k), equalTo(vr), superOf(vr));

        final StreamDataType resultValueType = streamDataTypeOf(aggregator.resultType, false);

        return new KTableWrapper(
                input.groupedStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        Named.as(name),
                        registerKeyValueStore(input.keyType(), resultValueType)),
                input.keyType(),
                resultValueType);
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
        final var k = input.keyType().userType().dataType();
        final var v = input.valueType().userType().dataType();
        final var vr = initializer.resultType.dataType();
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(ADDER_NAME, adder, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        checkFunction(SUBTRACTOR_NAME, subtractor, equalTo(vr), superOf(k), equalTo(vr), superOf(vr));

        final StreamDataType resultValueType = streamDataTypeOf(adder.resultType, false);

        return new KTableWrapper(
                input.groupedTable.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(adder),
                        new UserAggregator(subtractor),
                        Named.as(name),
                        registerKeyValueStore(input.keyType(), resultValueType)),
                input.keyType(),
                resultValueType);
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
        final var k = input.keyType().userType().dataType();
        final var v = input.valueType().userType().dataType();
        final var vr = initializer.resultType.dataType();
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));
        checkFunction(MERGER_NAME, merger, equalTo(vr), superOf(k), equalTo(vr), superOf(vr));

        final StreamDataType resultValueType = streamDataTypeOf(merger.resultType, false);

        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        new UserMerger(merger),
                        Named.as(name),
                        registerSessionStore(input.keyType(), resultValueType)),
                windowedTypeOf(input.keyType()),
                resultValueType);
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
        final var k = input.keyType().userType().dataType();
        final var v = input.valueType().userType().dataType();
        final var vr = initializer.resultType.dataType();
        checkFunction(INITIALIZER_NAME, initializer, equalTo(vr));
        checkFunction(AGGREGATOR_NAME, aggregator, equalTo(vr), superOf(k), superOf(v), superOf(vr));

        final StreamDataType resultValueType = streamDataTypeOf(aggregator.resultType, false);

        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        Named.as(name),
                        registerWindowStore(input.keyType(), resultValueType)),
                windowedTypeOf(input.keyType()),
                resultValueType);
    }
}
