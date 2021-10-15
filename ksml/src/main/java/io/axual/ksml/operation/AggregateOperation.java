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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.stream.BaseStreamWrapper;
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

    private void checkAggregationFunction(UserFunction function, BaseStreamWrapper input, String name) {
        checkAssignable(function.parameters[0].type, input.keyType.type(), "Stream key type incompatible with " + name + "'s first parameter");
        checkAssignable(function.parameters[1].type, input.valueType.type(), "Stream value type incompatible with " + name + "'s second parameter");
        checkAssignable(function.parameters[2].type, initializer.resultType.type(), "Initializer result type is incompatible with " + name + "'s third parameter");
        checkAssignable(function.parameters[2].type, function.resultType.type(), name + " result type is incompatible with its third parameter");
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        checkNotNull(initializer, "initializer");
        checkNotNull(aggregator, "aggregator");
        checkAggregationFunction(aggregator, input, "Aggregator");

        return new KTableWrapper(
                input.groupedStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                input.keyType,
                streamDataTypeOf(aggregator.resultType, false));
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        checkNotNull(initializer, "initializer");
        checkNotNull(adder, "adder");
        checkNotNull(subtractor, "subtractor");
        checkAggregationFunction(adder, input, "Adder");
        checkAggregationFunction(subtractor, input, "Subtractor");

        return new KTableWrapper(
                input.groupedTable.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(adder),
                        new UserAggregator(subtractor),
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                input.keyType,
                streamDataTypeOf(adder.resultType, false));
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        checkNotNull(initializer, "initializer");
        checkNotNull(aggregator, "aggregator");
        checkNotNull(merger, "merger");
        checkAggregationFunction(aggregator, input, "Aggregator");
        checkAssignable(merger.parameters[0].type, input.keyType.type(), "Stream key type incompatible with Merger's first parameter");
        checkEqual(aggregator.resultType.type(), merger.parameters[1].type, "Aggregator result type is incompatible with Merger's second parameter");

        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        new UserMerger(merger),
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                streamDataTypeOf(new StaticUserType(new WindowedType(input.keyType.type()), input.keyType.notation().name()), true),
                streamDataTypeOf(aggregator.resultType, false));
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        checkNotNull(initializer, "initializer");
        checkNotNull(aggregator, "aggregator");
        checkAggregationFunction(aggregator, input, "Aggregator");

        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                streamDataTypeOf(new StaticUserType(new WindowedType(input.keyType.type()), input.keyType.notation().name()), true),
                streamDataTypeOf(aggregator.resultType, false));
    }
}
