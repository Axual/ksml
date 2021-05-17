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

import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.type.WindowType;
import io.axual.ksml.user.UserAggregator;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserInitializer;
import io.axual.ksml.user.UserMerger;

public class AggregateOperation extends BaseOperation {
    private final UserFunction initializer;
    private final UserFunction aggregator;
    private final UserFunction merger;
    private final UserFunction adder;
    private final UserFunction subtractor;

    public AggregateOperation(String name, UserFunction initializer, UserFunction aggregator, UserFunction merger, UserFunction adder, UserFunction subtractor) {
        super(name);
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.merger = merger;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        return new KTableWrapper(input.groupedStream.aggregate(
                new UserInitializer(initializer),
                new UserAggregator(aggregator),
                Named.as(name),
                Materialized.as(name)),
                input.keyType,
                input.valueType);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        return new KTableWrapper(input.groupedTable.aggregate(
                new UserInitializer(initializer),
                new UserAggregator(adder),
                new UserAggregator(subtractor),
                Named.as(name),
                Materialized.as(name)),
                input.keyType,
                input.valueType);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        new UserMerger(merger),
                        Named.as(name),
                        Materialized.as(name)),
                StreamDataType.of(new WindowType(input.keyType.type), true),
                input.valueType);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.aggregate(
                        new UserInitializer(initializer),
                        new UserAggregator(aggregator),
                        Named.as(name),
                        Materialized.as(name)),
                StreamDataType.of(new WindowType(input.keyType.type), true),
                input.valueType);
    }
}
