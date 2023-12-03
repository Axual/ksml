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
import io.axual.ksml.user.UserReducer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

public class ReduceOperation extends StoreOperation {
    private static final String ADDER_NAME = "Adder";
    private static final String REDUCER_NAME = "Reducer";
    private static final String SUBTRACTOR_NAME = "Subtractor";
    private final FunctionDefinition reducer;
    private final FunctionDefinition adder;
    private final FunctionDefinition subtractor;

    public ReduceOperation(StoreOperationConfig config, FunctionDefinition reducer, FunctionDefinition adder, FunctionDefinition subtractor) {
        super(config);
        this.reducer = reducer;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var red = checkFunction(REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(context.createUserFunction(red));
        final var kvStore = validateKeyValueStore(store(), k, v);

        if (kvStore != null) {
            final var mat = context.materialize(kvStore);
            final var output = name != null
                    ? input.groupedStream.reduce(userRed, Named.as(name), mat)
                    : input.groupedStream.reduce(userRed, mat);
            return new KTableWrapper(output, k, v);
        }

        final var output = input.groupedStream.reduce(userRed);
        return new KTableWrapper(output, k, v);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> reduce(
         *          final Reducer<V> adder,
         *          final Reducer<V> subtractor,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var add = checkFunction(ADDER_NAME, adder, v, equalTo(v), equalTo(v));
        final var sub = checkFunction(SUBTRACTOR_NAME, subtractor, v, equalTo(v), equalTo(v));
        final var userAdd = new UserReducer(context.createUserFunction(add));
        final var userSub = new UserReducer(context.createUserFunction(sub));
        final var kvStore = validateKeyValueStore(store(), k, v);

        if (kvStore != null) {
            final var mat = context.materialize(kvStore);
            final var output = name != null
                    ? input.groupedTable.reduce(userAdd, userSub, Named.as(name), mat)
                    : input.groupedTable.reduce(userAdd, userSub, mat);
            return new KTableWrapper(output, k, v);
        }

        final var output = input.groupedTable.reduce(userAdd, userSub);
        return new KTableWrapper(output, k, v);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> reduce(
         *          final Reducer<V> reducer,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var windowedK = context.windowedTypeOf(k);
        final var red = checkFunction(REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(context.createUserFunction(red));
        final var sessionStore = validateSessionStore(store(), windowedK, v);

        if (sessionStore != null) {
            final var mat = context.materialize(sessionStore);
            final var output = name != null
                    ? (KTable) input.sessionWindowedKStream.reduce(userRed, Named.as(name), mat)
                    : (KTable) input.sessionWindowedKStream.reduce(userRed, mat);
            return new KTableWrapper(output, windowedK, v);
        }

        final var output = (KTable) input.sessionWindowedKStream.reduce(userRed);
        return new KTableWrapper(output, windowedK, v);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> reduce(
         *          final Reducer<V> reducer,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var windowedK = context.windowedTypeOf(k);
        final var red = checkFunction(REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(context.createUserFunction(red));
        final var windowStore = validateWindowStore(store(), windowedK, v);

        if (windowStore != null) {
            final var mat = context.materialize(windowStore);
            final var output = name != null
                    ? (KTable) input.timeWindowedKStream.reduce(userRed, Named.as(name), mat)
                    : (KTable) input.timeWindowedKStream.reduce(userRed, mat);
            return new KTableWrapper(output, windowedK, v);
        }

        final var output = (KTable) input.timeWindowedKStream.reduce(userRed);
        return new KTableWrapper(output, windowedK, v);
    }
}
