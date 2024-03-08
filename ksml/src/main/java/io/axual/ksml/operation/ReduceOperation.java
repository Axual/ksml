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
import io.axual.ksml.user.UserReducer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;

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
        final var red = userFunctionOf(context, REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(red);
        final var kvStore = validateKeyValueStore(store(), k, v);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = mat != null
                ? named != null
                ? input.groupedStream.reduce(userRed, named, mat)
                : input.groupedStream.reduce(userRed, mat)
                : input.groupedStream.reduce(userRed);
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
        final var add = userFunctionOf(context, ADDER_NAME, adder, v, equalTo(v), equalTo(v));
        final var userAdd = new UserReducer(add);
        final var sub = userFunctionOf(context, SUBTRACTOR_NAME, subtractor, v, equalTo(v), equalTo(v));
        final var userSub = new UserReducer(sub);
        final var kvStore = validateKeyValueStore(store(), k, v);
        final var mat = materializedOf(context, kvStore);
        final var named = namedOf();
        final KTable<Object, Object> output = mat != null
                ? named != null
                ? input.groupedTable.reduce(userAdd, userSub, named, mat)
                : input.groupedTable.reduce(userAdd, userSub, mat)
                : input.groupedTable.reduce(userAdd, userSub);
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
        final var windowedK = windowedTypeOf(k);
        final var red = userFunctionOf(context, REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(red);
        final var sessionStore = validateSessionStore(store(), k, v);
        final var mat = materializedOf(context, sessionStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = mat != null
                ? named != null
                ? input.sessionWindowedKStream.reduce(userRed, named, mat)
                : input.sessionWindowedKStream.reduce(userRed, mat)
                : input.sessionWindowedKStream.reduce(userRed);
        return new KTableWrapper((KTable) output, windowedK, v);
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
        final var windowedK = windowedTypeOf(k);
        final var red = userFunctionOf(context, REDUCER_NAME, reducer, v, equalTo(v), equalTo(v));
        final var userRed = new UserReducer(red);
        final var windowStore = validateWindowStore(store(), k, v);
        final var mat = materializedOf(context, windowStore);
        final var named = namedOf();
        final KTable<Windowed<Object>, Object> output = mat != null
                ? named != null
                ? input.timeWindowedKStream.reduce(userRed, named, mat)
                : input.timeWindowedKStream.reduce(userRed, mat)
                : input.timeWindowedKStream.reduce(userRed);
        return new KTableWrapper((KTable) output, windowedK, v);
    }
}
