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
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserReducer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

public class ReduceOperation extends StoreOperation {
    private static final String ADDER_NAME = "Adder";
    private static final String REDUCER_NAME = "Reducer";
    private static final String SUBTRACTOR_NAME = "Subtractor";
    private final UserFunction reducer;
    private final UserFunction adder;
    private final UserFunction subtractor;

    public ReduceOperation(StoreOperationConfig config, UserFunction reducer, UserFunction adder, UserFunction subtractor) {
        super(config);
        this.reducer = reducer;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> reduce(
         *          final Reducer<V> reducer,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        checkFunction(REDUCER_NAME, reducer, equalTo(v), equalTo(v), equalTo(v));
        final var kvStore = validateKeyValueStore(store, k, v);

        if (kvStore != null) {
            final var mat = materialize(kvStore);
            final var output = name != null
                    ? input.groupedStream.reduce(new UserReducer(reducer), Named.as(name), mat)
                    : input.groupedStream.reduce(new UserReducer(reducer), mat);
            return new KTableWrapper(output, k, v);
        }

        final var output = input.groupedStream.reduce(new UserReducer(reducer));
        return new KTableWrapper(output, k, v);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> reduce(
         *          final Reducer<V> adder,
         *          final Reducer<V> subtractor,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        checkFunction(ADDER_NAME, adder, equalTo(v), equalTo(v), equalTo(v));
        checkFunction(SUBTRACTOR_NAME, subtractor, equalTo(v), equalTo(v), equalTo(v));
        final var kvStore = validateKeyValueStore(store, k, v);

        if (kvStore != null) {
            final var mat = materialize(kvStore);
            final var output = name != null
                    ? input.groupedTable.reduce(new UserReducer(reducer), new UserReducer(subtractor), Named.as(name), mat)
                    : input.groupedTable.reduce(new UserReducer(reducer), new UserReducer(subtractor), mat);
            return new KTableWrapper(output, k, v);
        }

        final var output = input.groupedTable.reduce(new UserReducer(reducer), new UserReducer(subtractor));
        return new KTableWrapper(output, k, v);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> reduce(
         *          final Reducer<V> reducer,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var windowedK = windowedTypeOf(k);
        checkFunction(REDUCER_NAME, reducer, equalTo(v), equalTo(v), equalTo(v));

        final var sessionStore = validateSessionStore(store, windowedK, v);

        if (sessionStore != null) {
            final var mat = materialize(sessionStore);
            final var output = name != null
                    ? (KTable) input.sessionWindowedKStream.reduce(new UserReducer(reducer), Named.as(name), mat)
                    : (KTable) input.sessionWindowedKStream.reduce(new UserReducer(reducer), mat);
            return new KTableWrapper(output, windowedK, v);
        }

        final var output = (KTable) input.sessionWindowedKStream.reduce(new UserReducer(reducer));
        return new KTableWrapper(output, windowedK, v);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<Windowed<K>, V> reduce(
         *          final Reducer<V> reducer,
         *          final Named named,
         *          final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
         */

        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        final var windowedK = windowedTypeOf(k);
        checkFunction(REDUCER_NAME, reducer, equalTo(v), equalTo(v), equalTo(v));

        final var windowStore = validateWindowStore(store, windowedK, v);

        if (windowStore != null) {
            final var mat = materialize(windowStore);
            final var output = name != null
                    ? (KTable) input.timeWindowedKStream.reduce(new UserReducer(reducer), Named.as(name), mat)
                    : (KTable) input.timeWindowedKStream.reduce(new UserReducer(reducer), mat);
            return new KTableWrapper(output, windowedK, v);
        }

        final var output = (KTable) input.timeWindowedKStream.reduce(new UserReducer(reducer));
        return new KTableWrapper(output, windowedK, v);
    }
}
