package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.reducer;
import static io.axual.ksml.operation.OperationTestSupport.sessionStore;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.windowStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ReduceOperationTest {

    @Test
    @DisplayName("reduce on a grouped stream returns a KTable and registers the reducer user function")
    @SuppressWarnings("unchecked")
    void applyToGroupedStreamCallsReduce() {
        final KGroupedStream<Object, Object> grouped = mock(KGroupedStream.class);
        final var input = new KGroupedStreamWrapper(grouped, key(), value());
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());
        final var context = mockContext();

        assertThat(operation.apply(input, context)).isInstanceOf(KTableWrapper.class);
        verify(grouped).reduce(any(Reducer.class));
        verify(context).createUserFunction(any());
    }

    @Test
    @DisplayName("reduce on a named grouped stream returns a KTable")
    void applyToGroupedStreamWithNameUsesNamed() {
        final var operation = new ReduceOperation(storeConfig("myReduce"), reducer());

        final var result = operation.apply(groupedStream(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("reduce on a grouped table returns a KTable using the adder and subtractor reducers")
    @SuppressWarnings("unchecked")
    void applyToGroupedTableCallsReduce() {
        final KGroupedTable<Object, Object> grouped = mock(KGroupedTable.class);
        final var input = new KGroupedTableWrapper(grouped, key(), value());
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer(), reducer());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(grouped).reduce(any(Reducer.class), any(Reducer.class));
    }

    @Test
    @DisplayName("reduce on a session windowed stream returns a KTable")
    void applyToSessionWindowedReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());

        final var result = operation.apply(sessionWindowed(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("reduce on a time windowed stream returns a KTable")
    void applyToTimeWindowedReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());

        final var result = operation.apply(timeWindowed(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("reduce on a grouped stream with a configured store materializes that store")
    void applyToGroupedStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(groupedStream(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("reduce on a grouped table with a configured store materializes that store")
    void applyToGroupedTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer(), reducer());
        final var context = mockContext();

        final var result = operation.apply(groupedTable(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("reduce on a session windowed stream with a configured session store materializes that store")
    void applyToSessionWindowedWithStoreMaterializes() {
        final var store = sessionStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(sessionWindowed(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("reduce on a time windowed stream with a configured window store materializes that store")
    void applyToTimeWindowedWithStoreMaterializes() {
        final var store = windowStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(timeWindowed(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
