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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.aggregator;
import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.initializer;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.merger;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.sessionStore;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowedCogrouped;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.windowStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AggregateOperationTest {

    private AggregateOperation operation() {
        return new AggregateOperation(storeConfig("aggregate"), initializer(), aggregator(), merger(), aggregator(), aggregator());
    }

    private AggregateOperation operation(io.axual.ksml.definition.StateStoreDefinition store) {
        return new AggregateOperation(storeConfig("aggregate", store), initializer(), aggregator(), merger(), aggregator(), aggregator());
    }

    @Test
    @DisplayName("applying to a grouped stream invokes aggregate and returns a KTable")
    @SuppressWarnings("unchecked")
    void applyToGroupedStreamCallsAggregate() {
        final KGroupedStream<Object, Object> grouped = mock(KGroupedStream.class);
        final var input = new KGroupedStreamWrapper(grouped, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(grouped).aggregate(any(Initializer.class), any(Aggregator.class));
    }

    @Test
    @DisplayName("applying to a grouped table invokes aggregate and returns a KTable")
    @SuppressWarnings("unchecked")
    void applyToGroupedTableCallsAggregate() {
        final KGroupedTable<Object, Object> grouped = mock(KGroupedTable.class);
        final var input = new KGroupedTableWrapper(grouped, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(grouped).aggregate(any(Initializer.class), any(Aggregator.class), any(Aggregator.class), any(Named.class));
    }

    @Test
    @DisplayName("applying to a session-windowed stream returns a KTable")
    void applyToSessionWindowedReturnsTable() {
        assertThat(operation().apply(sessionWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("applying to a time-windowed stream returns a KTable")
    void applyToTimeWindowedReturnsTable() {
        assertThat(operation().apply(timeWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("applying to a cogrouped stream returns a KTable")
    void applyToCogroupedStreamReturnsTable() {
        assertThat(operation().apply(cogroupedStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("applying to a time-windowed cogrouped stream returns a KTable")
    void applyToTimeWindowedCogroupedReturnsTable() {
        assertThat(operation().apply(timeWindowedCogrouped(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("applying to a grouped stream with a store materializes that store")
    void applyToGroupedStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var context = mockContext();

        assertThat(operation(store).apply(groupedStream(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("applying to a grouped table with a store materializes that store")
    void applyToGroupedTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var context = mockContext();

        assertThat(operation(store).apply(groupedTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("applying to a session-windowed stream with a store materializes that store")
    void applyToSessionWindowedWithStoreMaterializes() {
        final var store = sessionStore("store");
        final var context = mockContext();

        assertThat(operation(store).apply(sessionWindowed(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("applying to a time-windowed stream with a store materializes that store")
    void applyToTimeWindowedWithStoreMaterializes() {
        final var store = windowStore("store");
        final var context = mockContext();

        assertThat(operation(store).apply(timeWindowed(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
