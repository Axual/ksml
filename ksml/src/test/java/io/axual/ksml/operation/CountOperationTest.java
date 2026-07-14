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

import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
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

class CountOperationTest {

    private CountOperation operation() {
        return new CountOperation(storeConfig("count"));
    }

    @Test
    @DisplayName("count on a grouped stream delegates to KGroupedStream.count and returns a table")
    @SuppressWarnings("unchecked")
    void applyToGroupedStreamCallsCount() {
        final KGroupedStream<Object, Object> grouped = mock(KGroupedStream.class);
        final var input = new KGroupedStreamWrapper(grouped, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(grouped).count(any(Named.class));
    }

    @Test
    @DisplayName("count on a grouped table delegates to KGroupedTable.count and returns a table")
    @SuppressWarnings("unchecked")
    void applyToGroupedTableCallsCount() {
        final KGroupedTable<Object, Object> grouped = mock(KGroupedTable.class);
        final var input = new KGroupedTableWrapper(grouped, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(grouped).count(any(Named.class));
    }

    @Test
    @DisplayName("count on a session-windowed stream delegates to SessionWindowedKStream.count and returns a table")
    @SuppressWarnings("unchecked")
    void applyToSessionWindowedCallsCount() {
        final SessionWindowedKStream<Object, Object> windowed = mock(SessionWindowedKStream.class);
        final var input = new SessionWindowedKStreamWrapper(windowed, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(windowed).count(any(Named.class));
    }

    @Test
    @DisplayName("count on a time-windowed stream delegates to TimeWindowedKStream.count and returns a table")
    @SuppressWarnings("unchecked")
    void applyToTimeWindowedCallsCount() {
        final TimeWindowedKStream<Object, Object> windowed = mock(TimeWindowedKStream.class);
        final var input = new TimeWindowedKStreamWrapper(windowed, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(windowed).count(any(Named.class));
    }

    @Test
    @DisplayName("count on a grouped stream with a store materializes that store")
    void applyToGroupedStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(groupedStream(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("count on a grouped table with a store materializes that store")
    void applyToGroupedTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(groupedTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("count on a session-windowed stream materializes the session store by name")
    void applyToSessionWindowedWithStoreMaterializes() {
        final var store = sessionStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(sessionWindowed(), context)).isInstanceOf(KTableWrapper.class);
        final var materialized = ArgumentCaptor.forClass(SessionStateStoreDefinition.class);
        verify(context).materialize(materialized.capture());
        assertThat(materialized.getValue().name()).isEqualTo("store");
    }

    @Test
    @DisplayName("count on a time-windowed stream materializes the window store by name")
    void applyToTimeWindowedWithStoreMaterializes() {
        final var store = windowStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(timeWindowed(), context)).isInstanceOf(KTableWrapper.class);
        final var materialized = ArgumentCaptor.forClass(WindowStateStoreDefinition.class);
        verify(context).materialize(materialized.capture());
        assertThat(materialized.getValue().name()).isEqualTo("store");
    }
}
