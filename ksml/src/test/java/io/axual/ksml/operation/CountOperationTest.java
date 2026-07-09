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
import io.axual.ksml.stream.KTableWrapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.sessionStore;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.windowStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

class CountOperationTest {

    private CountOperation operation() {
        return new CountOperation(storeConfig("count"));
    }

    @Test
    void applyToGroupedStreamReturnsTable() {
        assertThat(operation().apply(groupedStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedTableReturnsTable() {
        assertThat(operation().apply(groupedTable(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToSessionWindowedReturnsTable() {
        assertThat(operation().apply(sessionWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToTimeWindowedReturnsTable() {
        assertThat(operation().apply(timeWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(groupedStream(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    void applyToGroupedTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new CountOperation(storeConfig("count", store));
        final var context = mockContext();

        assertThat(operation.apply(groupedTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
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
