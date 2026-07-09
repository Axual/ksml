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

import io.axual.ksml.stream.KTableWrapper;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.reducer;
import static io.axual.ksml.operation.OperationTestSupport.sessionStore;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.windowStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

class ReduceOperationTest {

    @Test
    void applyToGroupedStreamReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());
        final var context = mockContext();

        final var result = operation.apply(groupedStream(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).createUserFunction(any());
    }

    @Test
    void applyToGroupedStreamWithNameUsesNamed() {
        final var operation = new ReduceOperation(storeConfig("myReduce"), reducer());

        final var result = operation.apply(groupedStream(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedTableReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer(), reducer());

        final var result = operation.apply(groupedTable(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToSessionWindowedReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());

        final var result = operation.apply(sessionWindowed(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToTimeWindowedReturnsTable() {
        final var operation = new ReduceOperation(storeConfig("reduce"), reducer());

        final var result = operation.apply(timeWindowed(), mockContext());

        assertThat(result).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(groupedStream(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    void applyToGroupedTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer(), reducer());
        final var context = mockContext();

        final var result = operation.apply(groupedTable(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    void applyToSessionWindowedWithStoreMaterializes() {
        final var store = sessionStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(sessionWindowed(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    void applyToTimeWindowedWithStoreMaterializes() {
        final var store = windowStore("store");
        final var operation = new ReduceOperation(storeConfig("reduce", store), reducer());
        final var context = mockContext();

        final var result = operation.apply(timeWindowed(), context);

        assertThat(result).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
