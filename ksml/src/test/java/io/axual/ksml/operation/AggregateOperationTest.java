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

import static io.axual.ksml.operation.OperationTestSupport.aggregator;
import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.initializer;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.merger;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.sessionStore;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowedCogrouped;
import static io.axual.ksml.operation.OperationTestSupport.windowStore;
import static org.assertj.core.api.Assertions.assertThat;

class AggregateOperationTest {

    private AggregateOperation operation() {
        return new AggregateOperation(storeConfig("aggregate"), initializer(), aggregator(), merger(), aggregator(), aggregator());
    }

    private AggregateOperation operation(io.axual.ksml.definition.StateStoreDefinition store) {
        return new AggregateOperation(storeConfig("aggregate", store), initializer(), aggregator(), merger(), aggregator(), aggregator());
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
    void applyToCogroupedStreamReturnsTable() {
        assertThat(operation().apply(cogroupedStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToTimeWindowedCogroupedReturnsTable() {
        assertThat(operation().apply(timeWindowedCogrouped(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedStreamWithStoreMaterializes() {
        assertThat(operation(keyValueStore("store")).apply(groupedStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToGroupedTableWithStoreMaterializes() {
        assertThat(operation(keyValueStore("store")).apply(groupedTable(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToSessionWindowedWithStoreMaterializes() {
        assertThat(operation(sessionStore("store")).apply(sessionWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    void applyToTimeWindowedWithStoreMaterializes() {
        assertThat(operation(windowStore("store")).apply(timeWindowed(), mockContext())).isInstanceOf(KTableWrapper.class);
    }
}
