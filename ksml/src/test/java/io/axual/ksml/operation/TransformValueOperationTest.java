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

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.valueTransformer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

class TransformValueOperationTest {

    private TransformValueOperation operation() {
        return new TransformValueOperation(storeConfig("mapValues"), valueTransformer());
    }

    @Test
    @DisplayName("transform value on a stream produces a KStream result")
    void applyToStreamReturnsStream() {
        assertThat(operation().apply(kStream(), mockContext())).isInstanceOf(KStreamWrapper.class);
    }

    @Test
    @DisplayName("transform value on a table produces a KTable result")
    void applyToTableReturnsTable() {
        assertThat(operation().apply(kTable(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("transform value on a table with a configured store materializes that store")
    void applyToTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new TransformValueOperation(storeConfig("mapValues", store), valueTransformer());
        final var context = mockContext();

        assertThat(operation.apply(kTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
