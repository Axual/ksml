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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

class ToTableOperationTest extends OperationTestBase {

    @Test
    @DisplayName("toTable on a stream produces a KTable result")
    void applyToStreamReturnsTable() {
        final var operation = new ToTableOperation(storeConfig("toTable"));
        assertThat(operation.apply(kStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("toTable on a stream with a configured store materializes that store")
    void applyToStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ToTableOperation(storeConfig("toTable", store));
        final var context = mockContext();

        assertThat(operation.apply(kStream(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
