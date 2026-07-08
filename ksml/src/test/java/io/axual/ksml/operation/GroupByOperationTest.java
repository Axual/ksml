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
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.keyValueMapper;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.tupleKeyValueMapper;
import static org.assertj.core.api.Assertions.assertThat;

class GroupByOperationTest {

    @Test
    void applyToStreamReturnsGroupedStream() {
        final var operation = new GroupByOperation(storeConfig("groupBy"), keyValueMapper());
        assertThat(operation.apply(kStream(), mockContext())).isInstanceOf(KGroupedStreamWrapper.class);
    }

    @Test
    void applyToTableReturnsGroupedTable() {
        // KTable groupBy requires the selector to produce a (key, value) tuple.
        final var operation = new GroupByOperation(storeConfig("groupBy"), tupleKeyValueMapper());
        assertThat(operation.apply(kTable(), mockContext())).isInstanceOf(KGroupedTableWrapper.class);
    }
}
