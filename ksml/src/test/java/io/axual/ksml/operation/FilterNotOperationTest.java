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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.predicate;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class FilterNotOperationTest {

    private FilterNotOperation operation() {
        return new FilterNotOperation(storeConfig("filterNot"), predicate());
    }

    @Test
    @DisplayName("filterNot on a stream returns a stream")
    void applyToStreamReturnsStream() {
        // On a stream, filter and filterNot both delegate to KStream.processValues; the difference
        // is the supplied processor (FilterProcessor vs FilterNotProcessor), not a KStream method,
        // so only the resulting wrapper type can be asserted here.
        assertThat(operation().apply(kStream(), mockContext())).isInstanceOf(KStreamWrapper.class);
    }

    @Test
    @DisplayName("filterNot on a table delegates to KTable.filterNot and never to filter")
    @SuppressWarnings("unchecked")
    void applyToTableCallsFilterNot() {
        // A table filterNot must delegate to KTable.filterNot, never KTable.filter.
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(table).filterNot(any(Predicate.class), any(Named.class));
        verify(table, never()).filter(any(Predicate.class), any(Named.class));
    }

    @Test
    @DisplayName("filterNot on a table with a store materializes that store")
    void applyToTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new FilterNotOperation(storeConfig("filterNot", store), predicate());
        final var context = mockContext();

        assertThat(operation.apply(kTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
