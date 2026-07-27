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

import io.axual.ksml.operation.processor.FilterProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
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

class FilterOperationTest extends OperationTestBase {

    private FilterOperation operation() {
        return new FilterOperation(storeConfig("filter"), predicate());
    }

    @Test
    @DisplayName("filter on a stream wires a FilterProcessor through processValues")
    @SuppressWarnings("unchecked")
    void applyToStreamWiresFilterProcessor() {
        // On a stream, filter and filterNot both delegate to KStream.processValues; what differs is the
        // supplied processor, so capture the supplier and assert it produces a FilterProcessor.
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);

        final var captor = ArgumentCaptor.<FixedKeyProcessorSupplier<Object, Object, Object>>captor();
        verify(stream).processValues(captor.capture(), any(Named.class), any(String[].class));
        assertThat(captor.getValue().get()).isInstanceOf(FilterProcessor.class);
    }

    @Test
    @DisplayName("filter on a table delegates to KTable.filter and never to filterNot")
    @SuppressWarnings("unchecked")
    void applyToTableCallsFilter() {
        // A table filter must delegate to KTable.filter, never KTable.filterNot.
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        verify(table).filter(any(Predicate.class), any(Named.class));
        verify(table, never()).filterNot(any(Predicate.class), any(Named.class));
    }

    @Test
    @DisplayName("filter on a table with a store materializes that store")
    void applyToTableWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new FilterOperation(storeConfig("filter", store), predicate());
        final var context = mockContext();

        assertThat(operation.apply(kTable(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }
}
