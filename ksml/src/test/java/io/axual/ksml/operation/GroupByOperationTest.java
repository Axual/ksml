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
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueMapper;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.tupleKeyValueMapper;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GroupByOperationTest {

    @Test
    @DisplayName("groupBy on a stream produces a grouped stream")
    @SuppressWarnings("unchecked")
    void applyToStreamReturnsGroupedStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        when(stream.groupBy(any(KeyValueMapper.class), any(Grouped.class))).thenReturn(mock(KGroupedStream.class));
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new GroupByOperation(storeConfig("groupBy"), keyValueMapper());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KGroupedStreamWrapper.class);
        verify(stream).groupBy(any(KeyValueMapper.class), any(Grouped.class));
    }

    @Test
    @DisplayName("groupBy on a table with a tuple selector produces a grouped table")
    @SuppressWarnings("unchecked")
    void applyToTableReturnsGroupedTable() {
        // KTable groupBy requires the selector to produce a (key, value) tuple.
        final KTable<Object, Object> table = mock(KTable.class);
        when(table.groupBy(any(KeyValueMapper.class), any(Grouped.class))).thenReturn(mock(KGroupedTable.class));
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new GroupByOperation(storeConfig("groupBy"), tupleKeyValueMapper());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KGroupedTableWrapper.class);
        verify(table).groupBy(any(KeyValueMapper.class), any(Grouped.class));
    }
}
