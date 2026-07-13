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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueMapper;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ToStreamOperationTest {

    @Test
    @DisplayName("toStream with a key-value mapper converts a table to a KStream using the mapper")
    @SuppressWarnings("unchecked")
    void applyWithMapperReturnsStream() {
        final KTable<Object, Object> table = mock(KTable.class);
        final KStream<Object, Object> stream = mock(KStream.class);
        when(table.toStream(any(KeyValueMapper.class), any(Named.class))).thenReturn(stream);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new ToStreamOperation(operationConfig("toStream"), keyValueMapper());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(table).toStream(any(KeyValueMapper.class), any(Named.class));
    }

    @Test
    @DisplayName("toStream without a mapper converts a table to a KStream keeping the original keys")
    @SuppressWarnings("unchecked")
    void applyWithoutMapperReturnsStream() {
        final KTable<Object, Object> table = mock(KTable.class);
        final KStream<Object, Object> stream = mock(KStream.class);
        when(table.toStream(any(Named.class))).thenReturn(stream);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new ToStreamOperation(operationConfig("toStream"), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(table).toStream(any(Named.class));
    }
}
