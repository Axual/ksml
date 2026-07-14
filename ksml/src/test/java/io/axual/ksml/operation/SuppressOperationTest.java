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

import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.stream.KTableWrapper;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.windowedKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SuppressOperationTest {

    private static Suppressed<Object> untilTimeLimit() {
        return Suppressed.untilTimeLimit(Duration.ofMillis(10), Suppressed.BufferConfig.unbounded());
    }

    private static Suppressed<Windowed<?>> untilWindowCloses() {
        return Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull());
    }

    @Test
    @DisplayName("suppress until time limit delegates to KTable.suppress and returns a KTable")
    @SuppressWarnings("unchecked")
    void applyUntilTimeLimitToTableCallsSuppress() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var operation = SuppressOperation.create(operationConfig("suppress"), untilTimeLimit());

        assertThat(operation.apply(new KTableWrapper(table, key(), value()), mockContext())).isInstanceOf(KTableWrapper.class);
        verify(table).suppress(any(Suppressed.class));
    }

    @Test
    @DisplayName("suppress until window closes on a windowed-key table delegates to KTable.suppress and returns a KTable")
    @SuppressWarnings("unchecked")
    void applyUntilWindowClosesToWindowedTableCallsSuppress() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var operation = SuppressOperation.createWindowed(operationConfig("suppress"), untilWindowCloses());

        assertThat(operation.apply(new KTableWrapper(table, windowedKey(), value()), mockContext())).isInstanceOf(KTableWrapper.class);
        verify(table).suppress(any(Suppressed.class));
    }

    @Test
    @DisplayName("suppress until window closes on a non-windowed table throws a TopologyException")
    void applyUntilWindowClosesToNonWindowedTableFails() {
        final var operation = SuppressOperation.createWindowed(operationConfig("suppress"), untilWindowCloses());
        final var input = kTable();
        final var context = mockContext();
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Can not apply suppress");
    }
}
