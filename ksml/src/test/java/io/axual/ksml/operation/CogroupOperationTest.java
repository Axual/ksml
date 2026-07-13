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
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.aggregator;
import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CogroupOperationTest {

    private CogroupOperation operation() {
        return new CogroupOperation(operationConfig("cogroup"), aggregator());
    }

    @Test
    @DisplayName("cogroup on a grouped stream produces a cogrouped stream")
    @SuppressWarnings("unchecked")
    void applyToGroupedStreamReturnsCogrouped() {
        final KGroupedStream<Object, Object> grouped = mock(KGroupedStream.class);
        when(grouped.cogroup(any(Aggregator.class))).thenReturn(mock(CogroupedKStream.class));
        final var input = new KGroupedStreamWrapper(grouped, key(), value());

        assertThat(operation().apply(input, mockContext())).isInstanceOf(CogroupedKStreamWrapper.class);
        verify(grouped).cogroup(any(Aggregator.class));
    }

    @Test
    @DisplayName("cogroup on an already-cogrouped stream is rejected as unsupported")
    void applyToCogroupedStreamIsUnsupported() {
        final var operation = operation();
        final var input = cogroupedStream();
        final var context = mockContext();
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("not supported");
    }
}
