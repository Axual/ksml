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
import io.axual.ksml.stream.KStreamWrapper;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.axual.ksml.operation.OperationTestSupport.dualStoreConfig;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.saveStream;
import static io.axual.ksml.operation.OperationTestSupport.savedTarget;
import static io.axual.ksml.operation.OperationTestSupport.streamTarget;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.valueJoiner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OuterJoinWithStreamOperationTest extends OperationTestBase {

    @Test
    @DisplayName("outer join with a stream produces a KStream result using a JoinWindows-based outerJoin")
    @SuppressWarnings("unchecked")
    void applyToStreamReturnsStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new OuterJoinWithStreamOperation(
                dualStoreConfig("outerJoin"), streamTarget(), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).outerJoin(any(KStream.class), any(ValueJoiner.class), any(JoinWindows.class), any(StreamJoined.class));
    }

    @Test
    @DisplayName("outer join with a stream saved with \"as\" looks the stream up by name")
    @SuppressWarnings("unchecked")
    void applyToStreamWithSavedStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var context = mockContext();
        saveStream(context, "saved_stream", kStream());
        final var operation = new OuterJoinWithStreamOperation(
                dualStoreConfig("outerJoin"), savedTarget("saved_stream"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, context)).isInstanceOf(KStreamWrapper.class);
        verify(stream).outerJoin(any(KStream.class), any(ValueJoiner.class), any(JoinWindows.class), any(StreamJoined.class));
    }

    @Test
    @DisplayName("outerJoin with an unknown saved stream explains how to define or save it")
    void unknownSavedStreamIsNotFound() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var operation = new OuterJoinWithStreamOperation(
                dualStoreConfig("outerJoin"), savedTarget("missing"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThatThrownBy(() -> operation.apply(input, mockContext()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("OuterJoin stream \"missing\" not found")
                .hasMessageContaining("Define it under \"streams\", or save a stream with \"as: missing\" in a pipeline defined before this one");
    }

    @Test
    @DisplayName("outerJoin with a saved table as stream is rejected")
    void savedTableIsNotAStream() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var context = mockContext();
        saveStream(context, "saved_table", kTable());
        final var operation = new OuterJoinWithStreamOperation(
                dualStoreConfig("outerJoin"), savedTarget("saved_table"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("OuterJoin stream \"saved_table\" must be a stream, but found");
    }
}
