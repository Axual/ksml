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
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.axual.ksml.operation.OperationTestSupport.dualStoreConfig;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.joinWindowStore;
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

class JoinWithStreamOperationTest extends OperationTestBase {

    @Test
    @DisplayName("joining two streams over a window delegates to KStream.join and returns a stream")
    @SuppressWarnings("unchecked")
    void applyToStreamReturnsStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join"), streamTarget(), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).join(any(KStream.class), any(ValueJoinerWithKey.class), any(JoinWindows.class), any(StreamJoined.class));
    }

    @Test
    @DisplayName("joining two streams with explicit join stores delegates to KStream.join and returns a stream")
    @SuppressWarnings("unchecked")
    void applyToStreamWithStoresReturnsStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join", joinWindowStore("thisStore"), joinWindowStore("otherStore")),
                streamTarget(), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).join(any(KStream.class), any(ValueJoinerWithKey.class), any(JoinWindows.class), any(StreamJoined.class));
    }

    @Test
    @DisplayName("toString() includes both join store names")
    void toStringIncludesStoreNames() {
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join", joinWindowStore("thisStore"), joinWindowStore("otherStore")),
                streamTarget(), valueJoiner(), Duration.ofSeconds(1), null);
        assertThat(operation).asString().contains("thisStore").contains("otherStore");
    }

    @Test
    @DisplayName("joining with a stream saved with \"as\" looks the stream up by name")
    @SuppressWarnings("unchecked")
    void applyToStreamWithSavedStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var context = mockContext();
        saveStream(context, "saved_stream", kStream());
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join"), savedTarget("saved_stream"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, context)).isInstanceOf(KStreamWrapper.class);
        verify(stream).join(any(KStream.class), any(ValueJoinerWithKey.class), any(JoinWindows.class), any(StreamJoined.class));
    }

    @Test
    @DisplayName("joining with an unknown saved stream explains how to define or save it")
    void unknownSavedStreamIsNotFound() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join"), savedTarget("missing"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThatThrownBy(() -> operation.apply(input, mockContext()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Join stream \"missing\" not found")
                .hasMessageContaining("Define it under \"streams\", or save a stream with \"as: missing\" in a pipeline defined before this one");
    }

    @Test
    @DisplayName("joining with a saved table as stream is rejected")
    void savedTableIsNotAStream() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var context = mockContext();
        saveStream(context, "saved_table", kTable());
        final var operation = new JoinWithStreamOperation(
                dualStoreConfig("join"), savedTarget("saved_table"), valueJoiner(), Duration.ofSeconds(1), null);

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Join stream \"saved_table\" must be a stream, but found");
    }
}
