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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.saveStream;
import static io.axual.ksml.operation.OperationTestSupport.savedTarget;
import static io.axual.ksml.operation.OperationTestSupport.streamTarget;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MergeOperationTest extends OperationTestBase {

    @Test
    @DisplayName("merge on a stream delegates to KStream.merge with the other stream and returns a stream")
    @SuppressWarnings("unchecked")
    void applyToStreamMergesWithOtherStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new MergeOperation(operationConfig("merge"), streamTarget());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).merge(any(KStream.class), any(Named.class));
    }

    @Test
    @DisplayName("merge with a stream saved with \"as\" looks the stream up by name")
    @SuppressWarnings("unchecked")
    void applyToStreamWithSavedStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var context = mockContext();
        saveStream(context, "saved_stream", kStream());
        final var operation = new MergeOperation(operationConfig("merge"), savedTarget("saved_stream"));

        assertThat(operation.apply(input, context)).isInstanceOf(KStreamWrapper.class);
        verify(stream).merge(any(KStream.class), any(Named.class));
    }

    @Test
    @DisplayName("merge with a saved table is rejected")
    void savedTableIsNotAStream() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var context = mockContext();
        saveStream(context, "saved_table", kTable());
        final var operation = new MergeOperation(operationConfig("merge"), savedTarget("saved_table"));

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Merge stream \"saved_table\" must be a stream, but found");
    }

    @Test
    @DisplayName("merge with an unknown saved stream explains how to define or save it")
    void unknownSavedStreamIsNotFound() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var operation = new MergeOperation(operationConfig("merge"), savedTarget("missing"));

        assertThatThrownBy(() -> operation.apply(input, mockContext()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Merge stream \"missing\" not found")
                .hasMessageContaining("Define it under \"streams\", or save a stream with \"as: missing\" in a pipeline defined before this one");
    }
}
