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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.AggregatorDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.operation.OperationTestSupport.aggregator;
import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.genericFunction;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CogroupOperationTest extends OperationTestBase {

    private static final String SAVED_STREAM = "grouped_refunds";

    private CogroupOperation operation() {
        return operation(null);
    }

    private CogroupOperation operation(String stream) {
        return new CogroupOperation(operationConfig("cogroup"), stream, aggregator());
    }

    private CogroupOperation operation(String stream, UserType aggregatorResultType) {
        return new CogroupOperation(operationConfig("cogroup"), stream, new AggregatorDefinition(genericFunction(aggregatorResultType)));
    }

    private static StreamDataType typed(DataType type, boolean isKey) {
        return new StreamDataType(new UserType(type), isKey);
    }

    private static CogroupedKStreamWrapper cogroupedStreamReturning(CogroupedKStream<Object, Object> result, StreamDataType k, StreamDataType v) {
        return cogroupedStreamReturning(result, List.of(), k, v);
    }

    @SuppressWarnings("unchecked")
    private static CogroupedKStreamWrapper cogroupedStreamReturning(CogroupedKStream<Object, Object> result, List<KGroupedStream<Object, Object>> groupedStreams, StreamDataType k, StreamDataType v) {
        final CogroupedKStream<Object, Object> cogrouped = mock(CogroupedKStream.class);
        when(cogrouped.cogroup(any(KGroupedStream.class), any(Aggregator.class))).thenReturn(result);
        return new CogroupedKStreamWrapper(cogrouped, groupedStreams, k, v);
    }

    @Test
    @DisplayName("cogroup on a grouped stream produces a cogrouped stream")
    @SuppressWarnings("unchecked")
    void applyToGroupedStreamReturnsCogrouped() {
        final KGroupedStream<Object, Object> grouped = mock(KGroupedStream.class);
        when(grouped.cogroup(any(Aggregator.class))).thenReturn(mock(CogroupedKStream.class));
        final var input = new KGroupedStreamWrapper(grouped, key(), value());

        final var output = operation().apply(input, mockContext());

        assertThat(output).isInstanceOf(CogroupedKStreamWrapper.class);
        assertThat(((CogroupedKStreamWrapper) output).groupedStreams).containsExactly(grouped);
        verify(grouped).cogroup(any(Aggregator.class));
    }

    @Test
    @DisplayName("cogroup with a stream on a grouped stream (the first cogroup) is rejected")
    void applyToGroupedStreamWithStreamIsRejected() {
        final var operation = operation(SAVED_STREAM);
        final var input = groupedStream();
        final var context = mockContext();
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("follows another cogroup");
    }

    @Test
    @DisplayName("cogroup with a stream on a cogrouped stream adds the saved grouped stream to the cogroup")
    @SuppressWarnings("unchecked")
    void applyToCogroupedStreamAddsSavedGroupedStream() {
        final CogroupedKStream<Object, Object> result = mock(CogroupedKStream.class);
        final var first = groupedStream();
        final var input = cogroupedStreamReturning(result, List.of(first.groupedStream), key(), value());
        final var saved = groupedStream();
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(saved);

        final var output = operation(SAVED_STREAM).apply(input, context);

        assertThat(output).isInstanceOf(CogroupedKStreamWrapper.class);
        assertThat(((CogroupedKStreamWrapper) output).cogroupedStream).isSameAs(result);
        assertThat(((CogroupedKStreamWrapper) output).groupedStreams).containsExactly(first.groupedStream, saved.groupedStream);
        verify(input.cogroupedStream).cogroup(eq(saved.groupedStream), any(Aggregator.class));
    }

    @Test
    @DisplayName("cogroup with a stream that is already part of the cogroup is rejected")
    @SuppressWarnings("unchecked")
    void applyToCogroupedStreamWithDuplicateStreamIsRejected() {
        final var operation = operation(SAVED_STREAM);
        final var saved = groupedStream();
        final var input = new CogroupedKStreamWrapper(mock(CogroupedKStream.class), List.of(saved.groupedStream), key(), value());
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(saved);
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("already part of this cogroup");
        verify(input.cogroupedStream, never()).cogroup(any(KGroupedStream.class), any(Aggregator.class));
    }

    @Test
    @DisplayName("adding the same saved stream twice is rejected on the second cogroup")
    @SuppressWarnings("unchecked")
    void addingSameSavedStreamTwiceIsRejected() {
        final var saved = groupedStream();
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(saved);
        final var input = cogroupedStreamReturning(mock(CogroupedKStream.class), key(), value());

        final var afterFirst = (CogroupedKStreamWrapper) operation(SAVED_STREAM).apply(input, context);
        final var second = operation(SAVED_STREAM);

        assertThatThrownBy(() -> second.apply(afterFirst, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("already part of this cogroup");
    }

    @Test
    @DisplayName("cogroup on a cogrouped stream that another pipeline already continued from is rejected")
    @SuppressWarnings("unchecked")
    void applyToUsedCogroupedStreamIsRejected() {
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(groupedStream());
        final var input = cogroupedStreamReturning(mock(CogroupedKStream.class), key(), value());
        operation(SAVED_STREAM).apply(input, context);
        final var second = operation(SAVED_STREAM);

        assertThatThrownBy(() -> second.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("already used by another pipeline");
    }

    @Test
    @DisplayName("the added grouped stream may have a different value type than the cogrouped stream")
    @SuppressWarnings("unchecked")
    void applyToCogroupedStreamAcceptsOtherValueType() {
        final var k = typed(DataString.DATATYPE, true);
        final var vOut = typed(DataString.DATATYPE, false);
        final var input = cogroupedStreamReturning(mock(CogroupedKStream.class), k, vOut);
        final var saved = new KGroupedStreamWrapper(mock(KGroupedStream.class), k, typed(DataInteger.DATATYPE, false));
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(saved);

        final var output = operation(SAVED_STREAM, vOut.userType()).apply(input, context);

        assertThat(output.keyType()).isEqualTo(k);
        assertThat(output.valueType()).isEqualTo(vOut);
    }

    @Test
    @DisplayName("cogroup on a cogrouped stream without a stream is rejected with a hint")
    void applyToCogroupedStreamWithoutStreamIsRejected() {
        final var operation = operation();
        final var input = cogroupedStream();
        final var context = mockContext();
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("requires \"stream\"");
    }

    @Test
    @DisplayName("cogroup with an unknown stream name is rejected")
    void applyToCogroupedStreamWithUnknownStreamIsRejected() {
        final var operation = operation(SAVED_STREAM);
        final var input = cogroupedStream();
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenThrow(new TopologyException("Stream '" + SAVED_STREAM + "' not found"));
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining(SAVED_STREAM)
                .hasMessageContaining("not found")
                .hasMessageContaining("defined before this one");
    }

    @Test
    @DisplayName("cogroup with a stream that is not grouped is rejected")
    void applyToCogroupedStreamWithUngroupedStreamIsRejected() {
        final var operation = operation(SAVED_STREAM);
        final var input = cogroupedStream();
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(kStream());
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("must be a grouped stream");
    }

    @Test
    @DisplayName("cogroup with a stream of another key type is rejected")
    @SuppressWarnings("unchecked")
    void applyToCogroupedStreamWithOtherKeyTypeIsRejected() {
        final var operation = operation(SAVED_STREAM);
        final var input = new CogroupedKStreamWrapper(mock(CogroupedKStream.class), List.of(), typed(DataString.DATATYPE, true), value());
        final var saved = new KGroupedStreamWrapper(mock(KGroupedStream.class), typed(DataInteger.DATATYPE, true), value());
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(saved);
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("keyType");
    }

    @Test
    @DisplayName("cogroup with an aggregator whose result type differs from the cogrouped value type is rejected")
    @SuppressWarnings("unchecked")
    void applyToCogroupedStreamWithOtherAggregatorResultTypeIsRejected() {
        final var operation = operation(SAVED_STREAM, new UserType(DataInteger.DATATYPE));
        final var input = new CogroupedKStreamWrapper(mock(CogroupedKStream.class), List.of(), key(), typed(DataString.DATATYPE, false));
        final var context = mockContext();
        when(context.getStreamWrapper(eq(SAVED_STREAM), isNull())).thenReturn(groupedStream());
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Aggregator resultType");
    }
}
