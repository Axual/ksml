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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.axual.ksml.operation.OperationTestSupport.dualStoreConfig;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.streamDefinition;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.valueJoiner;
import static org.assertj.core.api.Assertions.assertThat;
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
                dualStoreConfig("outerJoin"), streamDefinition(), valueJoiner(), Duration.ofSeconds(1), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).outerJoin(any(KStream.class), any(ValueJoiner.class), any(JoinWindows.class), any(StreamJoined.class));
    }
}
