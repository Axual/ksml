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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.axual.ksml.operation.OperationTestSupport.UNKNOWN_TYPE;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ConvertOperationTest extends OperationTestBase {

    @ParameterizedTest(name = "{0}")
    @DisplayName("a convert operation on a stream returns a stream and delegates to the expected KStream method")
    @MethodSource("convertCases")
    @SuppressWarnings("unchecked")
    void convertOnStreamDelegatesToExpectedMethod(String name, StreamOperation operation, Consumer<KStream<Object, Object>> verification) {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verification.accept(stream);
    }

    static Stream<Arguments> convertCases() {
        return Stream.of(
                Arguments.of("convertKey reselects the key",
                        new ConvertKeyOperation(operationConfig("convertKey"), UNKNOWN_TYPE),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).selectKey(any(), any(Named.class))),
                Arguments.of("convertValue maps the values",
                        new ConvertValueOperation(operationConfig("convertValue"), UNKNOWN_TYPE),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).mapValues(any(ValueMapper.class), any(Named.class))),
                Arguments.of("convertKeyValue maps key and value",
                        new ConvertKeyValueOperation(operationConfig("convertKeyValue"), UNKNOWN_TYPE, UNKNOWN_TYPE),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).map(any(), any(Named.class)))
        );
    }
}
