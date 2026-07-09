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

import io.axual.ksml.stream.StreamWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.verify;

class AsOperationTest {

    private AsOperation operation() {
        return new AsOperation(operationConfig("as"), "myTarget");
    }

    @Test
    void applyToStreamRegistersUnderTargetNameAndReturnsNull() {
        final var context = mockContext();
        final var input = kStream();

        assertThat(operation().apply(input, context)).isNull();
        verify(context).registerStreamWrapper("myTarget", input);
    }

    static Stream<Arguments> nonStreamWrappers() {
        return Stream.of(
                arguments("table", (Supplier<StreamWrapper>) OperationTestSupport::kTable),
                arguments("globalTable", (Supplier<StreamWrapper>) OperationTestSupport::globalKTable),
                arguments("groupedStream", (Supplier<StreamWrapper>) OperationTestSupport::groupedStream),
                arguments("groupedTable", (Supplier<StreamWrapper>) OperationTestSupport::groupedTable),
                arguments("sessionWindowed", (Supplier<StreamWrapper>) OperationTestSupport::sessionWindowed),
                arguments("timeWindowed", (Supplier<StreamWrapper>) OperationTestSupport::timeWindowed),
                arguments("cogroupedStream", (Supplier<StreamWrapper>) OperationTestSupport::cogroupedStream),
                arguments("sessionWindowedCogrouped", (Supplier<StreamWrapper>) OperationTestSupport::sessionWindowedCogrouped),
                arguments("timeWindowedCogrouped", (Supplier<StreamWrapper>) OperationTestSupport::timeWindowedCogrouped));
    }

    @ParameterizedTest(name = "applyTo {0} returns null")
    @MethodSource("nonStreamWrappers")
    void applyToNonStreamReturnsNull(String name, Supplier<StreamWrapper> wrapper) {
        assertThat(wrapper.get().apply(operation(), mockContext())).isNull();
    }
}
