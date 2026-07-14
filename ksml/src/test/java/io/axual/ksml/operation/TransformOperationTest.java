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
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyTransformer;
import static io.axual.ksml.operation.OperationTestSupport.keyValueToKeyValueListTransformer;
import static io.axual.ksml.operation.OperationTestSupport.keyValueToValueListTransformer;
import static io.axual.ksml.operation.OperationTestSupport.keyValueTransformer;
import static io.axual.ksml.operation.OperationTestSupport.metadataTransformer;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TransformOperationTest extends OperationTestBase {

    @ParameterizedTest(name = "{0}")
    @DisplayName("a transform operation on a stream returns a stream and wires the expected processor")
    @MethodSource("transformCases")
    @SuppressWarnings("unchecked")
    void transformOnStreamWiresExpectedProcessor(String name, StreamOperation operation, Consumer<KStream<Object, Object>> verification) {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verification.accept(stream);
    }

    static Stream<Arguments> transformCases() {
        return Stream.of(
                Arguments.of("transformKey processes records via a processor supplier",
                        new TransformKeyOperation(operationConfig("mapKey"), keyTransformer()),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).process(any(ProcessorSupplier.class), any(Named.class))),
                Arguments.of("transformKeyValue processes records via a processor supplier",
                        new TransformKeyValueOperation(operationConfig("map"), keyValueTransformer()),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).process(any(ProcessorSupplier.class), any(Named.class))),
                Arguments.of("transformMetadata processes values via a fixed-key processor supplier",
                        new TransformMetadataOperation(operationConfig("transformMetadata"), metadataTransformer()),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).processValues(any(FixedKeyProcessorSupplier.class), any(Named.class), any(String[].class))),
                Arguments.of("transformKeyValueToKeyValueList processes records with named child nodes",
                        new TransformKeyValueToKeyValueListOperation(operationConfig("flatMap"), keyValueToKeyValueListTransformer()),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).process(any(ProcessorSupplier.class), any(Named.class), any(String[].class))),
                Arguments.of("transformKeyValueToValueList processes values via a fixed-key processor supplier",
                        new TransformKeyValueToValueListOperation(operationConfig("flatMapValues"), keyValueToValueListTransformer()),
                        (Consumer<KStream<Object, Object>>) s -> verify(s).processValues(any(FixedKeyProcessorSupplier.class), any(Named.class), any(String[].class)))
        );
    }
}
