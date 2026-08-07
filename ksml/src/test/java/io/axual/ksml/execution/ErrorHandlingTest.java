package io.axual.ksml.execution;

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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static io.axual.ksml.execution.ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
import static io.axual.ksml.execution.ErrorHandler.HandlerType.RETRY_ON_FAIL;
import static io.axual.ksml.execution.ErrorHandler.HandlerType.STOP_ON_FAIL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ErrorHandlingTest {

    @Mock
    private ErrorHandlerContext context;

    private ErrorHandling errorHandling;

    @BeforeEach
    void setUp() {
        errorHandling = new ErrorHandling();
    }

    private static ErrorHandler handler(boolean log, boolean logPayload, ErrorHandler.HandlerType type) {
        return new ErrorHandler(log, "test.logger", logPayload, type);
    }

    private static ConsumerRecord<byte[], byte[]> consumerRecord() {
        return new ConsumerRecord<>("topic", 0, 0L, "key".getBytes(), "value".getBytes());
    }

    private static ProducerRecord<byte[], byte[]> producerRecord() {
        return new ProducerRecord<>("topic", "key".getBytes(), "value".getBytes());
    }

    // --- payload conversion ----------------------------------------------------------------------

    @Test
    @DisplayName("bytesToString renders null as a placeholder and bytes as base64")
    void bytesToStringHandlesNullAndValue() {
        assertThat(errorHandling.bytesToString(null)).isEqualTo("<NULL>");
        assertThat(errorHandling.bytesToString("data".getBytes())).startsWith("(base64)");
    }

    @Test
    @DisplayName("objectToString renders null as a placeholder and prefixes the value type")
    void objectToStringHandlesNullAndValue() {
        assertThat(errorHandling.objectToString(null)).isEqualTo("<NULL>");
        assertThat(errorHandling.objectToString("data")).isEqualTo("(string)data");
    }

    // --- deserialization -------------------------------------------------------------------------

    @Test
    @DisplayName("a continue-on-fail consume handler with payload logging returns CONTINUE")
    void deserializationContinueOnFailWithPayloadLogging() {
        errorHandling.setConsumeHandler(handler(true, true, CONTINUE_ON_FAIL));
        assertThat(errorHandling.handle(context, consumerRecord(), new RuntimeException("boom")).result())
                .isEqualTo(DeserializationExceptionHandler.Result.RESUME);
    }

    @Test
    @DisplayName("a stop-on-fail consume handler without payload logging returns FAIL")
    void deserializationStopOnFailWithoutPayloadLogging() {
        errorHandling.setConsumeHandler(handler(true, false, STOP_ON_FAIL));
        assertThat(errorHandling.handle(context, consumerRecord(), new RuntimeException("boom")).result())
                .isEqualTo(DeserializationExceptionHandler.Result.FAIL);
    }

    @Test
    @DisplayName("retry-on-fail is unsupported for deserialization handling")
    void deserializationRetryIsUnsupported() {
        errorHandling.setConsumeHandler(handler(false, false, RETRY_ON_FAIL));
        final var rec = consumerRecord();
        final var exception = new RuntimeException("boom");
        assertThatThrownBy(() -> errorHandling.handle(context, rec, exception))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- processing ------------------------------------------------------------------------------

    @Test
    @DisplayName("a continue-on-fail process handler returns CONTINUE")
    void processingContinueOnFail() {
        errorHandling.setProcessHandler(handler(true, true, CONTINUE_ON_FAIL));
        assertThat(errorHandling.handle(context, new Record<>("key", "value", 0L), new RuntimeException("boom")).result())
                .isEqualTo(ProcessingExceptionHandler.Result.RESUME);
    }

    @Test
    @DisplayName("a stop-on-fail process handler returns FAIL")
    void processingStopOnFail() {
        errorHandling.setProcessHandler(handler(false, false, STOP_ON_FAIL));
        assertThat(errorHandling.handle(context, new Record<>("key", "value", 0L), new RuntimeException("boom")).result())
                .isEqualTo(ProcessingExceptionHandler.Result.FAIL);
    }

    @Test
    @DisplayName("retry-on-fail is unsupported for processing handling")
    void processingRetryIsUnsupported() {
        errorHandling.setProcessHandler(handler(false, false, RETRY_ON_FAIL));
        final var rec = new Record<>("key", "value", 0L);
        final var exception = new RuntimeException("boom");
        assertThatThrownBy(() -> errorHandling.handle(context, rec, exception))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- production ------------------------------------------------------------------------------

    @Test
    @DisplayName("a continue-on-fail produce handler returns CONTINUE")
    void productionContinueOnFail() {
        errorHandling.setProduceHandler(handler(true, true, CONTINUE_ON_FAIL));
        assertThat(errorHandling.handle(context, producerRecord(), new RuntimeException("boom")).result())
                .isEqualTo(ProductionExceptionHandler.Result.RESUME);
    }

    @Test
    @DisplayName("a stop-on-fail produce handler returns FAIL")
    void productionStopOnFail() {
        errorHandling.setProduceHandler(handler(false, false, STOP_ON_FAIL));
        assertThat(errorHandling.handle(context, producerRecord(), new RuntimeException("boom")).result())
                .isEqualTo(ProductionExceptionHandler.Result.FAIL);
    }

    @Test
    @DisplayName("a retry-on-fail produce handler returns RETRY")
    void productionRetryOnFail() {
        errorHandling.setProduceHandler(handler(true, true, RETRY_ON_FAIL));
        assertThat(errorHandling.handle(context, producerRecord(), new RuntimeException("boom")).result())
                .isEqualTo(ProductionExceptionHandler.Result.RETRY);
    }

    // --- uncaught exceptions ---------------------------------------------------------------------

    @Test
    @DisplayName("an uncaught topic authorization exception shuts down the client")
    void uncaughtTopicAuthorizationExceptionShutsDownClient() {
        final var cause = new TopicAuthorizationException(Set.of("secure-topic"));
        assertThat(errorHandling.uncaughtException(new StreamsException(cause)))
                .isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    }

    @Test
    @DisplayName("an uncaught non-streams exception shuts down the client")
    void uncaughtNonStreamsExceptionShutsDownClient() {
        errorHandling.setProcessHandler(handler(true, true, STOP_ON_FAIL));
        assertThat(errorHandling.uncaughtException(new RuntimeException("boom")))
                .isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    }
}
