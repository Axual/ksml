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
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler.ProcessingHandlerResponse;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.axual.ksml.execution.ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
import static io.axual.ksml.execution.ErrorHandler.HandlerType.STOP_ON_FAIL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies that {@link ExecutionErrorHandler} delegates to the shared {@link ErrorHandling} held by
 * {@link ExecutionContext}. The handlers are configured to STOP_ON_FAIL so each delegated call
 * yields a FAIL response.
 */
@ExtendWith(MockitoExtension.class)
class ExecutionErrorHandlerTest {

    @Mock
    private ErrorHandlerContext context;

    private final ExecutionErrorHandler handler = new ExecutionErrorHandler();

    @BeforeEach
    void configureHandlers() {
        applyHandlers(STOP_ON_FAIL);
    }

    @AfterEach
    void restoreHandlers() {
        // ErrorHandling lives on the shared ExecutionContext singleton and exposes no getters to
        // capture the originals, so reset to a neutral default to avoid leaking STOP_ON_FAIL into
        // other tests that share the same instance.
        applyHandlers(CONTINUE_ON_FAIL);
    }

    private static void applyHandlers(final ErrorHandler.HandlerType type) {
        final var errorHandling = ExecutionContext.INSTANCE.errorHandling();
        errorHandling.setConsumeHandler(new ErrorHandler(false, "test.logger", false, type));
        errorHandling.setProcessHandler(new ErrorHandler(false, "test.logger", false, type));
        errorHandling.setProduceHandler(new ErrorHandler(false, "test.logger", false, type));
    }

    @Test
    @DisplayName("deserialization handling is delegated to the shared error handling")
    void delegatesDeserializationHandling() {
        final var rec = new ConsumerRecord<>("topic", 0, 0L, "k".getBytes(), "v".getBytes());
        assertThat(handler.handle(context, rec, new RuntimeException("boom")))
                .isEqualTo(DeserializationHandlerResponse.FAIL);
    }

    @Test
    @DisplayName("processing handling is delegated to the shared error handling")
    void delegatesProcessingHandling() {
        assertThat(handler.handle(context, new Record<>("k", "v", 0L), new RuntimeException("boom")))
                .isEqualTo(ProcessingHandlerResponse.FAIL);
    }

    @Test
    @DisplayName("production handling is delegated to the shared error handling")
    void delegatesProductionHandling() {
        final var rec = new ProducerRecord<>("topic", "k".getBytes(), "v".getBytes());
        assertThat(handler.handle(context, rec, new RuntimeException("boom")))
                .isEqualTo(ProductionExceptionHandlerResponse.FAIL);
    }

    @Test
    @DisplayName("configure is a no-op and does not throw")
    void configureIsANoOp() {
        assertThatCode(() -> handler.configure(java.util.Map.of())).doesNotThrowAnyException();
    }
}
