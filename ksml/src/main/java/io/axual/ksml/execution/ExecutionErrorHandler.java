package io.axual.ksml.execution;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

public class ExecutionErrorHandler implements DeserializationExceptionHandler, ProcessingExceptionHandler, ProductionExceptionHandler {
    @Override
    public DeserializationHandlerResponse handle(final ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> rec, Exception exception) {
        return ExecutionContext.INSTANCE.errorHandling().handle(context, rec, exception);
    }

    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> rec, Exception exception) {
        return ExecutionContext.INSTANCE.errorHandling().handle(context, rec, exception);
    }

    @Override
    public ProductionExceptionHandlerResponse handle(final ErrorHandlerContext context, ProducerRecord<byte[], byte[]> rec, Exception exception) {
        return ExecutionContext.INSTANCE.errorHandling().handle(context, rec, exception);
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Nothing to do
    }
}
