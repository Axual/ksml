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
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionContext {
    public static final ExecutionContext INSTANCE = new ExecutionContext();
    private static final Logger log = LoggerFactory.getLogger(ExecutionContext.class);
    private boolean allowDataInLogs;
    private ErrorHandler consumeHandler;
    private ErrorHandler processHandler;
    private ErrorHandler produceHandler;

    private ExecutionContext() {
    }

    public static String maskData(Object data) {
        if (!INSTANCE.allowDataInLogs) return "*****";
        return data.toString();
    }

    public DeserializationExceptionHandler.DeserializationHandlerResponse consumeError(ConsumerRecord<byte[], byte[]>, Throwable throwable) {
        if (consumeHandler.handlerType()==)
        return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
    }

    public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtException(Throwable throwable) {
        log.error("Caught serious exception in thread!", throwable);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }

    public ProductionExceptionHandler.ProductionExceptionHandlerResponse produceError(ProducerRecord<byte[], byte[]> record, Throwable throwable) {
        return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
    }
}
