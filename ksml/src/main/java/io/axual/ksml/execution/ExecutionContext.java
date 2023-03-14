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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

public class ExecutionContext {
    public static final ExecutionContext INSTANCE = new ExecutionContext();
    private ErrorHandler consumeHandler;
    private ErrorHandler processHandler;
    private ErrorHandler produceHandler;

    private Logger consumeExceptionLogger;
    private Logger produceExceptionLogger;
    private Logger processExceptionLogger;

    private ExecutionContext() {
        // do nothing
    }

    public void setConsumeHandler(ErrorHandler consumeHandler) {
        this.consumeHandler = consumeHandler;
        this.consumeExceptionLogger = LoggerFactory.getLogger(consumeHandler.loggerName());
    }

    public void setProcessHandler(ErrorHandler processHandler) {
        this.processHandler = processHandler;
        this.processExceptionLogger = LoggerFactory.getLogger(processHandler.loggerName());
    }

    public void setProduceHandler(ErrorHandler produceHandler) {
        this.produceHandler = produceHandler;
        this.produceExceptionLogger = LoggerFactory.getLogger(produceHandler.loggerName());
    }

    public static final String DATA_MASK = "*****";
    public static final String DATA_NULL = "<NULL>";

    public String maskData(Object data) {
        if (!processHandler.logPayload()) return DATA_MASK;
        return data.toString();
    }

    public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtException(Throwable throwable) {
        processExceptionLogger.error("Caught serious exception, restarting the KSML client", throwable);
        // Restart only the current instance of Streams
        return
                StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    public DeserializationExceptionHandler.DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        if (ExecutionContext.INSTANCE.consumeHandler.log()) {
            // log record
            String key = consumeHandler.logPayload() ? bytesToString(record.key()) : DATA_MASK;
            String value = consumeHandler.logPayload() ? bytesToString(record.value()) : DATA_MASK;
            consumeExceptionLogger.error("Exception occurred while consuming a record from topic {}, partition {}, offset {}, key : {}, value : {}", context.topic(), context.partition(), context.offset(), key, value, exception);
        }
        return consumeHandler.handlerType() == ErrorHandler.HandlerType.CONTINUE_ON_FAIL ? DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE : DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL;
    }

    public String bytesToString(byte[] data) {
        return data == null ? DATA_NULL : Base64.getEncoder().encodeToString(data);
    }

    public ProductionExceptionHandler.ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (produceHandler.log()) {
            // log record
            String key = produceHandler.logPayload() ? bytesToString(record.key()) : DATA_MASK;
            String value = produceHandler.logPayload() ? bytesToString(record.value()) : DATA_MASK;
            produceExceptionLogger.error("Exception occurred while producing a record with key : {}, value : {}", key, value);
        }
        return produceHandler.handlerType() == ErrorHandler.HandlerType.CONTINUE_ON_FAIL ? ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE : ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL;
    }
}
