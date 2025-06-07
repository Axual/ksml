package io.axual.ksml.execution;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.streams.errors.*;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.stream.Collectors;

@Slf4j
public class ErrorHandling {
    private static final String DATA_MASK = "*****";
    private static final String DATA_NULL = "<NULL>";
    private static final String STRING_PREFIX = "(string)";

    private ErrorHandler consumeHandler;
    private ErrorHandler processHandler;
    private ErrorHandler produceHandler;

    private Logger consumeExceptionLogger;
    private Logger produceExceptionLogger;
    private Logger processExceptionLogger;

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

    public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtException(Throwable throwable) {
        if (throwable instanceof StreamsException streamsException) {
            if (streamsException.getCause() instanceof TopicAuthorizationException topicAuthorizationException && !topicAuthorizationException.unauthorizedTopics().isEmpty()) {
                log.error("Topic authorization exception was thrown. Please create / grant access to the following topics: \n"
                        + topicAuthorizationException.unauthorizedTopics().stream().map(t -> "  * " + t + "\n").collect(Collectors.joining()));
            }
        } else {
            processExceptionLogger.error("Caught unhandled exception, stopping this KSML instance", throwable);
        }
        // Stop only the current instance of KSML
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    public String bytesToString(byte[] data) {
        return data == null ? DATA_NULL : "(base64)" + Base64.getEncoder().encodeToString(data);
    }

    public String objectToString(Object data) {
        return data == null ? DATA_NULL : STRING_PREFIX + data;
    }

    private void logError(Logger logger, String errorType, ErrorHandlerContext context, String key, String value, Exception exception) {
        logger.error("{} error:\n  topic={}\n  partition={}\n  offset={}\n  processorNodeId={}\n  taskId={}\n  timestamp={}\n  key={}\n  value={}\n",
                errorType,
                context.topic() != null ? context.topic() : "<null>",
                context.partition(),
                context.offset(),
                context.processorNodeId() != null ? context.processorNodeId() : "<null>",
                context.taskId(),
                context.timestamp(),
                key,
                value,
                exception);
    }

    public DeserializationExceptionHandler.DeserializationHandlerResponse handle(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> rec, Exception exception) {
        if (consumeHandler.log()) {
            // log record
            String key = consumeHandler.logPayload() ? bytesToString(rec.key()) : DATA_MASK;
            String value = consumeHandler.logPayload() ? bytesToString(rec.value()) : DATA_MASK;
            logError(consumeExceptionLogger, "Deserialization", context, key, value, exception);
        }
        return switch (consumeHandler.handlerType()) {
            case CONTINUE_ON_FAIL -> DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
            case STOP_ON_FAIL -> DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL;
            default ->
                    throw new UnsupportedOperationException("Unsupported deserialization error handler type. Only CONTINUE_ON_FAIL or STOP_ON_FAIL are allowed.");
        };
    }

    public ProcessingExceptionHandler.ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> rec, Exception exception) {
        if (processHandler.log()) {
            // log record
            String key = processHandler.logPayload() ? objectToString(rec.key()) : DATA_MASK;
            String value = processHandler.logPayload() ? objectToString(rec.value()) : DATA_MASK;
            logError(processExceptionLogger, "Processing", context, key, value, exception);
        }
        return switch (processHandler.handlerType()) {
            case CONTINUE_ON_FAIL -> ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE;
            case STOP_ON_FAIL -> ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
            default ->
                    throw new UnsupportedOperationException("Unsupported processing error handler type. Only CONTINUE_ON_FAIL or STOP_ON_FAIL are allowed.");
        };
    }

    public ProductionExceptionHandler.ProductionExceptionHandlerResponse handle(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> rec, Exception exception) {
        if (produceHandler.log()) {
            // log record
            String key = produceHandler.logPayload() ? bytesToString(rec.key()) : DATA_MASK;
            String value = produceHandler.logPayload() ? bytesToString(rec.value()) : DATA_MASK;
            logError(produceExceptionLogger, "Produce", context, key, value, exception);
        }
        return switch (produceHandler.handlerType()) {
            case CONTINUE_ON_FAIL -> ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE;
            case STOP_ON_FAIL -> ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL;
            case RETRY_ON_FAIL -> ProductionExceptionHandler.ProductionExceptionHandlerResponse.RETRY;
        };
    }
}
