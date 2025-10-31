package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = false)
@JsonClassDescription("Controls how consume, produce and processing errors are handled")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorHandlingConfig {
    @JsonProperty(value = "consume", required = false)
    @JsonPropertyDescription("Configures the error handling for deserialization errors. The default logger name is 'ConsumeError'.")
    private ErrorTypeHandlingConfig consume;
    @JsonProperty(value = "produce", required = false)
    @JsonPropertyDescription("Configures the error handling for serialization errors. The default logger name is 'ProduceError'.")
    private ErrorTypeHandlingConfig produce;
    @JsonProperty(value = "process", required = false)
    @JsonPropertyDescription("Configures the error handling for pipeline processing errors. The default logger name is 'ProcessError'.")
    private ErrorTypeHandlingConfig process;

    public synchronized ErrorTypeHandlingConfig consumerErrorHandlingConfig() {
        if (consume == null) {
            consume = defaultErrorTypeHandlingConfig("ConsumeError");
        }
        if (consume.loggerName() == null) {
            consume.loggerName("ConsumeError");
        }
        return consume;
    }

    @JsonIgnore
    public synchronized ErrorTypeHandlingConfig producerErrorHandlingConfig() {
        if (produce == null) {
            produce = defaultErrorTypeHandlingConfig("ProduceError");
        }
        if (produce.loggerName() == null) {
            produce.loggerName("ProduceError");
        }
        return produce;
    }

    @JsonIgnore
    public synchronized ErrorTypeHandlingConfig processErrorHandlingConfig() {
        if (process == null) {
            process = defaultErrorTypeHandlingConfig("ProcessError");
        }
        if (process.loggerName() == null) {
            process.loggerName("ProcessError");
        }
        return process;
    }

    private ErrorTypeHandlingConfig defaultErrorTypeHandlingConfig(String logger) {
        var errorHandlingConfig = new ErrorTypeHandlingConfig();
        errorHandlingConfig.loggerName(logger);
        return errorHandlingConfig;
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    @JsonClassDescription("Contains the configuration on how to handle Errors.")
    @Data
    public static class ErrorTypeHandlingConfig {
        @JsonProperty(value = "log", required = false)
        @JsonPropertyDescription("Toggle to enable logging the error. Defaults to true.")
        private boolean log = true;
        @JsonProperty(value = "logPayload", required = false)
        @JsonPropertyDescription("Toggle to add the payload which caused the error to the logged output. Defaults to false.")
        private boolean logPayload = false;
        @JsonProperty(value = "loggerName", required = false)
        @JsonPropertyDescription("Sets the logger name used for handling. Setting these combined with setting specific logger configuration allows the log entries to be filtered, or written to another location.")
        private String loggerName;
        @JsonProperty(value = "handler", required = false)
        @JsonPropertyDescription("Controls the behaviour of handling the error, which can be Stop KSML (stopOnFail), Continue KSML (continueOnFail) and Retry (retryOnFail)")
        private Handler handler = Handler.STOP;

        @JsonClassDescription("Enumeration controlling error handling behaviour.")
        public enum Handler {
            @JsonProperty("stopOnFail")
            STOP,
            @JsonProperty("continueOnFail")
            CONTINUE,
            @JsonProperty("retryOnFail")
            RETRY;

            @JsonCreator
            public static Handler forValues(String value) {
                if (value == null) {
                    return null;
                }

                return switch (value) {
                    case "continue", "continueOnFail" -> CONTINUE;
                    case "stop", "stopOnFail" -> STOP;
                    case "retry", "retryOnFail" -> RETRY;
                    default -> null;
                };
            }
        }
    }
}
