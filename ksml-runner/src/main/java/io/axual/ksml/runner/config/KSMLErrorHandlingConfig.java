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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Jacksonized
public class KSMLErrorHandlingConfig {
    private final ErrorHandlingConfig consume;
    private final ErrorHandlingConfig produce;
    private final ErrorHandlingConfig process;

    public ErrorHandlingConfig consumerErrorHandlingConfig() {
        if (consume == null) {
            return defaultErrorHandlingConfig("ConsumeError");
        }
        if (consume.loggerName() == null) {
            consume.loggerName("ConsumeError");
        }
        return consume;
    }

    public ErrorHandlingConfig producerErrorHandlingConfig() {
        if (produce == null) {
            return defaultErrorHandlingConfig("ProduceError");
        }
        if (produce.loggerName() == null) {
            produce.loggerName("ProduceError");
        }
        return produce;
    }

    public ErrorHandlingConfig processErrorHandlingConfig() {
        if (process == null) {
            return defaultErrorHandlingConfig("ProcessError");
        }
        if (process.loggerName() == null) {
            process.loggerName("ProcessError");
        }
        return process;
    }

    private ErrorHandlingConfig defaultErrorHandlingConfig(String logger) {
        var errorHandlingConfig = new ErrorHandlingConfig();
        errorHandlingConfig.loggerName(logger);
        return errorHandlingConfig;
    }

    @Data
    public static class ErrorHandlingConfig {
        @JsonProperty("log")
        private boolean log = true;
        @JsonProperty("logPayload")
        private boolean logPayload = false;
        @JsonProperty("loggerName")
        private String loggerName;
        @JsonProperty("handler")
        private Handler handler = Handler.STOP;

        public enum Handler {
            STOP,
            CONTINUE;

            @JsonCreator
            public static Handler forValues(String value) {
                if (value == null) {
                    return null;
                }

                return switch (value) {
                    case "continue", "continueOnFail" -> CONTINUE;
                    case "stop", "stopOnFail" -> STOP;
                    default -> null;
                };
            }
        }
    }
}
