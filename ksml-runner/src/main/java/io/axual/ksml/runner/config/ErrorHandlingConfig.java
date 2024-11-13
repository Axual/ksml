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
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class ErrorHandlingConfig {
    private ErrorTypeHandlingConfig consume;
    private ErrorTypeHandlingConfig produce;
    private ErrorTypeHandlingConfig process;

    public ErrorTypeHandlingConfig consumerErrorHandlingConfig() {
        if (consume == null) {
            return defaultErrorTypeHandlingConfig("ConsumeError");
        }
        if (consume.loggerName() == null) {
            consume.loggerName("ConsumeError");
        }
        return consume;
    }

    public ErrorTypeHandlingConfig producerErrorHandlingConfig() {
        if (produce == null) {
            return defaultErrorTypeHandlingConfig("ProduceError");
        }
        if (produce.loggerName() == null) {
            produce.loggerName("ProduceError");
        }
        return produce;
    }

    public ErrorTypeHandlingConfig processErrorHandlingConfig() {
        if (process == null) {
            return defaultErrorTypeHandlingConfig("ProcessError");
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Data
    public static class ErrorTypeHandlingConfig {
        private boolean log = true;
        private boolean logPayload = false;
        private String loggerName;
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
