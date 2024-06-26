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

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Jacksonized
public class KSMLErrorHandlingConfig {
    private ErrorHandlingConfig consume;
    private ErrorHandlingConfig produce;
    private ErrorHandlingConfig process;


    public ErrorHandlingConfig getConsumerErrorHandlingConfig() {
        if (consume == null) {
            return getDefaultErrorHandlingConfig("ConsumeError");
        }
        if (consume.getLoggerName() == null) {
            consume.setLoggerName("ConsumeError");
        }
        return consume;
    }

    public ErrorHandlingConfig getProducerErrorHandlingConfig() {
        if (produce == null) {
            return getDefaultErrorHandlingConfig("ProduceError");
        }
        if (produce.getLoggerName() == null) {
            produce.setLoggerName("ProduceError");
        }
        return produce;
    }

    public ErrorHandlingConfig getProcessErrorHandlingConfig() {
        if (process == null) {
            return getDefaultErrorHandlingConfig("ProcessError");
        }
        if (process.getLoggerName() == null) {
            process.setLoggerName("ProcessError");
        }
        return process;
    }

    private ErrorHandlingConfig getDefaultErrorHandlingConfig(String logger) {
        var errorHandlingConfig = new ErrorHandlingConfig();
        errorHandlingConfig.setLoggerName(logger);
        return errorHandlingConfig;
    }

    @Setter
    @Getter
    public static class ErrorHandlingConfig {
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
