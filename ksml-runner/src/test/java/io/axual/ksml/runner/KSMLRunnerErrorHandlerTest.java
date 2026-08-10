package io.axual.ksml.runner;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig;
import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig.Handler;
import io.axual.ksml.runner.exception.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KSMLRunnerErrorHandlerTest {

    @ParameterizedTest(name = "handler {0} -> {1}, retry-capable")
    @CsvSource({
            "STOP,     STOP_ON_FAIL",
            "CONTINUE, CONTINUE_ON_FAIL",
            "RETRY,    RETRY_ON_FAIL"
    })
    void mapsEveryHandlerToItsHandlerTypeWhenRetryIsSupported(Handler handler, ErrorHandler.HandlerType expected) {
        final var cfg = new ErrorTypeHandlingConfig();
        cfg.handler(handler);
        assertThat(KSMLRunner.getErrorHandler(cfg, true).handlerType()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "handler {0} -> {1}, non-retry-capable")
    @CsvSource({
            "STOP,     STOP_ON_FAIL",
            "CONTINUE, CONTINUE_ON_FAIL"
    })
    void mapsNonRetryHandlersEvenWhenRetryIsUnsupported(Handler handler, ErrorHandler.HandlerType expected) {
        final var cfg = new ErrorTypeHandlingConfig();
        cfg.handler(handler);
        assertThat(KSMLRunner.getErrorHandler(cfg, false).handlerType()).isEqualTo(expected);
    }

    @Test
    void rejectsRetryOnFailWhenNotSupported() {
        final var cfg = new ErrorTypeHandlingConfig();
        cfg.handler(Handler.RETRY); // e.g. configured for consume or process, which cannot retry
        assertThatThrownBy(() -> KSMLRunner.getErrorHandler(cfg, false))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("retryOnFail");
    }

    @Test
    void nullHandlerFallsBackToStopWithoutThrowing() {
        final var cfg = new ErrorTypeHandlingConfig();
        cfg.handler(null); // mirrors an empty "handler:" entry in the YAML config
        assertThatCode(() -> KSMLRunner.getErrorHandler(cfg, false)).doesNotThrowAnyException();
        assertThat(KSMLRunner.getErrorHandler(cfg, false).handlerType())
                .isEqualTo(ErrorHandler.HandlerType.STOP_ON_FAIL);
    }
}
