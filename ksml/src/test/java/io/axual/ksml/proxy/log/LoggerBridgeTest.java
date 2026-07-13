package io.axual.ksml.proxy.log;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class LoggerBridgeTest {

    private final LoggerBridge bridge = new LoggerBridge();

    @Test
    @DisplayName("getLogger returns a logger carrying the requested name")
    void getLoggerReturnsNamedLogger() {
        assertThat(bridge.getLogger("io.axual.ksml.test").getName()).isEqualTo("io.axual.ksml.test");
    }

    @Test
    @DisplayName("all log-level enablement checks execute without throwing")
    void levelChecksDelegateWithoutError() {
        final var logger = bridge.getLogger("io.axual.ksml.test");
        assertThatCode(() -> {
            logger.isTraceEnabled();
            logger.isDebugEnabled();
            logger.isInfoEnabled();
            logger.isWarnEnabled();
            logger.isErrorEnabled();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("logging at every level with zero, one or two arguments does not throw")
    void logMethodsDoNotThrow() {
        final var logger = bridge.getLogger("io.axual.ksml.test");
        assertThatCode(() -> {
            logger.trace("trace");
            logger.trace("trace {}", "one");
            logger.trace("trace {} {}", "one", "two");
            logger.debug("debug");
            logger.debug("debug {}", "one");
            logger.debug("debug {} {}", "one", "two");
            logger.info("info");
            logger.info("info {}", "one");
            logger.info("info {} {}", "one", "two");
            logger.warn("warn");
            logger.warn("warn {}", "one");
            logger.warn("warn {} {}", "one", "two");
            logger.error("error");
            logger.error("error {}", "one");
            logger.error("error {} {}", "one", "two");
        }).doesNotThrowAnyException();
    }
}
