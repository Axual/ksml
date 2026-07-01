package io.axual.ksml.runner.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

/**
 * Tests for {@link ConfigException} and {@link RunnerException}, focusing on the message composition
 * (the {@code MESSAGE_DETAIL_FORMAT} key/value rendering and its null handling) and cause propagation
 * rather than merely asserting that the exceptions can be thrown.
 */
class RunnerExceptionsTest {

    @Test
    @DisplayName("The (key, value) constructor renders the default message with the formatted detail")
    void configExceptionKeyValueUsesDefaultMessage() {
        final var exception = new ConfigException("myKey", "myValue");

        assertThat(exception).isInstanceOf(RunnerException.class);
        assertThat(exception.getMessage())
                .startsWith(ConfigException.DEFAULT_MESSAGE)
                .contains("Configuration Key   : 'myKey'")
                .contains("Configuration Value : 'myValue'");
    }

    @Test
    @DisplayName("A null configuration value is rendered as the literal 'null'")
    void configExceptionRendersNullValue() {
        final var exception = new ConfigException("myKey", null, "custom message");

        assertThat(exception.getMessage())
                .startsWith("custom message")
                .contains("Configuration Key   : 'myKey'")
                .contains("Configuration Value : 'null'");
    }

    @Test
    @DisplayName("The (key, value, message, cause) constructor formats the detail and keeps the cause")
    void configExceptionKeyValueMessageAndCause() {
        final var cause = new RuntimeException("root");
        final var exception = new ConfigException("dir", "/bad/path", "Could not create the directory", cause);

        assertThat(exception.getMessage())
                .startsWith("Could not create the directory")
                .contains("Configuration Key   : 'dir'")
                .contains("Configuration Value : '/bad/path'");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    @DisplayName("The (key, value, message, cause) constructor renders a null value as 'null'")
    void configExceptionKeyValueMessageAndCauseWithNullValue() {
        final var cause = new RuntimeException("root");
        final var exception = new ConfigException("dir", null, "Could not create the directory", cause);

        assertThat(exception.getMessage()).contains("Configuration Value : 'null'");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
