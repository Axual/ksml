package io.axual.ksml.client.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

class ConfigExceptionTest {
    @Test
    @DisplayName("The default message reports the offending key and value")
    void defaultMessageIncludesKeyAndValue() {
        assertThat(new ConfigException("my.key", "my.value"))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining(ConfigException.DEFAULT_MESSAGE)
                .hasMessageContaining("my.key")
                .hasMessageContaining("my.value");
    }

    @Test
    @DisplayName("A null value is rendered as the literal 'null'")
    void nullValueIsRendered() {
        assertThat(new ConfigException("my.key", null))
                .hasMessageContaining("my.key")
                .hasMessageContaining("null");
    }

    @Test
    @DisplayName("A custom message replaces the default message")
    void customMessageIsUsed() {
        assertThat(new ConfigException("my.key", "my.value", "Custom problem"))
                .hasMessageContaining("Custom problem")
                .hasMessageContaining("my.key")
                .hasMessageContaining("my.value");
    }
}
