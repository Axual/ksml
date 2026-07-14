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

class ClientExceptionTest {
    @Test
    @DisplayName("A message is prefixed with the exception name")
    void messageIsPrefixed() {
        assertThat(new ClientException("boom")).hasMessage("ClientException: boom");
    }

    @Test
    @DisplayName("A cause is retained alongside the prefixed message")
    void messageAndCauseAreRetained() {
        final var cause = new IllegalStateException("root");
        assertThat(new ClientException("boom", cause))
                .hasMessage("ClientException: boom")
                .hasCause(cause);
    }

    @Test
    @DisplayName("NotSupportedException inherits the ClientException message prefix")
    void notSupportedExceptionIsPrefixed() {
        assertThat(new NotSupportedException("nope"))
                .isInstanceOf(ClientException.class)
                .hasMessage("ClientException: nope");
    }
}
