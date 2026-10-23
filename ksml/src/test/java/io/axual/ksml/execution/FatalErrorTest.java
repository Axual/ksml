package io.axual.ksml.execution;

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

class FatalErrorTest {

    @Test
    @DisplayName("report wraps a throwable into a runtime exception carrying its message")
    void reportReturnsRuntimeExceptionWithMessage() {
        final var reported = FatalError.report(new IllegalStateException("boom"));
        assertThat(reported).isInstanceOf(RuntimeException.class).hasMessage("boom");
    }

    @Test
    @DisplayName("report returns the top-level message and keeps the reported throwable as cause")
    void reportReturnsTopLevelMessageAndKeepsCause() {
        final var cause = new IllegalArgumentException("root cause");
        final var wrapper = new RuntimeException("wrapper", cause);
        final var reported = FatalError.report(wrapper);
        // report() logs the full cause chain and returns a fresh exception with the top-level message,
        // which keeps the reported throwable, so the root cause can still be found.
        assertThat(reported).hasMessage("wrapper").hasCause(wrapper).hasRootCause(cause);
    }
}
