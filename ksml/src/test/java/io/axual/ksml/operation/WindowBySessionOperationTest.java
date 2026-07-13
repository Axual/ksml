package io.axual.ksml.operation;

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

import io.axual.ksml.stream.SessionWindowedCogroupedKStreamWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static org.assertj.core.api.Assertions.assertThat;

class WindowBySessionOperationTest {

    private static SessionWindows sessionWindows() {
        return SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("windowing a grouped stream by session produces a session-windowed stream")
    void applyToGroupedStreamReturnsSessionWindowed() {
        final var operation = new WindowBySessionOperation(operationConfig("windowBySession"), sessionWindows());
        assertThat(operation.apply(groupedStream(), mockContext())).isInstanceOf(SessionWindowedKStreamWrapper.class);
    }

    @Test
    @DisplayName("windowing a cogrouped stream by session produces a session-windowed cogrouped stream")
    void applyToCogroupedStreamReturnsSessionWindowedCogrouped() {
        final var operation = new WindowBySessionOperation(operationConfig("windowBySession"), sessionWindows());
        assertThat(operation.apply(cogroupedStream(), mockContext())).isInstanceOf(SessionWindowedCogroupedKStreamWrapper.class);
    }
}
