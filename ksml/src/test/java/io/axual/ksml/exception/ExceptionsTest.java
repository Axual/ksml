package io.axual.ksml.exception;

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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionsTest {

    private static final Throwable CAUSE = new IllegalStateException("root");

    @Test
    void topologyExceptionCarriesMessageAndCause() {
        assertThat(new TopologyException("broken")).hasMessageContaining("broken");
        assertThat(new TopologyException("broken", CAUSE)).hasMessageContaining("broken").hasCause(CAUSE);
    }

    @Test
    void metricRegistrationExceptionCarriesMessageAndCause() {
        assertThat(new MetricRegistrationException("dup")).hasMessageContaining("dup");
        assertThat(new MetricRegistrationException("dup", CAUSE)).hasMessageContaining("dup").hasCause(CAUSE);
    }

    @Test
    void metricObjectNamingExceptionCarriesMessageAndCause() {
        assertThat(new MetricObjectNamingException("bad name")).hasMessageContaining("bad name");
        assertThat(new MetricObjectNamingException("bad name", CAUSE)).hasMessageContaining("bad name").hasCause(CAUSE);
    }
}
