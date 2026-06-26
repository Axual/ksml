package io.axual.ksml.runner.config;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ApplicationServerConfig}, mirroring the "hidden when disabled" semantics of the
 * Prometheus config: host, port and the combined application-server string are only exposed when enabled.
 */
class ApplicationServerConfigTest {

    @Test
    @DisplayName("When disabled, host, port and application server are hidden (null)")
    void disabledHidesEverything() {
        final var config = new ApplicationServerConfig(); // enabled defaults to false

        assertThat(config.getHost()).isNull();
        assertThat(config.getPort()).isNull();
        assertThat(config.getApplicationServer()).isNull();
    }

    @Test
    @DisplayName("When enabled, the default host and port are exposed")
    void enabledExposesDefaults() {
        final var config = new ApplicationServerConfig();
        config.enabled(true);

        assertThat(config.getHost()).isEqualTo("0.0.0.0");
        assertThat(config.getPort()).isEqualTo(8080);
        assertThat(config.getApplicationServer()).isEqualTo("0.0.0.0:8080");
    }

    @Test
    @DisplayName("When enabled, explicit host and port are used")
    void enabledUsesExplicitHostAndPort() {
        final var config = new ApplicationServerConfig();
        config.enabled(true);
        config.host("127.0.0.1");
        config.port(8888);

        assertThat(config.getApplicationServer()).isEqualTo("127.0.0.1:8888");
    }

    @Test
    @DisplayName("When enabled with null host/port, the defaults are substituted")
    void enabledFallsBackToDefaultsForNullValues() {
        final var config = new ApplicationServerConfig();
        config.enabled(true);
        config.host(null);
        config.port(null);

        assertThat(config.getHost()).isEqualTo("0.0.0.0");
        assertThat(config.getPort()).isEqualTo(8080);
    }
}
