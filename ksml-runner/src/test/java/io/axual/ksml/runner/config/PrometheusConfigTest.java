package io.axual.ksml.runner.config;

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

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusConfigTest {

    @Test
    @DisplayName("When disabled, host, port and config file are hidden (null)")
    void disabledHidesEverything() {
        final var config = new PrometheusConfig();      // enabled defaults to false
        assertThat(config.getHost()).isNull();
        assertThat(config.getPort()).isNull();
        assertThat(config.getConfigFile()).isNull();
    }

    @Test
    @DisplayName("When enabled, the default host and port are exposed")
    void enabledExposesDefaults() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        assertThat(config.getHost()).isEqualTo("0.0.0.0");
        assertThat(config.getPort()).isEqualTo(9999);
    }

    @Test
    @DisplayName("When enabled with null host/port, the defaults are substituted")
    void enabledFallsBackToDefaultsForNullValues() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        config.host(null);
        config.port(null);
        assertThat(config.getHost()).isEqualTo("0.0.0.0");
        assertThat(config.getPort()).isEqualTo(9999);
    }

    @Test
    @DisplayName("When enabled without a configFile, the internal default config is materialized")
    void enabledFallsBackToInternalDefaultConfigFile() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        assertThat(config.getConfigFile()).isNotNull();          // lazily loaded from the classpath default
    }

    @Test
    @DisplayName("When enabled with an explicit configFile, that file is returned")
    void enabledReturnsExplicitConfigFile() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        config.configFile("/tmp/custom-exporter.yaml");
        assertThat(config.getConfigFile()).isEqualTo(new File("/tmp/custom-exporter.yaml"));
    }

    @Test
    @DisplayName("Copy constructor copies all fields")
    void copyConstructorCopiesFields() {
        final var original = new PrometheusConfig();
        original.enabled(true);
        original.host("127.0.0.1");
        original.port(1234);
        original.configFile("/tmp/x.yaml");

        final var copy = new PrometheusConfig(original);

        assertThat(copy.getHost()).isEqualTo("127.0.0.1");
        assertThat(copy.getPort()).isEqualTo(1234);
        assertThat(copy.getConfigFile()).isEqualTo(new File("/tmp/x.yaml"));
    }
}
