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

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class PrometheusConfigTest {

    @Test
    @DisplayName("When disabled, host, port and config file are hidden (null)")
    void disabledHidesEverything() {
        final var config = new PrometheusConfig();      // enabled defaults to false
        assertNull(config.getHost());
        assertNull(config.getPort());
        assertNull(config.getConfigFile());
    }

    @Test
    @DisplayName("When enabled, the default host and port are exposed")
    void enabledExposesDefaults() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        assertEquals("0.0.0.0", config.getHost());
        assertEquals(9999, config.getPort());
    }

    @Test
    @DisplayName("When enabled without a configFile, the internal default config is materialized")
    void enabledFallsBackToInternalDefaultConfigFile() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        assertNotNull(config.getConfigFile());          // lazily loaded from the classpath default
    }

    @Test
    @DisplayName("When enabled with an explicit configFile, that file is returned")
    void enabledReturnsExplicitConfigFile() {
        final var config = new PrometheusConfig();
        config.enabled(true);
        config.configFile("/tmp/custom-exporter.yaml");
        assertEquals(new File("/tmp/custom-exporter.yaml"), config.getConfigFile());
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

        assertEquals("127.0.0.1", copy.getHost());
        assertEquals(1234, copy.getPort());
        assertEquals(new File("/tmp/x.yaml"), copy.getConfigFile());
    }
}
