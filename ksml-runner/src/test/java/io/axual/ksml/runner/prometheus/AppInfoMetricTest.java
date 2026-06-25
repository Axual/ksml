package io.axual.ksml.runner.prometheus;

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

import io.axual.ksml.runner.KsmlInfo;
import io.axual.ksml.runner.config.PrometheusConfig;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Labels;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that the {@code ksml_app} build-info metric, produced by the app-info rule in the shipped
 * default configuration, exposes clean label values (no leftover JMX quoting or bracket artifacts).
 */
class AppInfoMetricTest {
    private static final String APP_ID = "io.axual.ksml.test.app";

    @AfterEach
    void unregisterAppInfoBean() throws Exception {
        final var server = ManagementFactory.getPlatformMBeanServer();
        for (final var name : server.queryNames(new ObjectName("ksml:type=app-info,*"), null)) {
            server.unregisterMBean(name);
        }
    }

    @Test
    @DisplayName("ksml_app exposes app metadata as clean labels, without JMX quotes or brackets")
    void exposesCleanAppInfoLabels() throws Exception {
        KsmlInfo.registerKsmlAppInfo(APP_ID);

        final var prometheusConfig = new PrometheusConfig();
        prometheusConfig.enabled(true);
        final var collector = new JmxCollector(prometheusConfig.getConfigFile())
                .register(new PrometheusRegistry());

        final var appInfo = collector.collect().stream()
                .filter(s -> s.getMetadata().getName().equals("ksml_app"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("ksml_app metric was not exported"));
        final Labels labels = appInfo.getDataPoints().get(0).getLabels();

        assertEquals(APP_ID, labels.get("app_id"));
        // Values come from ksml/ksml-info.properties; assert they are present and unquoted/untrimmed
        assertEquals(KsmlInfo.APP_NAME, labels.get("name"));
        assertEquals(KsmlInfo.APP_VERSION, labels.get("version"));
        assertEquals(KsmlInfo.BUILD_TIME, labels.get("build_time"));
        // Guard against the previous mangling (surrounding quotes and a trailing "><")
        final var buildTime = labels.get("build_time");
        assertFalse(buildTime.contains("\"") || buildTime.contains(">") || buildTime.contains("<"),
                "build_time label is mangled: " + buildTime);
    }
}
