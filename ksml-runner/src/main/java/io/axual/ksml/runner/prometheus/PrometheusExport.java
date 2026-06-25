package io.axual.ksml.runner.prometheus;

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

import io.axual.ksml.metric.Metrics;
import io.axual.ksml.runner.config.PrometheusConfig;
import io.prometheus.jmx.BuildInfoMetrics;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.jmx.common.http.HTTPServerFactory;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.management.MalformedObjectNameException;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

/**
 * Exposes the JMX metrics, based on the Prometheus JMX exporter agent
 */
@Slf4j
public class PrometheusExport implements Closeable {

    private final PrometheusConfig config;

    private HTTPServer httpServer;

    public PrometheusExport(PrometheusConfig config) {
        // Use a copy of the provided config
        this.config = new PrometheusConfig(config);
    }

    @Synchronized
    public void start() throws Exception {
        Metrics.init();
        if (!config.enabled()) {
            log.info("Prometheus export is disabled");
            return;
        }
        final var configFile = config.getConfigFile();
        if (configFile == null) {
            log.info("No Prometheus export config file found, export disabled");
            return;
        }
        log.info("Loading Prometheus export config from {}", configFile);

        new BuildInfoMetrics().register(PrometheusRegistry.defaultRegistry);
        JvmMetrics.builder().register(PrometheusRegistry.defaultRegistry);
        registerJmxCollectorWithHelpText(PrometheusRegistry.defaultRegistry, configFile);

        httpServer = new HTTPServerFactory()
                .createHTTPServer(
                        InetAddress.getByName(config.getHost()),
                        config.getPort(),
                        PrometheusRegistry.defaultRegistry,
                        configFile);
    }

    /**
     * Registers the JMX exporter on the given registry with KSML metric HELP text enrichment.
     * <p>
     * The collector is registered first so it initializes its operational metrics
     * (jmx_scrape_duration_seconds, jmx_config_reload_*) as separate collectors on the registry. It is
     * then unregistered and replaced by a {@link HelpEnrichingCollector} wrapper: unregistering the
     * MultiCollector leaves those operational metrics in place, while the wrapper exposes the JMX
     * metrics with descriptive HELP text.
     */
    static void registerJmxCollectorWithHelpText(PrometheusRegistry registry, File configFile) throws IOException, MalformedObjectNameException {
        final var jmxCollector = new JmxCollector(configFile, JmxCollector.Mode.AGENT);
        jmxCollector.register(registry);
        registry.unregister(jmxCollector);
        registry.register(new HelpEnrichingCollector(jmxCollector));
    }

    public synchronized void stop() {
        Optional.ofNullable(httpServer).ifPresent(HTTPServer::close);
        httpServer = null;
    }

    @Override
    public void close() {
        stop();
    }
}
