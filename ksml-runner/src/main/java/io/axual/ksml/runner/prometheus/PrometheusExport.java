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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.axual.ksml.metric.KSMLMetrics;
import io.axual.ksml.runner.config.PrometheusConfig;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.BufferPoolsExports;
import io.prometheus.client.hotspot.ClassLoadingExports;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryAllocationExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;
import io.prometheus.client.hotspot.VersionInfoExports;
import io.prometheus.jmx.BuildInfoCollector;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.jmx.common.http.HTTPServerFactory;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * Exposes the JMX metrics, based on the Prometheys JMX exporter agent
 */
@Slf4j
public class PrometheusExport implements Closeable {

    private final PrometheusConfig config;

    private HTTPServer httpServer;
    private CollectorRegistry registry;
    private final List<Collector> collectorList = new ArrayList<>();

    public PrometheusExport(PrometheusConfig config) {
        // Use a copy of the provided config
        this.config = (config != null ? config.toBuilder() : PrometheusConfig.builder()).build();

    }

    @Synchronized
    public void start() throws Exception {
        KSMLMetrics.init();
        if (!config.isEnabled()) {
            log.info("Prometheus export is disabled");
            return;
        }
        var configFile = config.getConfigFile();
        if (configFile == null) {
            log.info("No Prometheus export config file found, export disabled");
            return;
        }
        log.info("Loading Prometheus export config from {}", configFile);
        registry = CollectorRegistry.defaultRegistry;

        if (collectorList.isEmpty()) {
            collectorList.add(new BuildInfoCollector());
            collectorList.add(new JmxCollector(configFile));
            collectorList.add(new StandardExports());
            collectorList.add(new MemoryPoolsExports());
            collectorList.add(new MemoryAllocationExports());
            collectorList.add(new BufferPoolsExports());
            collectorList.add(new GarbageCollectorExports());
            collectorList.add(new ThreadExports());
            collectorList.add(new ClassLoadingExports());
            collectorList.add(new VersionInfoExports());
        }
        collectorList.forEach(registry::register);


        httpServer = new HTTPServerFactory()
                .createHTTPServer(
                        new InetSocketAddress(config.getHost(), config.getPort()),
                        registry,
                        true,
                        configFile);
    }

    public synchronized void stop() {
        Optional.ofNullable(httpServer).ifPresent(HTTPServer::close);
        httpServer = null;
        Optional.ofNullable(registry).ifPresent(r -> collectorList.forEach(r::unregister));
        registry = null;
        collectorList.clear();
    }

    @Override
    public void close() {
        stop();
    }
}
