package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;

/**
 * A Kafka MetricsReporter implementation that converts selected Kafka Streams metrics
 * into KSML-compliant metrics using an injected {@link KsmlTagEnricher}.
 */
@NoArgsConstructor // Required by Kafka for reflective instantiation
@Slf4j
public final class KsmlMetricsReporter implements MetricsReporter {

    // Kafka configuration key used to inject a KsmlTagEnricher instance into this reporter
    public static final String ENRICHER_INSTANCE_CONFIG = "ksml.tag.enricher";

    // Package-private for testing
    MetricsRegistry registry;

    private KsmlTagEnricher enricher;
    private final Map<org.apache.kafka.common.MetricName, MetricName> registered = new ConcurrentHashMap<>();

    public KsmlMetricsReporter(KsmlTagEnricher enricher) {
        this.enricher = Objects.requireNonNull(enricher, "enricher");
        this.registry = createRegistry();
    }

    // Package-private for testing
    void setRegistry(MetricsRegistry registry) {
        this.registry = registry;
    }

    // Package-private to allow override in tests
    MetricsRegistry createRegistry() {
        return Metrics.registry();
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    /**
     * Called during reporter initialization to configure the instance.
     * Expects the configuration map to include a reference to a {@link KsmlTagEnricher}
     * under the 'ksml.tag.enricher' key. This enricher will be used to map Kafka metrics
     * to KSML-enriched metrics.
     *
     * @param configs Configuration map passed by Kafka
     * @throws IllegalArgumentException if the required enricher is missing or of the wrong type
     */
    @Override
    public void configure(@NotNull Map<String, ?> configs) {
        Object enricherObj = configs.get(ENRICHER_INSTANCE_CONFIG);
        if (enricherObj == null) {
            throw new IllegalArgumentException(
                    "Config '%s' is required but was not found".formatted(ENRICHER_INSTANCE_CONFIG));
        }
        if (enricherObj instanceof KsmlTagEnricher e) {
            this.enricher = e;
        } else {
            throw new IllegalArgumentException(
                    "Config '%s' must hold a KsmlTagEnricher instance".formatted(ENRICHER_INSTANCE_CONFIG));
        }

        // Initialize registry if not set (for no-arg constructor case)
        if (this.registry == null) {
            this.registry = createRegistry();
        }
    }

    /**
     * Called when a new metric is added or an existing one is updated.
     * If the metric is relevant, it's enriched and registered in the KSML registry.
     */
    @Override
    public void metricChange(KafkaMetric kafkaMetric) {
        if (kafkaMetric == null) {
            log.warn("Received null kafka metric, ignoring");
            return;
        }

        if (enricher == null) {
            log.warn("Tag enricher not initialized, ignoring metric: {}", kafkaMetric.metricName());
            return;
        }

        if (!enricher.isInteresting(kafkaMetric)) {
            return;
        }

        MetricName ksmlName = enricher.toKsmlMetricName(kafkaMetric.metricName());
        if (ksmlName == null) {
            log.debug("Dropping metric due to missing enrichment: {}", kafkaMetric.metricName());
            return;
        }

        try {
            registered.compute(kafkaMetric.metricName(), (ignored, oldName) -> {
                try {
                    if (oldName != null) {
                        registry.remove(oldName);
                    }
                    registry.registerGauge(ksmlName, createGauge(kafkaMetric));
                    return ksmlName;
                } catch (Exception e) {
                    log.warn("Failed to register gauge for metric {}", ksmlName, e);
                    return oldName; // Keep the old registration if new one fails
                }
            });
        } catch (Exception ex) {
            log.warn("Could not register metric {} due to compute operation failure", ksmlName, ex);
        }
    }

    /**
     * Wraps the Kafka metric value in a DoubleSupplier-compatible gauge.
     * Package-private to allow testing.
     *
     * @param m Kafka metric to wrap
     * @return a DoubleSupplier reading the metric value
     */
    DoubleSupplier createGauge(KafkaMetric m) {
        return () -> {
            try {
                Object v = m.metricValue();
                return (v instanceof Number n) ? n.doubleValue() : Double.NaN;
            } catch (Exception e) {
                log.debug("Failed to read metric value for {}: {}", m.metricName(), e.getMessage());
                return Double.NaN;
            }
        };
    }

    /**
     * Removes a previously registered KSML metric if it was enriched.
     */
    @Override
    public void metricRemoval(KafkaMetric kafkaMetric) {
        Optional.ofNullable(registered.remove(kafkaMetric.metricName()))
                .ifPresent(registry::remove);
    }

    /**
     * Closes the reporter by deregistering enriched KSML metrics.
     */
    @Override
    public void close() {
        List<MetricName> toRemove = new ArrayList<>(registered.values());
        registered.clear();
        toRemove.forEach(registry::remove);
    }

    // Package-private for testing
    Map<org.apache.kafka.common.MetricName, MetricName> getRegisteredMetrics() {
        return new ConcurrentHashMap<>(registered);
    }
}