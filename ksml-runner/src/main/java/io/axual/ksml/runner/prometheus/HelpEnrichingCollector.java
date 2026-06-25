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

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusScrapeRequest;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Wraps another collector (the Prometheus JMX exporter) and replaces the generic
 * "Attribute exposed for management ..." HELP text of KSML custom metrics with a human-readable
 * description. Metric names, labels, values and types are passed through unchanged.
 * <p>
 * The JMX exporter can only attach a HELP text to a rule that also defines an explicit metric name,
 * but naming a rule discards the bean properties that KSML relies on for its metric labels (function
 * name, pipeline, namespace, ...). Rewriting the HELP text after collection therefore lets us
 * document the metrics without losing those labels.
 */
@RequiredArgsConstructor
public final class HelpEnrichingCollector implements MultiCollector {
    // HELP text per KSML metric name prefix; a timer/meter expands into several metrics
    // (_count, _max, percentiles, rates) that share the prefix and therefore the description.
    private static final Map<String, String> HELP_BY_NAME_PREFIX = Map.of(
            "ksml_app", "Build and version information for the running KSML application; the value is always 1 and the details are exposed as labels",
            "ksml_execution_time", "Execution time statistics of a KSML user function per invocation; durations are in milliseconds and rates are per second",
            "ksml_record_e2e_latency_avg_ms", "Average end-to-end latency of records from the source topic to this KSML processor node, in milliseconds",
            "ksml_record_e2e_latency_min_ms", "Minimum end-to-end latency of records from the source topic to this KSML processor node, in milliseconds",
            "ksml_record_e2e_latency_max_ms", "Maximum end-to-end latency of records from the source topic to this KSML processor node, in milliseconds",
            "ksml_user_defined_counter", "User-defined counter metric registered from KSML user code",
            "ksml_user_defined_meter", "User-defined meter metric registered from KSML user code; rates are in events per second",
            "ksml_user_defined_timer", "User-defined timer metric registered from KSML user code; durations are in milliseconds and rates are per second");

    private final MultiCollector delegate;

    @Override
    public MetricSnapshots collect() {
        return enrich(delegate.collect());
    }

    @Override
    public MetricSnapshots collect(Predicate<String> includedNames) {
        return enrich(delegate.collect(includedNames));
    }

    @Override
    public MetricSnapshots collect(Predicate<String> includedNames, PrometheusScrapeRequest scrapeRequest) {
        return enrich(delegate.collect(includedNames, scrapeRequest));
    }

    @Override
    public List<String> getPrometheusNames() {
        return delegate.getPrometheusNames();
    }

    // A scrape returns a MetricSnapshots collection: one immutable MetricSnapshot per metric, each
    // holding its metadata (name + HELP) and data points. We copy them through, swapping the HELP
    // text on the ones we recognise.
    private MetricSnapshots enrich(MetricSnapshots snapshots) {
        final var builder = MetricSnapshots.builder();
        for (final var snapshot : snapshots) {
            builder.metricSnapshot(withHelp(snapshot));
        }
        return builder.build();
    }

    private MetricSnapshot withHelp(MetricSnapshot snapshot) {
        final var metadata = snapshot.getMetadata();
        final var name = metadata.getName();
        final var help = helpFor(name);
        // Not a KSML metric we describe, or it already has the right text: leave it as-is.
        if (help == null || help.equals(metadata.getHelp())) {
            return snapshot;
        }
        // Snapshots are immutable, so we cannot just set the HELP; we rebuild the snapshot with new
        // metadata (same name/unit, new help) and the original data points (the values + labels).
        final var enriched = metadata.getUnit() != null
                ? new MetricMetadata(name, help, metadata.getUnit())
                : new MetricMetadata(name, help);
        // A snapshot has a concrete type per metric kind. GaugeSnapshot = a value that goes up/down
        // (latency, execution time); CounterSnapshot = a value that only increases. We must rebuild
        // the same type so the metric keeps its semantics.
        if (snapshot instanceof GaugeSnapshot gauge) {
            return new GaugeSnapshot(enriched, gauge.getDataPoints());
        }
        if (snapshot instanceof CounterSnapshot counter) {
            return new CounterSnapshot(enriched, counter.getDataPoints());
        }
        // KSML custom metrics only surface as gauges or counters; leave anything else untouched.
        return snapshot;
    }

    private String helpFor(String metricName) {
        for (final var entry : HELP_BY_NAME_PREFIX.entrySet()) {
            if (metricName.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }
}
