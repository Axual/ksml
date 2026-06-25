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

import io.axual.ksml.metric.MetricName;
import io.axual.ksml.metric.MetricTag;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.metric.MetricsRegistry;
import io.axual.ksml.runner.config.PrometheusConfig;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.registry.PrometheusScrapeRequest;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link HelpEnrichingCollector}. Runs entirely in-process (no external system): one group
 * drives the real Prometheus JMX exporter against in-JVM MBeans and the shipped default configuration,
 * the other exercises the rewrite logic in isolation with a stub delegate.
 */
class HelpEnrichingCollectorTest {
    private static final String EXECUTION_TIME_HELP = "Execution time statistics of a KSML user function per invocation; durations are in milliseconds and rates are per second";
    private static final String COUNTER_HELP = "User-defined counter metric registered from KSML user code";
    private static final String APP_INFO_HELP = "Build and version information for the running KSML application; the value is always 1 and the details are exposed as labels";
    private static final String DEFAULT_HELP_PREFIX = "Attribute exposed for management";

    /**
     * Verifies the whole chain: KSML custom metrics exported through the shipped default Prometheus
     * configuration and wrapped by {@link HelpEnrichingCollector} expose an informative HELP text while
     * keeping the labels that the JMX exporter derives from the metric's JMX bean properties.
     */
    @Nested
    @DisplayName("Through the real JMX exporter and shipped default configuration")
    class ViaJmxExporter {
        private static final String JMX_DOMAIN = "ksml";

        private MetricsRegistry registry;

        @BeforeEach
        void registerKsmlMetricsAsJmxBeans() {
            registry = new MetricsRegistry();
            registry.enableJmx(JMX_DOMAIN, List.<MetricTag>of());

            // execution-time: registered by io.axual.ksml.python.Invoker for every user function
            registry.registerTimer(new MetricName("execution-time", new MetricTags()
                    .append("function-type", "forEach")
                    .append("function-name", "pipelines_consume_avro_forEach")));

            // record_e2e_latency_*_ms: registered by io.axual.ksml.metric.KsmlTagEnricher
            registry.registerGauge(new MetricName("record_e2e_latency_avg_ms", latencyTags()), () -> 1.0);
            registry.registerGauge(new MetricName("record_e2e_latency_min_ms", latencyTags()), () -> 1.0);
            registry.registerGauge(new MetricName("record_e2e_latency_max_ms", latencyTags()), () -> 1.0);

            // user-defined-*: registered by io.axual.ksml.proxy.metric.MetricsBridge from user code
            registry.registerCounter(new MetricName("user-defined-counter", new MetricTags().append("custom-name", "my_counter")));
            registry.registerMeter(new MetricName("user-defined-meter", new MetricTags().append("custom-name", "my_meter")));
            registry.registerTimer(new MetricName("user-defined-timer", new MetricTags().append("custom-name", "my_timer")));
        }

        private MetricTags latencyTags() {
            return new MetricTags().append("namespace", "inspect").append("pipeline", "consume_avro");
        }

        @AfterEach
        void cleanup() {
            registry.removeAll();
            registry.disableJmx();
        }

        @Test
        @DisplayName("Every KSML custom metric gets an informative HELP text while keeping its labels")
        void enrichesHelpTextAndPreservesLabels() throws Exception {
            final var prometheusConfig = new PrometheusConfig();
            prometheusConfig.enabled(true);
            final var jmxCollector = new JmxCollector(prometheusConfig.getConfigFile())
                    .register(new PrometheusRegistry());
            final var collector = new HelpEnrichingCollector(jmxCollector);

            final var snapshots = collector.collect();

            assertHelp(snapshots, "ksml_execution_time", "Execution time statistics", List.of("function_name", "function_type"));
            assertHelp(snapshots, "ksml_record_e2e_latency_avg_ms", "Average end-to-end latency", List.of("namespace", "pipeline"));
            assertHelp(snapshots, "ksml_record_e2e_latency_min_ms", "Minimum end-to-end latency", List.of("namespace", "pipeline"));
            assertHelp(snapshots, "ksml_record_e2e_latency_max_ms", "Maximum end-to-end latency", List.of("namespace", "pipeline"));
            assertHelp(snapshots, "ksml_user_defined_counter", "User-defined counter", List.of("custom_name"));
            assertHelp(snapshots, "ksml_user_defined_meter", "User-defined meter", List.of("custom_name"));
            assertHelp(snapshots, "ksml_user_defined_timer", "User-defined timer", List.of("custom_name"));
        }

        @Test
        @DisplayName("PrometheusExport wiring keeps the JMX exporter's operational metrics and enriches KSML help")
        void wiringPreservesOperationalMetricsAndEnrichesHelp() throws Exception {
            final var prometheusConfig = new PrometheusConfig();
            prometheusConfig.enabled(true);
            final var prometheusRegistry = new PrometheusRegistry();

            PrometheusExport.registerJmxCollectorWithHelpText(prometheusRegistry, prometheusConfig.getConfigFile());

            final var snapshots = prometheusRegistry.scrape();
            final var names = snapshots.stream().map(s -> s.getMetadata().getName()).toList();

            // The JMX exporter's own operational metrics must survive the unregister of the raw collector
            assertTrue(names.contains("jmx_scrape_duration_seconds"),
                    "jmx_scrape_duration_seconds operational metric was lost. Exported names: " + names.stream().sorted().toList());
            assertTrue(names.contains("jmx_config_reload_success"),
                    "jmx_config_reload_success operational metric was lost. Exported names: " + names.stream().sorted().toList());

            // ... and the KSML custom metrics must be exposed with the enriched HELP text and their labels
            assertHelp(snapshots, "ksml_execution_time", "Execution time statistics", List.of("function_name", "function_type"));
        }

        /**
         * Asserts that every exported snapshot whose name starts with {@code namePrefix} carries a HELP
         * text containing {@code expectedHelpFragment} (and not the generic JMX exporter default), and
         * that each of {@code expectedLabels} is present on its first data point.
         */
        private void assertHelp(MetricSnapshots snapshots, String namePrefix, String expectedHelpFragment, List<String> expectedLabels) {
            final var matches = snapshots.stream()
                    .filter(s -> s.getMetadata().getName().startsWith(namePrefix))
                    .toList();

            if (matches.isEmpty()) {
                fail("No exported metric found with name starting with '" + namePrefix + "'. Exported names: "
                        + snapshots.stream().map(s -> s.getMetadata().getName()).sorted().toList());
            }

            for (final var snapshot : matches) {
                final var help = snapshot.getMetadata().getHelp();
                assertFalse(help != null && help.startsWith(DEFAULT_HELP_PREFIX),
                        "Metric '" + snapshot.getMetadata().getName() + "' still uses the generic default HELP text: " + help);
                assertTrue(help != null && help.contains(expectedHelpFragment),
                        "Metric '" + snapshot.getMetadata().getName() + "' HELP '" + help + "' does not contain '" + expectedHelpFragment + "'");

                final var labels = snapshot.getDataPoints().get(0).getLabels();
                for (final var expectedLabel : expectedLabels) {
                    assertTrue(labels.contains(expectedLabel),
                            "Metric '" + snapshot.getMetadata().getName() + "' lost expected label '" + expectedLabel
                                    + "'. Present labels: " + labels);
                }
            }
        }
    }

    /**
     * Exercises the rewrite logic with a stub delegate, covering branches the JMX-backed group cannot
     * reach: counter-typed metrics, non-matching/pre-set HELP passthrough, the predicate-based collect
     * overloads, and name delegation.
     */
    @Nested
    @DisplayName("Rewrite logic with a stub delegate")
    class WithStubDelegate {

        @Test
        @DisplayName("Rewrites HELP for matching gauges and counters, preserving the snapshot type")
        void rewritesGaugeAndCounter() {
            final var input = MetricSnapshots.builder()
                    .metricSnapshot(gauge("ksml_execution_time_count", "Attribute exposed for management ..."))
                    .metricSnapshot(gauge("ksml_app", "ksml:name=null,type=app-info,attribute=Value"))
                    .metricSnapshot(counter("ksml_user_defined_counter_count", "Attribute exposed for management ..."))
                    .build();
            final var collector = new HelpEnrichingCollector(new StubCollector(input));

            final var result = collector.collect();

            assertEquals(EXECUTION_TIME_HELP, helpOf(result, "ksml_execution_time_count"));
            assertEquals(APP_INFO_HELP, helpOf(result, "ksml_app"));
            assertEquals(COUNTER_HELP, helpOf(result, "ksml_user_defined_counter_count"));
            // the counter must stay a counter, not be downgraded to a gauge
            assertInstanceOf(CounterSnapshot.class, byName(result, "ksml_user_defined_counter_count"));
        }

        @Test
        @DisplayName("Leaves non-KSML metrics and already-described metrics untouched (same instance)")
        void leavesOthersUntouched() {
            final var nonKsml = gauge("jvm_threads_current", "JVM thread count");
            final var alreadySet = gauge("ksml_execution_time_count", EXECUTION_TIME_HELP);
            final var input = MetricSnapshots.builder().metricSnapshot(nonKsml).metricSnapshot(alreadySet).build();
            final var collector = new HelpEnrichingCollector(new StubCollector(input));

            final var result = collector.collect();

            // unchanged snapshots are passed through as the exact same object (no needless rebuild)
            assertSame(nonKsml, byName(result, "jvm_threads_current"));
            assertSame(alreadySet, byName(result, "ksml_execution_time_count"));
        }

        @Test
        @DisplayName("Both predicate collect overloads and getPrometheusNames delegate and enrich")
        void delegatesAllCollectVariants() {
            final var input = MetricSnapshots.builder()
                    .metricSnapshot(gauge("ksml_execution_time_count", "Attribute exposed for management ...")).build();
            final var stub = new StubCollector(input);
            final var collector = new HelpEnrichingCollector(stub);
            final Predicate<String> all = name -> true;

            assertEquals(stub.names, collector.getPrometheusNames());
            assertEquals(EXECUTION_TIME_HELP, helpOf(collector.collect(all), "ksml_execution_time_count"));
            assertEquals(EXECUTION_TIME_HELP, helpOf(collector.collect(all, (PrometheusScrapeRequest) null), "ksml_execution_time_count"));
        }
    }

    private static GaugeSnapshot gauge(String name, String help) {
        return GaugeSnapshot.builder()
                .name(name)
                .help(help)
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("function_name", "f"))
                        .value(1.0)
                        .build())
                .build();
    }

    private static CounterSnapshot counter(String name, String help) {
        return CounterSnapshot.builder()
                .name(name)
                .help(help)
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder()
                        .labels(Labels.of("custom_name", "c"))
                        .value(2.0)
                        .build())
                .build();
    }

    private static String helpOf(MetricSnapshots snapshots, String name) {
        return byName(snapshots, name).getMetadata().getHelp();
    }

    private static MetricSnapshot byName(MetricSnapshots snapshots, String name) {
        return snapshots.stream().filter(s -> s.getMetadata().getName().equals(name)).findFirst().orElseThrow();
    }

    /** Minimal delegate that returns a fixed set of snapshots for every collect variant. */
    private static final class StubCollector implements MultiCollector {
        private final MetricSnapshots snapshots;
        private final List<String> names = List.of("ksml_execution_time_count");

        private StubCollector(MetricSnapshots snapshots) {
            this.snapshots = snapshots;
        }

        @Override
        public MetricSnapshots collect() {
            return snapshots;
        }

        @Override
        public MetricSnapshots collect(Predicate<String> includedNames) {
            return snapshots;
        }

        @Override
        public MetricSnapshots collect(Predicate<String> includedNames, PrometheusScrapeRequest scrapeRequest) {
            return snapshots;
        }

        @Override
        public List<String> getPrometheusNames() {
            return names;
        }
    }
}
