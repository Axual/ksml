package io.axual.ksml.metric;

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

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static io.axual.ksml.metric.KsmlMetricsReporter.ENRICHER_INSTANCE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KsmlMetricsReporterUnitTest {

    @Mock
    private KsmlTagEnricher enricher;
    @Mock
    private KafkaMetric kafkaMetric;

    private KsmlMetricsReporter reporter;

    private static org.apache.kafka.common.MetricName kafkaName(String name) {
        return new org.apache.kafka.common.MetricName(name, "group", "desc", Map.of());
    }

    @BeforeEach
    void setUp() {
        reporter = new KsmlMetricsReporter(enricher);
        reporter.setRegistry(new MetricsRegistry(new MetricRegistry()));
    }

    // --- configure -------------------------------------------------------------------------------

    @Test
    @DisplayName("configure fails when the enricher instance config is missing")
    void configureFailsWhenEnricherMissing() {
        assertThatThrownBy(() -> reporter.configure(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ENRICHER_INSTANCE_CONFIG);
    }

    @Test
    @DisplayName("configure fails when the enricher config value has the wrong type")
    void configureFailsWhenEnricherHasWrongType() {
        final Map<String, Object> configs = Map.of(ENRICHER_INSTANCE_CONFIG, "not an enricher");
        assertThatThrownBy(() -> reporter.configure(configs))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("configure succeeds when a valid enricher is supplied")
    void configureAcceptsEnricher() {
        assertThatCode(() -> reporter.configure(Map.of(ENRICHER_INSTANCE_CONFIG, enricher)))
                .doesNotThrowAnyException();
    }

    // --- metricChange ----------------------------------------------------------------------------

    @Test
    @DisplayName("metricChange ignores a null metric")
    void metricChangeIgnoresNullMetric() {
        assertThatCode(() -> reporter.metricChange(null)).doesNotThrowAnyException();
        assertThat(reporter.getRegisteredMetrics()).isEmpty();
    }

    @Test
    @DisplayName("metricChange ignores a metric when the enricher is not initialized")
    void metricChangeIgnoresMetricWhenEnricherNotInitialized() {
        when(kafkaMetric.metricName()).thenReturn(kafkaName("m"));
        try (final var noEnricher = new KsmlMetricsReporter()) {
            noEnricher.setRegistry(new MetricsRegistry(new MetricRegistry()));

            assertThatCode(() -> noEnricher.metricChange(kafkaMetric)).doesNotThrowAnyException();
            assertThat(noEnricher.getRegisteredMetrics()).isEmpty();
        }
    }

    @Test
    @DisplayName("metricChange ignores a metric the enricher considers uninteresting")
    void metricChangeIgnoresUninterestingMetric() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(false);
        reporter.metricChange(kafkaMetric);
        assertThat(reporter.getRegisteredMetrics()).isEmpty();
    }

    @Test
    @DisplayName("metricChange drops a metric when the enricher yields no name")
    void metricChangeDropsMetricWithoutEnrichedName() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        when(kafkaMetric.metricName()).thenReturn(kafkaName("m"));
        when(enricher.toKsmlMetricName(kafkaMetric.metricName())).thenReturn(null);

        reporter.metricChange(kafkaMetric);
        assertThat(reporter.getRegisteredMetrics()).isEmpty();
    }

    @Test
    @DisplayName("metricChange registers an interesting metric as a gauge")
    void metricChangeRegistersGauge() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        final var name = kafkaName("m");
        when(kafkaMetric.metricName()).thenReturn(name);
        when(enricher.toKsmlMetricName(name)).thenReturn(new MetricName("ksml.metric"));

        reporter.metricChange(kafkaMetric);

        assertThat(reporter.getRegisteredMetrics()).containsKey(name);
    }

    @Test
    @DisplayName("metricChange re-registers a metric under a new enriched name")
    void metricChangeReRegistersUnderNewName() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        final var name = kafkaName("m");
        when(kafkaMetric.metricName()).thenReturn(name);
        when(enricher.toKsmlMetricName(name))
                .thenReturn(new MetricName("ksml.first"))
                .thenReturn(new MetricName("ksml.second"));

        reporter.metricChange(kafkaMetric);
        reporter.metricChange(kafkaMetric);

        assertThat(reporter.getRegisteredMetrics()).containsEntry(name, new MetricName("ksml.second"));
    }

    // --- createGauge -----------------------------------------------------------------------------

    @Test
    @DisplayName("createGauge exposes a numeric metric value")
    void createGaugeReturnsNumericValue() {
        when(kafkaMetric.metricValue()).thenReturn(5.0d);
        assertThat(reporter.createGauge(kafkaMetric).getAsDouble()).isEqualTo(5.0d);
    }

    @Test
    @DisplayName("createGauge returns NaN for a non-numeric metric value")
    void createGaugeReturnsNaNForNonNumericValue() {
        lenient().when(kafkaMetric.metricName()).thenReturn(kafkaName("m"));
        when(kafkaMetric.metricValue()).thenReturn("not a number");
        assertThat(reporter.createGauge(kafkaMetric).getAsDouble()).isNaN();
    }

    @Test
    @DisplayName("createGauge returns NaN when reading the metric value throws")
    void createGaugeReturnsNaNWhenValueReadThrows() {
        lenient().when(kafkaMetric.metricName()).thenReturn(kafkaName("m"));
        when(kafkaMetric.metricValue()).thenThrow(new RuntimeException("boom"));
        assertThat(reporter.createGauge(kafkaMetric).getAsDouble()).isNaN();
    }

    // --- removal / close / init ------------------------------------------------------------------

    @Test
    @DisplayName("metricRemoval removes a previously registered metric")
    void metricRemovalRemovesRegisteredMetric() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        final var name = kafkaName("m");
        when(kafkaMetric.metricName()).thenReturn(name);
        when(enricher.toKsmlMetricName(name)).thenReturn(new MetricName("ksml.metric"));
        reporter.metricChange(kafkaMetric);

        reporter.metricRemoval(kafkaMetric);

        assertThat(reporter.getRegisteredMetrics()).isEmpty();
    }

    @Test
    @DisplayName("metricRemoval of an unregistered metric is a no-op")
    void metricRemovalOfUnknownMetricIsNoOp() {
        when(kafkaMetric.metricName()).thenReturn(kafkaName("m"));
        assertThatCode(() -> reporter.metricRemoval(kafkaMetric)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("close removes all registered metrics")
    void closeRemovesAllRegisteredMetrics() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        final var name = kafkaName("m");
        when(kafkaMetric.metricName()).thenReturn(name);
        when(enricher.toKsmlMetricName(name)).thenReturn(new MetricName("ksml.metric"));
        reporter.metricChange(kafkaMetric);

        reporter.close();

        assertThat(reporter.getRegisteredMetrics()).isEmpty();
    }

    @Test
    @DisplayName("init forwards each supplied metric through metricChange")
    void initForwardsEachMetricToMetricChange() {
        when(enricher.isInteresting(kafkaMetric)).thenReturn(true);
        final var name = kafkaName("m");
        when(kafkaMetric.metricName()).thenReturn(name);
        when(enricher.toKsmlMetricName(name)).thenReturn(new MetricName("ksml.metric"));

        reporter.init(List.of(kafkaMetric));

        assertThat(reporter.getRegisteredMetrics()).containsKey(name);
    }
}
