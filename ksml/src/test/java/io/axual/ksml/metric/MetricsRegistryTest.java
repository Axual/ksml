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

import io.axual.ksml.exception.MetricRegistrationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsRegistryTest {

    private final MetricsRegistry registry = new MetricsRegistry();

    private static MetricName name(String value) {
        return new MetricName(value);
    }

    @Test
    @DisplayName("a registered counter can be retrieved")
    void registersAndRetrievesCounter() {
        final var metricName = name("counter");
        final var counter = registry.registerCounter(metricName);
        assertThat(registry.getCounter(metricName)).isSameAs(counter);
    }

    @Test
    @DisplayName("a registered meter can be retrieved")
    void registersAndRetrievesMeter() {
        final var metricName = name("meter");
        final var meter = registry.registerMeter(metricName);
        assertThat(registry.getMeter(metricName)).isSameAs(meter);
    }

    @Test
    @DisplayName("a registered timer can be retrieved")
    void registersAndRetrievesTimer() {
        final var metricName = name("timer");
        final var timer = registry.registerTimer(metricName);
        assertThat(registry.getTimer(metricName)).isSameAs(timer);
    }

    @Test
    @DisplayName("a registered histogram can be retrieved")
    void registersAndRetrievesHistogram() {
        final var metricName = name("histogram");
        final var histogram = registry.registerHistogram(metricName);
        assertThat(registry.getHistogram(metricName)).isSameAs(histogram);
    }

    @Test
    @DisplayName("a gauge registered from a supplier exposes the supplied value")
    void registersGaugeFromSupplier() {
        final var metricName = name("gauge");
        registry.registerGauge(metricName, () -> "gaugeValue");
        assertThat(registry.getGauge(metricName).getValue()).isEqualTo("gaugeValue");
    }

    @Test
    @DisplayName("a gauge registered from a double supplier exposes the supplied value")
    void registersGaugeFromDoubleSupplier() {
        final var metricName = name("doubleGauge");
        registry.registerGauge(metricName, () -> 3.14d);
        assertThat(registry.getGauge(metricName).getValue()).isEqualTo(3.14d);
    }

    @Test
    @DisplayName("registering the same metric twice fails")
    void registeringSameMetricTwiceFails() {
        final var metricName = name("dup");
        registry.registerCounter(metricName);
        assertThatThrownBy(() -> registry.registerCounter(metricName))
                .isInstanceOf(MetricRegistrationException.class)
                .hasMessageContaining("already registered");
    }

    @Test
    @DisplayName("getting an unknown metric returns null")
    void getReturnsNullForUnknownMetric() {
        assertThat(registry.getCounter(name("missing"))).isNull();
    }

    @Test
    @DisplayName("getting a metric with the wrong type fails")
    void getWithWrongTypeFails() {
        final var metricName = name("typed");
        registry.registerCounter(metricName);
        assertThatThrownBy(() -> registry.getMeter(metricName))
                .isInstanceOf(MetricRegistrationException.class)
                .hasMessageContaining("registered as");
    }

    @Test
    @DisplayName("remove deletes a single metric")
    void removeDeletesMetric() {
        final var metricName = name("removable");
        registry.registerCounter(metricName);
        registry.remove(metricName);
        assertThat(registry.getCounter(metricName)).isNull();
    }

    @Test
    @DisplayName("removeAll clears every registered metric")
    void removeAllClearsMetrics() {
        registry.registerCounter(name("a"));
        registry.registerMeter(name("b"));
        registry.removeAll();
        assertThat(registry.getCounter(name("a"))).isNull();
        assertThat(registry.getMeter(name("b"))).isNull();
    }

    @Test
    @DisplayName("enabling and disabling JMX does not throw")
    void enableAndDisableJmx() {
        assertThatCode(() -> {
            registry.enableJmx("io.axual.ksml.test", List.of(new MetricTag("app", "test")));
            registry.disableJmx();
        }).doesNotThrowAnyException();
    }
}
