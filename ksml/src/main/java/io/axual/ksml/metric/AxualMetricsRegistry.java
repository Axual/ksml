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

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The MetricsRegistry is a simple registry for metrics. The metrics created in an instance can be exposed as JMX MBeans.
 * <p>
 * Unique names for a metric are generated from the provided {@link AxualMetricName}. There can be no duplicate metrics using the same name.
 * <p>
 * The JMX Domain is provided when the JMX Exposure is started, and additional {@link AxualMetricTag} can be provided which will be added to the JMX MBean name.
 * <p>
 * The MBean name will be <i>{@literal <domain>:type=<metricType>,name=<metricName.name>,[any additional tags],[metricName.tags] }</i>
 */
public class AxualMetricsRegistry {
    private final MetricRegistry metricRegistry;
    private JmxReporter jmxReporter;
    private final ConsoleReporter reporter;

    private final Map<AxualMetricName, Metric> registeredMetrics = new HashMap<>();

    public AxualMetricsRegistry() {
        this(new MetricRegistry());
    }

    // For testing
    AxualMetricsRegistry(MetricRegistry registry) {
        this.metricRegistry = registry;
        reporter = ConsoleReporter.forRegistry(registry).outputTo(System.err).build();
        reporter.start(5, TimeUnit.SECONDS);
    }

    /**
     * Register or get a new counter with the provided name.
     *
     * @param metricName the name for the counter
     * @return the counter registered to the provided name
     */
    public Counter registerCounter(AxualMetricName metricName) {
        return register(metricName, () -> metricRegistry.counter(encodeName(metricName)));
    }

    /**
     * Get the counter which was registered with the provided name.
     *
     * @param metricName the name for the counter
     * @return the counter registered to the provided name, or null
     * @throws MetricRegistrationException if the metric name was used for another metric type
     */
    public Counter getCounter(AxualMetricName metricName) {
        return get(metricName, Counter.class);
    }

    /**
     * Register or get a new meter with the provided name.
     *
     * @param metricName the name for the meter
     * @return the meter registered to the provided name
     */
    public Meter registerMeter(AxualMetricName metricName) {
        return register(metricName, () -> metricRegistry.meter(encodeName(metricName)));
    }

    /**
     * Get the meter which was registered with the provided name.
     *
     * @param metricName the name for the meter
     * @return the meter registered to the provided name, or null
     * @throws MetricRegistrationException if the metric name was used for another metric type
     */
    public Meter getMeter(AxualMetricName metricName) {
        return get(metricName, Meter.class);
    }

    /**
     * Register or get a new gauge with the provided name.
     *
     * @param metricName    the name for the gauge
     * @param valueSupplier the supplied to use to get the metric value
     * @return the gauge registered to the provided name
     */
    public <T> Gauge<T> registerGauge(AxualMetricName metricName, Supplier<T> valueSupplier) {
        final Gauge<T> metricSupplier = valueSupplier::get;
        return register(metricName, () -> metricRegistry.gauge(encodeName(metricName), () -> metricSupplier));
    }

    /**
     * Get the gauge which was registered with the provided name.
     *
     * @param metricName the name for the gauge
     * @return the gauge registered to the provided name, or null
     * @throws MetricRegistrationException if the metric name was used for another metric type
     */
    @SuppressWarnings("unchecked")
    public <T> Gauge<T> getGauge(AxualMetricName metricName) {
        return get(metricName, Gauge.class);
    }

    /**
     * Register or get a new histogram with the provided name.
     *
     * @param metricName the name for the histogram
     * @return the histogram registered to the provided name
     */
    public Histogram registerHistogram(AxualMetricName metricName) {
        return register(metricName, () -> metricRegistry.histogram(encodeName(metricName)));
    }

    /**
     * Get the histogram which was registered with the provided name.
     *
     * @param metricName the name for the histogram
     * @return the histogram registered to the provided name, or null
     * @throws MetricRegistrationException if the metric name was used for another metric type
     */
    public Histogram getHistogram(AxualMetricName metricName) {
        return get(metricName, Histogram.class);
    }

    /**
     * Register or get a new timer with the provided name.
     *
     * @param metricName the name for the timer
     * @return the timer registered to the provided name
     */
    public Timer registerTimer(AxualMetricName metricName) {
        return register(metricName, () -> metricRegistry.timer(encodeName(metricName)));
    }

    /**
     * Get the timer which was registered with the provided name.
     *
     * @param metricName the name for the timer
     * @return the timer registered to the provided name, or null
     * @throws MetricRegistrationException if the metric name was used for another metric type
     */
    public Timer getTimer(AxualMetricName metricName) {
        return get(metricName, Timer.class);
    }

    /**
     * Remove a metric with the provided name
     *
     * @param metricName the name of the metric to be removed
     * @return true if the metric was removed
     */
    public synchronized boolean remove(AxualMetricName metricName) {
        registeredMetrics.remove(metricName);
        return metricRegistry.remove(encodeName(metricName));
    }

    private String encodeName(AxualMetricName metricName) {
        return AxualMetricObjectNaming.stringFromMetricName(metricName);
    }


    /**
     * Expose the metrics as JMX MBeans, using the provided domain, and any additional tags that should be applied to the object name.
     * The MBean name will be <i>{@literal <domain>:type=<metricType>,name=<metricName.name>,[standardTags],[metricName.tags] }</i>
     *
     * @param domain     the JMX domain where the metrics are exposed
     * @param commonTags These tags are added to every MBean to allow unique names for JMX. For example the name and task id for a connector task.
     */
    public void enableJmx(String domain, List<AxualMetricTag> commonTags) {
        disableJmx();

        jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain(domain)
                .createsObjectNamesWith(new AxualMetricObjectNameFactory(commonTags))
                .build();
        jmxReporter.start();
    }

    /**
     * Stop exposing the metrics as JMX Mbeans.
     */
    public void disableJmx() {
        if (jmxReporter != null) {
            jmxReporter.stop();
            jmxReporter.close();
            jmxReporter = null;
        }
    }

    private synchronized <M extends Metric> M register(AxualMetricName metricName, Supplier<M> metricSupplier) {
        if (registeredMetrics.containsKey(metricName)) {
            throw new MetricRegistrationException("Metric %s is already registered".formatted(metricName));
        }
        M newInstance = metricSupplier.get();
        registeredMetrics.put(metricName, newInstance);
        return newInstance;
    }

    private synchronized <M extends Metric> M get(AxualMetricName metricName, Class<M> clazz) {
        var metric = registeredMetrics.get(metricName);
        if (metric == null) {
            return null;
        }
        if (clazz.isInstance(metric)) {
            return clazz.cast(metric);
        }

        throw new MetricRegistrationException("Metric %s is registered as a %s".formatted(metricName, metric.getClass()));
    }
}
