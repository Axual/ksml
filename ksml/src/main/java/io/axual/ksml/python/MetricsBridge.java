package io.axual.ksml.python;

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

import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.metric.MetricName;
import io.axual.ksml.metric.MetricsRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Bridge class to create metrics from a supplied context.
 * <br/>
 * All metrics will have the name <i>user-defined-metrics</i> and a tag <i>custom-name</i> to identify the metric.
 * <br/>
 * All metrics registered using this bridge will be removed when closing the bridge
 */
public class MetricsBridge {
    private final MetricsRegistry registry;
    private final Map<MetricName, CounterBridge> counters = new HashMap<>();
    private final Map<MetricName, MeterBridge> meters = new HashMap<>();
    private final Map<MetricName, TimerBridge> timers = new HashMap<>();

    public MetricsBridge(MetricsRegistry registry) {
        this.registry = registry;
    }

    /**
     * Instantiates a counter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link CounterBridge} instance that is to be used to update the timer metric
     */
    public CounterBridge counter(String name, Map<?, ?> tags) {
        final var metricName = createMetricName("counter", name, tags);
        if (counters.containsKey(metricName)) {
            return counters.get(metricName);
        }

        // Register a new counter
        final var counter = registry.registerCounter(metricName);
        final var result = new CounterBridge(metricName, counter, m -> removeMetric(m.name(), counters));
        counters.put(metricName, result);
        return result;
    }

    /**
     * Instantiates a counter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @return The {@link CounterBridge} instance that is to be used to update the timer metric
     */
    public CounterBridge counter(String name) {
        return counter(name, null);
    }

    /**
     * Instantiates a meter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link MeterBridge} instance that is to be used to update the timer metric
     */
    public MeterBridge meter(String name, Map<?, ?> tags) {
        final var metricName = createMetricName("meter", name, tags);
        if (meters.containsKey(metricName)) {
            return meters.get(metricName);
        }

        // Register a new counter
        final var meter = registry.registerMeter(metricName);
        final var result = new MeterBridge(metricName, meter, m -> removeMetric(m.name(), meters));
        meters.put(metricName, result);
        return result;
    }

    /**
     * Instantiates a meter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @return The {@link MeterBridge} instance that is to be used to update the timer metric
     */
    public MeterBridge meter(String name) {
        return meter(name, null);
    }

    /**
     * Instantiates a timer metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link TimerBridge} instance that is to be used to update the timer metric
     */
    public TimerBridge timer(String name, Map<?, ?> tags) {
        final var metricName = createMetricName("timer", name, tags);
        if (timers.containsKey(metricName)) {
            return timers.get(metricName);
        }

        // Register a new counter
        final var timer = registry.registerTimer(metricName);
        final var result = new TimerBridge(metricName, timer, m -> removeMetric(m.name(), timers));
        timers.put(metricName, result);
        return result;
    }

    /**
     * Instantiates a timer metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @return The {@link TimerBridge} instance that is to be used to update the timer metric
     */
    public TimerBridge timer(String name) {
        return timer(name, null);
    }

    private MetricName createMetricName(String type, String name, Map<?, ?> tagMap) {
        var tags = new ContextTags().append("custom-name", name);
        if (tagMap != null) {
            for (Map.Entry<?, ?> entry : tagMap.entrySet()) {
                tags = tags.append(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        return new MetricName("user-defined-" + type, tags);
    }

    private void removeMetric(MetricName metricName, Map<MetricName, ?> metricMap) {
        registry.remove(metricName);
        metricMap.remove(metricName);
    }
}
