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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.value.Pair;
import io.axual.ksml.metric.MetricName;
import io.axual.ksml.data.tag.ContextTag;
import io.axual.ksml.metric.MetricsRegistry;

import static io.axual.ksml.metric.MetricsUtil.metricTag;

/**
 * Bridge class to create metrics from a supplied context.
 * <br/>
 * All metrics will have the name <i>user-defined-metrics</i> and a tag <i>custom-name</i> to identify the metric.
 * <br/>
 * All metrics registered using this bridge will be removed when closing the bridge
 */
public class MetricsBridge implements Closeable {
    private final MetricsRegistry metricRegistry;
    private final List<MetricBridge<?>> registeredBridges = new ArrayList<>();

    public MetricsBridge(MetricsRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void close() throws IOException {
        registeredBridges.forEach(metricBridge -> metricRegistry.remove(metricBridge.name));
        registeredBridges.clear();
    }

    /**
     * Instantiates a timer metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link TimerBridge} instance that is to be used to update the timer metric
     */
    public TimerBridge timer(String name, ContextTags tags) {
        var metricName = createMetricName(name, tags);
        var timer = metricRegistry.getTimer(metricName);
        if (timer == null) {
            timer = metricRegistry.registerTimer(metricName);
            return registerForClose(new TimerBridge(metricName, timer));
        }

        // Do not register for close, the original creator already does that
        return new TimerBridge(metricName, timer);
    }

    /**
     * Instantiates a counter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link CounterBridge} instance that is to be used to update the timer metric
     */
    public CounterBridge counter(String name, ContextTags tags) {
        var metricName = new MetricName(name, tags);
        var counter = metricRegistry.getCounter(metricName);
        if (counter == null) {
            counter = metricRegistry.registerCounter(metricName);
            return registerForClose(new CounterBridge(metricName, counter));
        }

        // Do not register for close, the original creator already does that
        return new CounterBridge(metricName, counter);
    }

    /**
     * Instantiates a meter metric and wraps it in a proxy class to be used in the context.
     *
     * @param name the name of the new metric, this will be used as the tag <i>custom-name</i>
     * @param tags Additional key value pairs to used to identify the metric
     * @return The {@link MeterBridge} instance that is to be used to update the timer metric
     */
    public MeterBridge meter(String name, ContextTags tags) {
        var metricName = createMetricName(name, tags);
        var meter = metricRegistry.getMeter(metricName);
        if (meter == null) {
            meter = metricRegistry.registerMeter(metricName);
            return registerForClose(new MeterBridge(metricName, meter));
        }

        // Do not register for close, the original creator already does that
        return new MeterBridge(metricName, meter);
    }


    private MetricName createMetricName(String name, ContextTags tags) {
        return new MetricName("user-defined-metrics", createMetricTags(name, tags));
    }

    private ContextTags createMetricTags(String name, ContextTags tags) {
        return tags.append("custom-name", name);
    }

    private <K extends Metric, R extends MetricBridge<K>> R registerForClose(R bridge) {
        registeredBridges.add(bridge);
        return bridge;
    }


    /**
     * Base class for the metric proxy
     */
    public static abstract class MetricBridge<M extends Metric> {
        protected final MetricName name;
        protected final M metric;

        private MetricBridge(MetricName name, M metric) {
            this.name = name;
            this.metric = metric;
        }

    }

    /**
     * Metric proxy for a timer metric
     */
    public static class TimerBridge extends MetricBridge<Timer> {
        private TimerBridge(MetricName name, Timer metric) {
            super(name, metric);
        }

        /**
         * Update the timer metric with a value of seconds
         *
         * @param valueSeconds the amount of time that the metric needs to be updated with
         */
        public void updateSeconds(long valueSeconds) {
            update(Duration.ofSeconds(valueSeconds));
        }

        /**
         * Update the timer metric with a value of milliseconds
         *
         * @param valueMilliseconds the amount of time that the metric needs to be updated with
         */
        public void updateMillis(long valueMilliseconds) {
            update(Duration.ofMillis(valueMilliseconds));
        }

        /**
         * Update the timer metric with a value of nanoseconds
         *
         * @param valueNanoseconds the amount of time that the metric needs to be updated with
         */
        public void updateNanos(long valueNanoseconds) {
            update(Duration.ofNanos(valueNanoseconds));
        }

        private void update(Duration duration) {
            metric.update(duration);
        }
    }

    /**
     * Metric proxy for a counter metric
     */
    public static class CounterBridge extends MetricBridge<Counter> {
        private CounterBridge(MetricName name, Counter metric) {
            super(name, metric);
        }

        /**
         * Update the metric that the count needs to be incremented with 1
         */
        public void increment() {
            increment(1);
        }

        /**
         * Update the metric that the count needs to be incremented by a specific value
         *
         * @param delta the value with which to increment the counter
         */
        public void increment(long delta) {
            metric.inc(delta);
        }
    }

    /**
     * Metric proxy for a meter metric
     */
    public static class MeterBridge extends MetricBridge<Meter> {
        private MeterBridge(MetricName name, Meter metric) {
            super(name, metric);
        }

        /**
         * Update the metric that a single event occurred
         */
        public void mark() {
            mark(1);
        }

        /**
         * Update the metric that a number of events have occurred
         *
         * @param nrOfEvents the number of events that have occurred
         */
        public void mark(long nrOfEvents) {
            metric.mark(nrOfEvents);
        }
    }
}
