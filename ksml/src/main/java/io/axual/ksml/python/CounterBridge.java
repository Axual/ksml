package io.axual.ksml.python;

import com.codahale.metrics.Counter;
import io.axual.ksml.metric.MetricName;

import java.util.function.Consumer;

/**
 * Metric proxy for a counter metric
 */
public class CounterBridge extends MetricBridge<Counter> {
    public CounterBridge(MetricName name, Counter metric, Consumer<MetricBridge<Counter>> onCloseCallback) {
        super(name, metric, onCloseCallback);
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
