package io.axual.ksml.python;

import com.codahale.metrics.Meter;
import io.axual.ksml.metric.MetricName;

import java.util.function.Consumer;

/**
 * Metric proxy for a meter metric
 */
public class MeterBridge extends MetricBridge<Meter> {
    public MeterBridge(MetricName name, Meter metric, Consumer<MetricBridge<Meter>> onCloseCallback) {
        super(name, metric, onCloseCallback);
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
