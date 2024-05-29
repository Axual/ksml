package io.axual.ksml.python;

import com.codahale.metrics.Metric;
import io.axual.ksml.metric.MetricName;
import lombok.Getter;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class for the metric proxy
 */
public abstract class MetricBridge<M extends Metric> {
    @Getter
    private final MetricName name;
    protected final M metric;
    private final Consumer<MetricBridge<M>> onCloseCallback;

    protected MetricBridge(MetricName name, M metric, Consumer<MetricBridge<M>> onCloseCallback) {
        this.name = name;
        this.metric = metric;
        this.onCloseCallback = onCloseCallback;
    }

    public void close() {
        if (onCloseCallback != null) onCloseCallback.accept(this);
    }
}
