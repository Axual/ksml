package io.axual.ksml.python;

import com.codahale.metrics.Timer;
import io.axual.ksml.metric.MetricName;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Metric proxy for a timer metric
 */
public class TimerBridge extends MetricBridge<Timer> {
    public TimerBridge(MetricName name, Timer metric, Consumer<MetricBridge<Timer>> onCloseCallback) {
        super(name, metric, onCloseCallback);
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
