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

import com.codahale.metrics.Timer;
import io.axual.ksml.metric.MetricName;
import org.graalvm.polyglot.HostAccess;

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
    @HostAccess.Export
    public void updateSeconds(long valueSeconds) {
        update(Duration.ofSeconds(valueSeconds));
    }

    /**
     * Update the timer metric with a value of milliseconds
     *
     * @param valueMilliseconds the amount of time that the metric needs to be updated with
     */
    @HostAccess.Export
    public void updateMillis(long valueMilliseconds) {
        update(Duration.ofMillis(valueMilliseconds));
    }

    /**
     * Update the timer metric with a value of nanoseconds
     *
     * @param valueNanoseconds the amount of time that the metric needs to be updated with
     */
    @HostAccess.Export
    public void updateNanos(long valueNanoseconds) {
        update(Duration.ofNanos(valueNanoseconds));
    }

    private void update(Duration duration) {
        metric.update(duration);
    }
}
