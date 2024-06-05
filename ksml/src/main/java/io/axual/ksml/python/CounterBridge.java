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
