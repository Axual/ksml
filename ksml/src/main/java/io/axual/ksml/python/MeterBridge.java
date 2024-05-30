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
