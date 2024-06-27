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

import com.codahale.metrics.Metric;
import io.axual.ksml.metric.MetricName;
import lombok.Getter;

import java.util.function.Consumer;

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
