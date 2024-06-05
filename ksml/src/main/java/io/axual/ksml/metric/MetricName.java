package io.axual.ksml.metric;

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

import io.axual.ksml.data.tag.ContextTag;
import io.axual.ksml.data.tag.ContextTags;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * The MetricName identifies a single metric in the system. If a name is used multiple times,
 * one or more {@link ContextTag} can be added to identify the instance.
 * <p>
 * Example: A metric counting the number of produce calls for a topic partition would look like
 * <pre>
 *  {@code
 * var produceMetricName = new MetricName("produce-counter", List.of( new MetricTag("topic","my-topic"), new MetricTag("topic","my-topic")) );
 * }
 * </pre>
 */
@Slf4j
public record MetricName(String name, ContextTags tags) {
    public MetricName {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Metric name can not be null or empty");
        }
    }

    public MetricName(String name) {
        this(name, new ContextTags());
    }

    @Override
    public String toString() {
        return "name=" + name + (!tags.isEmpty() ? "," : "") + tags;
    }
}
