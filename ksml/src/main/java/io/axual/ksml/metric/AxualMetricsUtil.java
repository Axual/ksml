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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Collection of convenience methods for creating or handling Metric related classes and records
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AxualMetricsUtil {
    /**
     * Create a new Tag instance.
     *
     * @param key   the metric tag key to use
     * @param value the metric tag value to use
     * @return the new metric tags
     */
    public static AxualMetricTag metricTag(String key, String value) {
        return new AxualMetricTag(key, value);
    }

    /**
     * Create a mutable list Tags from the provided Strings. The number of String values must be a multiple of two
     *
     * @param keyValues The key value pairs to transform into a list. If no arguments are provided an empty list is returned
     * @throws IllegalArgumentException if the keyValues are not a multiple of two
     */
    public static List<AxualMetricTag> metricTags(String... keyValues) {
        if (keyValues == null) {
            return new ArrayList<>();
        }
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Size of keyValues must be a multiple of two");
        }
        var tagList = new ArrayList<AxualMetricTag>();
        for (int pos = 0; pos < keyValues.length; pos += 2) {
            tagList.add(new AxualMetricTag(keyValues[pos], keyValues[pos + 1]));
        }
        return tagList;
    }

    /**
     * Create a new metric name fom the provided name and tags.
     *
     * @param name            the name of the metric to create
     * @param axualMetricTags the metric tags to identify a unique instance for the given name
     */
    public static AxualMetricName metricName(String name, List<AxualMetricTag> axualMetricTags) {
        return new AxualMetricName(name, axualMetricTags);
    }

    /**
     * Create a new metric name fom the provided name and tags.
     *
     * @param name            the name of the metric to create
     * @param axualMetricTags the metric tags to identify a unique instance for the given name
     */
    public static AxualMetricName metricName(String name, AxualMetricTag... axualMetricTags) {
        return new AxualMetricName(name, axualMetricTags == null ? Collections.emptyList() : Arrays.asList(axualMetricTags));
    }

    /**
     * Create a new metric name fom the provided name.
     *
     * @param name the name of the metric to create
     */
    public static AxualMetricName metricName(String name) {
        return new AxualMetricName(name, List.of());
    }
}
