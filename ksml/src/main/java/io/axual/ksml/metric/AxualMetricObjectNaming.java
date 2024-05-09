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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static io.axual.ksml.metric.AxualMetricsUtil.metricName;
import static io.axual.ksml.metric.AxualMetricsUtil.metricTag;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class AxualMetricObjectNaming {
    public static final String NAME_TAG_KEY = "name";
    private static final String TAG_SEPARATOR = "#";
    private static final String KEY_VALUE_SEPARATOR = "=";

    static String stringFromMetricName(AxualMetricName metricName) {
        return stringFromNameAndMetricsTags(metricName.name(), metricName.axualMetricTags());
    }

    static String stringFromNameAndMetricsTags(String name, List<AxualMetricTag> tags) {
        final var tagsWithName = new ArrayList<AxualMetricTag>(tags.size() + 1);
        tagsWithName.add(metricTag(NAME_TAG_KEY, name));
        tagsWithName.addAll(tags);
        return stringFromMetricsTags(tagsWithName);
    }

    static String stringFromMetricsTags(List<AxualMetricTag> tags) {
        final var keys = new HashSet<String>();
        var builder = new StringBuilder();

        for (var tag : tags) {
            if (!keys.add(tag.key())) {
                throw new AxualMetricObjectNamingException("Same tag key exists %s".formatted(tag.key()));
            }

            if(!builder.isEmpty()){
                builder.append(TAG_SEPARATOR);
            }
            builder.append(tag.key())
                    .append(KEY_VALUE_SEPARATOR)
                    .append(tag.value());
        }
        return builder.toString();
    }

    static List<AxualMetricTag> metricTagsFromString(String name) {
        var keyValues = StringUtils.splitByWholeSeparator(name, TAG_SEPARATOR);
        var tags = new ArrayList<AxualMetricTag>();
        for (var kvString : keyValues) {
            if (kvString == null || kvString.isBlank()) {
                continue;
            }
            var keyValue = StringUtils.splitByWholeSeparator(kvString, KEY_VALUE_SEPARATOR, 2);
            if (keyValue.length != 2) {
                throw new AxualMetricObjectNamingException("KeyValue String doesn't contain separator. Failing part %s  of name %s".formatted(kvString, name));
            }
            tags.add(metricTag(keyValue[0], keyValue[1]));
        }

        return tags;
    }

    static AxualMetricName metricNameFromString(String name) {
        var tags = metricTagsFromString(name);
        // Find the name tag
        var nameTag = tags.stream()
                .filter(tag -> NAME_TAG_KEY.equals(tag.key()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Provided String does not contain name tag"));
        tags.remove(nameTag);
        return metricName(nameTag.value(), tags);
    }
}