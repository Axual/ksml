package io.axual.ksml.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.metric.MetricTag;
import io.axual.ksml.metric.MetricTags;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopologyResources extends TopologyBaseResources {
    // All registered KStreams, KTables and KGlobalTables
    private final Map<String, TopicDefinition> topics = new HashMap<>();
    // All registered operation names
    private final Set<String> operationNames = new HashSet<>();

    public TopologyResources(String namespace) {
        super(namespace);
    }

    public void register(String name, TopicDefinition def) {
        if (topics.containsKey(name)) {
            throw new TopologyException("Topic definition must be unique: " + name);
        }
        topics.put(name, def);
    }

    public TopicDefinition topic(String name) {
        return topics.get(name);
    }

    public Map<String, TopicDefinition> topics() {
        return ImmutableMap.copyOf(topics);
    }

    public String getUniqueOperationName(MetricTags tags) {
        final var basename = String.join("_", tags.stream().map(MetricTag::value).toArray(String[]::new));
        return getUniqueOperationNameInternal(basename);
    }

    public String getUniqueOperationName(String name) {
        return getUniqueOperationNameInternal(namespace() + "_" + name);
    }

    private String getUniqueOperationNameInternal(final String basename) {
        if (!operationNames.contains(basename)) {
            operationNames.add(basename);
            return basename;
        }
        int index = 1;
        while (true) {
            var postfix = "" + index;
            // Pad with leading zeros unless the number gets bigger than 1000
            if (postfix.length() < 3) postfix = ("000" + postfix).substring(postfix.length());
            final var name = basename + "_" + postfix;
            if (!operationNames.contains(name)) {
                operationNames.add(name);
                return name;
            }
            index++;
        }
    }
}
