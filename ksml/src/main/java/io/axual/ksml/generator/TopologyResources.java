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
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.metric.MetricTag;
import io.axual.ksml.metric.MetricTags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopologyResources extends TopologyBaseResources {
    // Where an "as" name must be saved to be found, used in error messages
    public static final String SAVED_EARLIER = "in a pipeline defined before this one, in the same definition file";

    // All registered KStreams, KTables and KGlobalTables
    private final Map<String, TopicDefinition> topics = new HashMap<>();
    // All registered operation names
    private final Set<String> operationNames = new HashSet<>();
    // All names that parsed pipelines save their result under, with "as" or by their own name, and where
    private final Map<String, String> savedNames = new HashMap<>();

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

    // The section that defines the name, or null if it is not defined
    public String sectionOf(String name) {
        final var topic = topic(name);
        if (topic == null) return null;
        if (topic instanceof TableDefinition) return "tables";
        if (topic instanceof GlobalTableDefinition) return "globalTables";
        return "streams";
    }

    // The tags of the node that saves the result tell where it is saved
    public void registerSavedName(String name, MetricTags tags) {
        savedNames.put(name, pipelineOf(tags));
    }

    public boolean isSavedName(String name) {
        return savedNames.containsKey(name);
    }

    // Where the result under the name is saved, for example: pipeline "orders" (branch2), or null
    public String pipelineSaving(String name) {
        return savedNames.get(name);
    }

    // For example: pipeline "orders", or pipeline "orders" (branch2, branch1) for a nested branch
    public static String pipelineOf(MetricTags tags) {
        String pipeline = null;
        final var branches = new ArrayList<String>();
        for (final var tag : tags) {
            if (tag.key().equals("pipeline")) pipeline = tag.value();
            if (tag.key().equals("branch")) branches.add(tag.value());
        }
        if (pipeline == null) return "an earlier pipeline";
        return "pipeline \"" + pipeline + "\"" + (branches.isEmpty() ? "" : " (" + String.join(", ", branches) + ")");
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
