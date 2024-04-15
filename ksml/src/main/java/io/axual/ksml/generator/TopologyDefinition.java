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
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.exception.TopologyException;

import java.util.LinkedHashMap;
import java.util.Map;

public class TopologyDefinition extends TopologyResources {
    // All registered pipelines in order of insertion
    private final Map<String, PipelineDefinition> pipelines = new LinkedHashMap<>();
    // All registered producers in order of insertion
    private final Map<String, ProducerDefinition> producers = new LinkedHashMap<>();

    public void register(String name, PipelineDefinition pipelineDefinition) {
        if (pipelines.containsKey(name)) {
            throw new TopologyException("Pipeline definition must be unique: " + name);
        }
        pipelines.put(name, pipelineDefinition);
    }

    public TopologyDefinition(String namespace) {
        super(namespace);
    }

    public PipelineDefinition pipeline(String name) {
        return pipelines.get(name);
    }

    public Map<String, PipelineDefinition> pipelines() {
        return ImmutableMap.copyOf(pipelines);
    }

    public void register(String name, ProducerDefinition producerDefinition) {
        if (pipelines.containsKey(name)) {
            throw new TopologyException("Pipeline definition must be unique: " + name);
        }
        producers.put(name, producerDefinition);
    }

    public ProducerDefinition producer(String name) {
        return producers.get(name);
    }

    public Map<String, ProducerDefinition> producers() {
        return ImmutableMap.copyOf(producers);
    }
}
