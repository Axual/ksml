package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.metric.MetricTags;

import java.util.function.BiFunction;

// Field-builder helpers available once the full resource set is known, i.e. once all
// streams/tables/topics have been parsed and assembled into a TopologyResources (which is a
// TopologyBaseResources). This is the phase pipelines and operations run in, so topics can be
// referenced by name (topicField) in addition to the base-phase capabilities (delegated to a
// composed TopologyBaseResourceFields, so the base-phase logic isn't duplicated).
public class TopologyResourceFields {
    private final TopologyBaseResourceFields base;
    private final TopologyResources resources;

    public TopologyResourceFields(TopologyResources resources) {
        this.base = new TopologyBaseResourceFields(resources);
        this.resources = resources;
    }

    public TopologyResources resources() {
        if (resources != null) return resources;
        throw new TopologyException("Topology resources not properly initialized. This is a programming error.");
    }

    public <F extends FunctionDefinition> StructsParser<FunctionDefinition> functionField(String childName, String doc, StructsParser<F> parser) {
        return base.functionField(childName, doc, parser);
    }

    public <S> StructsParser<S> lookupField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<? extends S> parser) {
        return base.lookupField(resourceType, childName, doc, lookup, parser);
    }

    public <S> StructsParser<TopologyResource<S>> topologyResourceField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<S> parser) {
        return base.topologyResourceField(resourceType, childName, doc, lookup, parser);
    }

    public StructsParser<TopicDefinition> topicField(String childName, String doc, DefinitionParser<? extends TopicDefinition> parser) {
        return lookupField("topic", childName, doc, (name, context) -> resources.topic(name), parser);
    }
}
