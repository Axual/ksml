package io.axual.ksml.parser;

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


import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.metric.MetricTags;

import java.util.List;
import java.util.function.BiFunction;

public abstract class TopologyBaseResourceAwareParser<T> extends DefinitionParser<T> {
    // The set of functions and stores that streams, producers and pipelines can reference
    private final TopologyBaseResources resources;

    protected TopologyBaseResourceAwareParser(TopologyBaseResources resources) {
        this.resources = resources;
    }

    protected TopologyBaseResources resources() {
        if (resources != null) return resources;
        throw new TopologyException("Topology base resources not properly initialized. This is a programming error.");
    }

    protected <F extends FunctionDefinition> StructsParser<FunctionDefinition> functionField(String childName, String doc, StructsParser<F> parser) {
        final var resourceParser = new TopologyResourceParser<>("function", childName, doc, (name, tags) -> resources.function(name), parser);
        return StructsParser.of(resourceParser::parseDefinition, resourceParser.schemas());
    }

    protected <S> StructsParser<S> lookupField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<? extends S> parser) {
        final var resourceParser = new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser);
        final var schemas = resourceParser.schemas();
        return new StructsParser<>() {
            @Override
            public S parse(ParseNode node) {
                if (node == null) return null;
                final var resource = resourceParser.parse(node);
                return (resource != null) ? resource.definition() : null;
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }

    protected <S> StructsParser<TopologyResource<S>> topologyResourceField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<S> parser) {
        return new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser, true);
    }
}
