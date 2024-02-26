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


import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public abstract class ContextAwareParser<T> extends DefinitionParser<T> {
    private static final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();
    // The set of streams, functions and stores that producers and pipelines can reference
    private final TopologyResources resources;

    protected ContextAwareParser(TopologyResources resources) {
        super(resources != null ? resources.namespace() : null);
        this.resources = resources;
    }

    protected TopologyResources resources() {
        if (resources != null) return resources;
        throw new TopologyException("Topology resources not properly initialized. This is a programming error.");
    }

    protected String determineName(String name) {
        final var basename = namespace() + "_" + name;
        return String.format("%s_%03d", basename, typeInstanceCounters.computeIfAbsent(basename, t -> new AtomicInteger(1)).getAndIncrement());
    }

    protected <F extends FunctionDefinition> StructParser<FunctionDefinition> functionField(String childName, boolean mandatory, String doc, DefinitionParser<F> parser) {
        final var resourceParser = new TopologyResourceParser<>("function", childName, doc, resources::function, parser);
        final var schema = mandatory ? resourceParser.schema() : optional(resourceParser).schema();
        return StructParser.of(resourceParser::parseDefinition, schema);
    }

    protected <F extends FunctionDefinition> StructParser<FunctionDefinition> functionField(String childName, String doc, DefinitionParser<F> parser) {
        return functionField(childName, true, doc, parser);
    }

    protected <S> StructParser<S> lookupField(String resourceType, String childName, boolean mandatory, String doc, Function<String, S> lookup, DefinitionParser<? extends S> parser) {
        final var resourceParser = new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser);
        final var schema = mandatory ? resourceParser.schema() : optional(resourceParser).schema();
        return new StructParser<>() {
            @Override
            public S parse(ParseNode node) {
                if (node == null) return null;
                final var resource = resourceParser.parse(node);
                if (resource != null && (resource.definition() != null || !mandatory)) return resource.definition();
                throw new ParseException(node, resourceType + " not defined");
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }

    protected <S> StructParser<TopologyResource<S>> topologyResourceField(String resourceType, String childName, String doc, Function<String, S> lookup, DefinitionParser<S> parser) {
        final var resourceParser = new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser, true);
        return StructParser.of(resourceParser::parse, resourceParser.schema());
    }

    public StructParser<TopicDefinition> topicField(String childName, boolean mandatory, String doc, DefinitionParser<? extends TopicDefinition> parser) {
        return lookupField("topic", childName, mandatory, doc, resources::topic, parser);
    }
}
