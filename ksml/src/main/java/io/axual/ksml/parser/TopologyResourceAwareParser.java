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


import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;

import java.util.function.BiFunction;

public abstract class TopologyResourceAwareParser<T> extends TopologyBaseResourceAwareParser<T> {
    // The set of streams, functions and stores that producers and pipelines can reference
    private final TopologyResources resources;

    protected TopologyResourceAwareParser(TopologyResources resources) {
        super(resources);
        this.resources = resources;
    }

    protected TopologyResources resources() {
        if (resources != null) return resources;
        throw new TopologyException("Topology resources not properly initialized. This is a programming error.");
    }

    public StructParser<TopicDefinition> topicField(String childName, String doc, DefinitionParser<? extends TopicDefinition> parser) {
        return lookupField("topic", childName, doc, (name, context) -> resources.topic(name), parser);
    }
}
