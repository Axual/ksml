package io.axual.ksml.definition.parser;

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

import io.axual.ksml.data.parser.MapParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.PIPELINES;
import static io.axual.ksml.dsl.KSMLDSL.PRODUCERS;

public class TopologyDefinitionParser extends DefinitionParser<TopologyDefinition> {
    private final TopologyResourcesParser resourcesParser;

    public TopologyDefinitionParser(String namespace) {
        resourcesParser = new TopologyResourcesParser(namespace);
    }

    @Override
    public StructParser<TopologyDefinition> parser() {
        final var dummyResources = new TopologyResources("dummy");
        final var pipelinesParser = optional(mapField(PIPELINES, "pipeline", "pipeline", "Collection of named pipelines", new PipelineDefinitionParser(dummyResources)));
        final var producersParser = optional(mapField(PRODUCERS, "producer", "producer", "Collection of named producers", new ProducerDefinitionParser(dummyResources)));

        final var fields = resourcesParser.fields();
        fields.addAll(pipelinesParser.fields());
        fields.addAll(producersParser.fields());
        final var schema = structSchema(TopologyDefinition.class, "KSML definition", fields);

        return new StructParser<>() {
            @Override
            public TopologyDefinition parse(ParseNode node) {
                final var resources = resourcesParser.parse(node);
                final var result = new TopologyDefinition(resources.namespace());
                // Copy the resources into the topology definition
                resources.topics().forEach(result::register);
                resources.stateStores().forEach(result::register);
                resources.functions().forEach(result::register);
                // Parse all defined pipelines, using this topology's name as operation prefix
                new MapParser<>("pipeline", "pipeline definition", new PipelineDefinitionParser(resources)).parse(node.get(PIPELINES)).forEach(result::register);
                // Parse all defined producers, using this topology's name as operation prefix
                new MapParser<>("producer", "producer definition", new ProducerDefinitionParser(resources)).parse(node.get(PRODUCERS)).forEach(result::register);
                return result;
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
