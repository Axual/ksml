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

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.PIPELINES;
import static io.axual.ksml.dsl.KSMLDSL.PRODUCERS;

public class TopologyDefinitionParser extends DefinitionParser<TopologyDefinition> {
    private static final String PIPELINE = "pipeline";
    private static final String PRODUCER = "producer";
    private final TopologyResourcesParser resourcesParser;

    public TopologyDefinitionParser(String namespace) {
        resourcesParser = new TopologyResourcesParser(namespace);
    }

    @Override
    public StructsParser<TopologyDefinition> parser() {
        final var dummyResources = new TopologyResources("dummy");
        final var nameParser = optional(stringField(KSMLDSL.NAME, false, null, "The name of this KSML definition"));
        final var versionParser = optional(stringField(KSMLDSL.VERSION, false, null, "The version of this KSML definition"));
        final var descriptionParser = optional(stringField(KSMLDSL.DESCRIPTION, false, null, "The description of this KSML definition"));
        final var pipelinesParser = optional(mapField(PIPELINES, PIPELINE, PIPELINE, "Collection of named pipelines", new PipelineDefinitionParser(dummyResources)));
        final var producersParser = optional(mapField(PRODUCERS, PRODUCER, PRODUCER, "Collection of named producers", new ProducerDefinitionParser(dummyResources)));

        final var fields = resourcesParser.schemas().getFirst().fields();
        fields.addAll(nameParser.schemas().getFirst().fields());
        fields.addAll(versionParser.schemas().getFirst().fields());
        fields.addAll(descriptionParser.schemas().getFirst().fields());
        fields.addAll(pipelinesParser.schemas().getFirst().fields());
        fields.addAll(producersParser.schemas().getFirst().fields());
        final var schemas = List.of(structSchema(TopologyDefinition.class, "KSML definition", fields));

        return new StructsParser<>() {
            @Override
            public TopologyDefinition parse(ParseNode node) {
                final var name = nameParser.parse(node);
                final var version = versionParser.parse(node);
                final var description = descriptionParser.parse(node);
                final var resources = resourcesParser.parse(node);
                final var result = new TopologyDefinition(resources.namespace(), name, version, description);
                // Copy the resources into the topology definition
                resources.topics().forEach(result::register);
                resources.stateStores().forEach(result::register);
                resources.functions().forEach(result::register);
                // Parse all defined pipelines, using this topology's name as the operation prefix
                new MapParser<>(PIPELINE, "pipeline definition", new PipelineDefinitionParser(resources)).parse(node.get(PIPELINES)).forEach(result::register);
                // Parse all defined producers, using this topology's name as the operation prefix
                new MapParser<>(PRODUCER, "producer definition", new ProducerDefinitionParser(resources)).parse(node.get(PRODUCERS)).forEach(result::register);
                return result;
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }
}
