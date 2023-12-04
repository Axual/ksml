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

import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.PIPELINES_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.PRODUCERS_DEFINITION;

public class TopologyDefinitionParser extends BaseParser<TopologyDefinition> {
    private final String name;

    public TopologyDefinitionParser(String name) {
        this.name = name;
    }

    @Override
    public TopologyDefinition parse(YamlNode node) {
        // If there is nothing to parse, return immediately
        if (node == null) return null;

        // Parse the underlying resources first
        final var resources = new TopologyResourcesParser().parse(node);

        // Set up an index for the topology specification
        final var result = new TopologyDefinition(resources);

        // Parse all defined pipelines, using this topology's name as operation prefix
        new MapParser<>("pipeline definition", new PipelineDefinitionParser(name, resources)).parse(node.get(PIPELINES_DEFINITION)).forEach(result::register);
        // Parse all defined producers, using this topology's name as operation prefix
        new MapParser<>("producer definition", new ProducerDefinitionParser(name, resources)).parse(node.get(PRODUCERS_DEFINITION)).forEach(result::register);

        return result;
    }
}
