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

import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TopologyResourcesParser extends BaseParser<TopologyResources> {
    @Override
    public TopologyResources parse(YamlNode node) {
        // If there is nothing to parse, return immediately
        if (node == null) return new TopologyResources("empty");

        // Set up an index for the topology resources
        final var result = new TopologyResources(node.getName());

        // Parse all defined streams
        new MapParser<>("stream definition", new StreamDefinitionParser()).parse(node.get(STREAMS_DEFINITION)).forEach(result::register);
        new MapParser<>("table definition", new TableDefinitionParser()).parse(node.get(TABLES_DEFINITION)).forEach(result::register);
        new MapParser<>("globalTable definition", new GlobalTableDefinitionParser()).parse(node.get(GLOBALTABLES_DEFINITION)).forEach(result::register);

        // Parse all defined state stores
        new MapParser<>("state store definition", new StateStoreDefinitionParser()).parse(node.get(STORES_DEFINITION)).forEach(result::register);

        // Parse all defined functions
        new MapParser<>("function definition", new TypedFunctionDefinitionParser()).parse(node.get(FUNCTIONS_DEFINITION)).forEach(result::register);

        return result;
    }
}
