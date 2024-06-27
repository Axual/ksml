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
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TopologyResourcesParser extends DefinitionParser<TopologyResources> {
    private final TopologyBaseResourcesParser baseResourcesParser;

    public TopologyResourcesParser(String namespace) {
        baseResourcesParser = new TopologyBaseResourcesParser(namespace);
    }

    @Override
    public StructsParser<TopologyResources> parser() {
        final var dummyResources = new TopologyBaseResources("dummy");
        final var streamsParser = optional(mapField(STREAMS, "stream", "stream definition", "Streams that can be referenced in producers and pipelines", new StreamDefinitionParser(dummyResources, true)));
        final var tablesParser = optional(mapField(TABLES, "table", "table definition", "Tables that can be referenced in producers and pipelines", new TableDefinitionParser(dummyResources, true)));
        final var globalTablesParser = optional(mapField(GLOBAL_TABLES, "globalTable", "globalTable definition", "GlobalTables that can be referenced in producers and pipelines", new GlobalTableDefinitionParser(dummyResources, true)));

        final var fields = baseResourcesParser.schemas().getFirst().fields();
        fields.addAll(streamsParser.schemas().getFirst().fields());
        fields.addAll(tablesParser.schemas().getFirst().fields());
        fields.addAll(globalTablesParser.schemas().getFirst().fields());
        final var schemas = List.of(structSchema(TopologyResources.class, "KSML definition resources", fields));

        return new StructsParser<>() {
            @Override
            public TopologyResources parse(ParseNode node) {
                final var resources = baseResourcesParser.parse(node);
                final var result = new TopologyResources(resources.namespace());
                // Copy the resources into the topology definition
                resources.stateStores().forEach(result::register);
                resources.functions().forEach(result::register);
                // Parse all defined streams, using this topology's name as operation prefix
                new MapParser<>("stream", "stream definition", new StreamDefinitionParser(resources, true)).parse(node.get(STREAMS)).forEach(result::register);
                // Parse all defined tables, using this topology's name as operation prefix
                new MapParser<>("table", "table definition", new TableDefinitionParser(resources, true)).parse(node.get(TABLES)).forEach(result::register);
                // Parse all defined global tables, using this topology's name as operation prefix
                new MapParser<>("globalTable", "globalTable definition", new GlobalTableDefinitionParser(resources, true)).parse(node.get(GLOBAL_TABLES)).forEach(result::register);
                return result;
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }
}
