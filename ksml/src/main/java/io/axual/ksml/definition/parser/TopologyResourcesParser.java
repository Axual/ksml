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
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TopologyResourcesParser extends DefinitionParser<TopologyResources> {
    private final String namespace;

    public TopologyResourcesParser(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public StructParser<TopologyResources> parser() {
        return structParser(
                TopologyResources.class,
                "",
                "Contains a list of streams, functions and state stores to be used in producers and pipelines",
                optional(mapField(STREAMS, "stream definition", "Streams that can be referenced in producers and pipelines", new StreamDefinitionParser(true))),
                optional(mapField(TABLES, "table definition", "Tables that can be referenced in producers and pipelines", new TableDefinitionParser(true))),
                optional(mapField(GLOBAL_TABLES, "globalTable definition", "GlobalTables that can be referenced in producers and pipelines", new GlobalTableDefinitionParser(true))),
                optional(mapField(STORES, "state store definition", "State stores that can be referenced in producers and pipelines", new StateStoreDefinitionParser())),
                optional(mapField(FUNCTIONS, "function definition", "Functions that can be referenced in producers and pipelines", new TypedFunctionDefinitionParser())),
                (streams, tables, globalTables, stores, functions) -> {
                    final var result = new TopologyResources(namespace);
                    if (streams != null) streams.forEach(result::register);
                    if (tables != null) tables.forEach(result::register);
                    if (globalTables != null) globalTables.forEach(result::register);
                    if (stores != null) stores.forEach(result::register);
                    if (functions != null)
                        functions.forEach((name, func) -> result.register(name, func.withName(name)));
                    return result;
                }
        );
    }
}
