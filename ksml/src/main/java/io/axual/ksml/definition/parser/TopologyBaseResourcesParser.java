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

import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;

import static io.axual.ksml.dsl.KSMLDSL.FUNCTIONS;
import static io.axual.ksml.dsl.KSMLDSL.STORES;

public class TopologyBaseResourcesParser extends DefinitionParser<TopologyBaseResources> {
    private final String namespace;

    public TopologyBaseResourcesParser(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public StructsParser<TopologyBaseResources> parser() {
        return structsParser(
                TopologyBaseResources.class,
                "",
                "Contains a list of functions and state stores to be used in streams, producers and pipelines",
                optional(mapField(STORES, "store", "state store definition", "State stores that can be referenced in producers and pipelines", new StateStoreDefinitionParser())),
                optional(mapField(FUNCTIONS, "function", "function definition", "Functions that can be referenced in producers and pipelines", new TypedFunctionDefinitionParser())),
                (stores, functions, tags) -> {
                    final var result = new TopologyBaseResources(namespace);
                    if (stores != null) stores.forEach(result::register);
                    if (functions != null)
                        functions.forEach((name, func) -> result.register(name, func.withName(name)));
                    return result;
                }
        );
    }
}
