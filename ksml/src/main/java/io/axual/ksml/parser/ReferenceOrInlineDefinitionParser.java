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


import io.axual.ksml.definition.Ref;

// Certain KSML resources (like streams, tables and functions) can be referenced from pipelines,
// or they can be defined inline. This parser distinguishes between the two.
public class ReferenceOrInlineDefinitionParser<T, F extends T> extends BaseParser<Ref<T>> {
    private final String resourceType;
    private final String childName;
    private final BaseParser<F> inlineParser;

    public ReferenceOrInlineDefinitionParser(String resourceType, String childName, BaseParser<F> inlineParser) {
        this.resourceType = resourceType;
        this.childName = childName;
        this.inlineParser = inlineParser;
    }

    @Override
    public Ref<T> parse(YamlNode node) {
        if (node == null) return null;

        // Check if the node is a text node --> parse as direct reference
        if (node.childIsText(childName)) {
            final var resourceToFind = parseString(node, childName);
            return new Ref<>(resourceToFind, node.get(childName), null);
        }

        // Parse as inline definition using the supplied inline parser
        return new Ref<>(null, node, inlineParser.parse(node.get(childName)));
    }
}
