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
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.definition.TopologyResource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

// Certain KSML resources (like streams, tables and functions) can be referenced from pipelines,
// or they can be defined inline. This parser distinguishes between the two.
public class TopologyResourceParser<T, F extends T> extends DefinitionParser<TopologyResource<T>> {
    private final String resourceType;
    private final String childName;
    private final String doc;
    private final BiFunction<String, ContextTags, T> lookup;
    private final StructsParser<F> inlineParser;
    private final boolean allowLookupFail;

    public TopologyResourceParser(String resourceType, String childName, String doc, BiFunction<String, ContextTags, T> lookup, StructsParser<F> inlineParser) {
        this(resourceType, childName, doc, lookup, inlineParser, false);
    }

    public TopologyResourceParser(String resourceType, String childName, String doc, BiFunction<String, ContextTags, T> lookup, StructsParser<F> inlineParser, boolean allowLookupFail) {
        this.resourceType = resourceType;
        this.childName = childName;
        this.doc = doc;
        this.lookup = lookup;
        this.inlineParser = inlineParser;
        this.allowLookupFail = allowLookupFail;
    }

    @Override
    public StructsParser<TopologyResource<T>> parser() {
        final var schemas = new ArrayList<DataSchema>();
        schemas.add(DataSchema.stringSchema());
        schemas.addAll(inlineParser.schemas());
        final var typeName = "StringOrInline" + String.join("OrInline", inlineParser.schemas().stream().map(NamedSchema::name).toArray(String[]::new));
//        final var doc = "Reference to " + resourceType + ", or inline " + String.join(", or inline ", inlineParser.schemas().stream().map(NamedSchema::name).toArray(String[]::new));
        final var schema = structSchema(typeName, doc, List.of(new DataField(childName, new UnionSchema(schemas.stream().map(DataField::new).toArray(DataField[]::new)), doc)));
        final var stringParser = stringField(childName, false, null, doc);
        return new StructsParser<>() {
            @Override
            public TopologyResource<T> parse(ParseNode node) {
                if (node == null) return null;

                // Check if the node is a text node --> parse as direct reference
                final var resourceToFind = stringParser.parse(node);
                if (resourceToFind != null) {
                    final var resource = lookup.apply(resourceToFind, node.tags());
                    if (resource == null && !allowLookupFail) {
                        throw new ParseException(node, "Unknown " + resourceType + " \"" + resourceToFind + "\"");
                    }
                    return new TopologyResource<>(resourceToFind, resource, node.tags());
                }

                // Parse as anonymous inline definition using the supplied inline parser
                final var childNode = node.get(childName);
                if (childNode != null) {
                    final var name = childNode.longName();
                    return new TopologyResource<>(name, inlineParser.parse(childNode), childNode.tags());
                }

                final var name = node.appendName(childName).longName();
                return new TopologyResource<>(name, null, node.tags());
            }

            @Override
            public List<StructSchema> schemas() {
                return List.of(schema);
            }
        };
    }

    public T parseDefinition(ParseNode node) {
        final var result = parse(node);
        return result != null ? result.definition() : null;
    }
}
