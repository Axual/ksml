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


import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceParser;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinTargetDefinitionParser extends BaseParser<TopologyResource<TopicDefinition>> {
    private final TopologyResources resources;

    public JoinTargetDefinitionParser(TopologyResources resources) {
        this.resources = resources;
    }

    // The key that holds the join target, or null if there is none
    public static String keyOf(ParseNode node) {
        if (node.get(Operations.Join.WITH_STREAM) != null) return Operations.Join.WITH_STREAM;
        if (node.get(Operations.Join.WITH_TABLE) != null) return Operations.Join.WITH_TABLE;
        if (node.get(Operations.Join.WITH_GLOBAL_TABLE) != null) return Operations.Join.WITH_GLOBAL_TABLE;
        return null;
    }

    @Override
    public TopologyResource<TopicDefinition> parse(ParseNode node) {
        if (node == null) return null;
        final var key = keyOf(node);
        if (key == null) throw new ParseException(node, "Stream definition missing");
        return switch (key) {
            // An unknown name may be an "as" name. The stream or table field of the join checks it
            case Operations.Join.WITH_STREAM -> parseTarget(node, "stream", key, new StreamDefinitionParser(resources, true));
            case Operations.Join.WITH_TABLE -> parseTarget(node, "table", key, new TableDefinitionParser(resources, true));
            default -> {
                final var result = parseTarget(node, "globalTable", key, new GlobalTableDefinitionParser(resources, true));
                // Kafka Streams can not make a GlobalKTable from a pipeline result
                if (result.definition() == null) {
                    throw new ParseException(node, "Unknown globalTable \"" + result.name() + "\". Define it under \"" + KSMLDSL.GLOBAL_TABLES + "\". For a table saved with \"as\", use \"table\" instead");
                }
                yield result;
            }
        };
    }

    // Keeps an unknown name, without definition
    private <F extends TopicDefinition> TopologyResource<TopicDefinition> parseTarget(ParseNode node, String type, String key, StructsParser<F> parser) {
        return new TopologyResourceParser<TopicDefinition, F>(type, key, null, (name, tags) -> resources.topic(name), parser, true).parse(node);
    }
}
