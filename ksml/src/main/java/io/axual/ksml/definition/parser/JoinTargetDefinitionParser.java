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


import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.TopologyResourceParser;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinTargetDefinitionParser extends BaseParser<TopologyResource<TopicDefinition>> {
    private final TopologyResources resources;

    public JoinTargetDefinitionParser(TopologyResources resources) {
        this.resources = resources;
    }

    @Override
    public TopologyResource<TopicDefinition> parse(ParseNode node) {
        if (node == null) return null;
        if (node.get(Operations.Join.WITH_STREAM) != null) {
            return new TopologyResourceParser<>("stream", Operations.Join.WITH_STREAM, null, (name, tags) -> resources.topic(name), new StreamDefinitionParser(false)).parse(node);
        }
        if (parseString(node, Operations.Join.WITH_TABLE) != null) {
            return new TopologyResourceParser<>("table", Operations.Join.WITH_TABLE, null, (name, tags) -> resources.topic(name), new TableDefinitionParser(false)).parse(node);
        }
        if (parseString(node, Operations.Join.WITH_GLOBAL_TABLE) != null) {
            return new TopologyResourceParser<>("globalTable", Operations.Join.WITH_GLOBAL_TABLE, null, (name, tags) -> resources.topic(name), new GlobalTableDefinitionParser(false)).parse(node);
        }
        throw new ParseException(node, "Stream definition missing");
    }
}
