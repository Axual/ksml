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
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class JoinTargetDefinitionParser extends ContextAwareParser<TopicDefinition> {
    public JoinTargetDefinitionParser(String prefix, TopologyResources resources) {
        super(prefix, resources);
    }

    @Override
    public TopicDefinition parse(YamlNode node) {
        if (node == null) return null;
        if (node.get(Operations.Join.WITH_STREAM) != null) {
            return new TopologyResourceParser<>("stream", Operations.Join.WITH_STREAM, resources::topic, new StreamDefinitionParser()).parseDefinition(node);
        }
        if (parseString(node, Operations.Join.WITH_TABLE) != null) {
            return new TopologyResourceParser<>("table", Operations.Join.WITH_TABLE, resources::topic, new TableDefinitionParser()).parseDefinition(node);
        }
        if (parseString(node, Operations.Join.WITH_GLOBAL_TABLE) != null) {
            return new TopologyResourceParser<>("globalTable", Operations.Join.WITH_GLOBAL_TABLE, resources::topic, new GlobalTableDefinitionParser()).parseDefinition(node);
        }
        throw new KSMLParseException(node, "Stream definition missing");
    }
}
