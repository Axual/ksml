package io.axual.ksml.datagenerator.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import io.axual.ksml.datagenerator.definition.ProducerDefinition;
import io.axual.ksml.definition.parser.PredicateDefinitionParser;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.datagenerator.dsl.ProducerDSL.*;

public class ProducerDefinitionParser extends ContextAwareParser<ProducerDefinition> {
    public ProducerDefinitionParser(ParseContext context) {
        super(context);
    }

    @Override
    public ProducerDefinition parse(YamlNode node) {
        if (node == null) return null;
        return new ProducerDefinition(
                new ReferenceOrInlineParser<>("generator", GENERATOR_ATTRIBUTE, context.getFunctionDefinitions()::get, new GeneratorDefinitionParser()).parse(node),
                parseDuration(node, INTERVAL_ATTRIBUTE),
                new ReferenceOrInlineParser<>("condition", CONDITION_ATTRIBUTE, context.getFunctionDefinitions()::get, new PredicateDefinitionParser()).parse(node),
                new ReferenceOrInlineParser<>("to", TARGET_ATTRIBUTE, context.getStreamDefinitions()::get, new StreamDefinitionParser()).parseDefinition(node));
    }
}
