package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.MessageProducerDefinition;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.MESSAGE_PRODUCER_GENERATOR;
import static io.axual.ksml.dsl.KSMLDSL.MESSAGE_PRODUCER_WINDOW;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_FROM_ATTRIBUTE;

public class MessageProducerDefinitionParser extends ContextAwareParser<MessageProducerDefinition> {
    public MessageProducerDefinitionParser(ParseContext context) {
        super(context);
    }

    @Override
    public MessageProducerDefinition parse(YamlNode node) {
        if (node == null) return null;
        return new MessageProducerDefinition(
                new ReferenceOrInlineParser<>("generator", MESSAGE_PRODUCER_GENERATOR, context.getFunctionDefinitions()::get, new MessageGeneratorDefinitionParser()).parse(node),
                parseDuration(node, MESSAGE_PRODUCER_WINDOW),
                new ReferenceOrInlineParser<>("target", PIPELINE_FROM_ATTRIBUTE, context.getStreamDefinitions()::get, new StreamDefinitionParser()).parse(node));
    }
}
