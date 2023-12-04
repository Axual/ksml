package io.axual.ksml.definition.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Generator
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


import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class ProducerDefinitionParser extends ContextAwareParser<ProducerDefinition> {
    public ProducerDefinitionParser(String prefix, TopologyResources resources) {
        super(prefix, resources);
    }

    @Override
    public ProducerDefinition parse(YamlNode node) {
        if (node == null) return null;
        return new ProducerDefinition(
                new TopologyResourceParser<>("generator", PRODUCER_GENERATOR_ATTRIBUTE, resources::function, new GeneratorDefinitionParser()).parseDefinition(node),
                parseDuration(node, PRODUCER_INTERVAL_ATTRIBUTE),
                new TopologyResourceParser<>("condition", PRODUCER_CONDITION_ATTRIBUTE, resources::function, new PredicateDefinitionParser()).parseDefinition(node),
                new TopologyResourceParser<>("to", PRODUCER_TARGET_ATTRIBUTE, resources::topic, new TopicDefinitionParser()).parseDefinition(node));
    }
}
