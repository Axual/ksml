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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;

import java.time.Duration;

import static io.axual.ksml.dsl.KSMLDSL.Producers;

public class ProducerDefinitionParser extends ContextAwareParser<ProducerDefinition> {
    public ProducerDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    public StructParser<ProducerDefinition> parser() {
        return structParser(
                ProducerDefinition.class,
                "",
                "Definition of a Producer that regularly generates messages for a topic",
                functionField(Producers.GENERATOR, "The function that generates records", new GeneratorDefinitionParser()),
                durationField(Producers.INTERVAL, "The interval with which the generator is called"),
                optional(functionField(Producers.CONDITION, "A function that validates the generator's result message. Returns \"true\" when the message may be produced on the topic, \"false\" otherwise.", new PredicateDefinitionParser())),
                topicField(Producers.TARGET, "The topic to produce to", new TopicDefinitionParser(false)),
                optional(integerField(Producers.COUNT, "The number of messages to produce.")),
                optional(functionField(Producers.UNTIL, "A predicate that returns true to indicate producing should stop.", new PredicateDefinitionParser())),
                (generator, interval, condition, target, count, until, tags) -> new ProducerDefinition(generator, interval, condition, target, count, until));
    }
}
