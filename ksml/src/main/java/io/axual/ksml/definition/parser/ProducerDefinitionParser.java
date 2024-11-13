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
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceAwareParser;

import static io.axual.ksml.dsl.KSMLDSL.Producers;

public class ProducerDefinitionParser extends TopologyResourceAwareParser<ProducerDefinition> {
    public ProducerDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    public StructsParser<ProducerDefinition> parser() {
        return structsParser(
                ProducerDefinition.class,
                "",
                "Definition of a Producer that regularly generates messages for a topic",
                functionField(Producers.GENERATOR, "The function that generates records", new GeneratorDefinitionParser(false)),
                optional(functionField(Producers.CONDITION, "A function that validates the generator's result message. Returns \"true\" when the message may be produced on the topic, \"false\" otherwise.", new PredicateDefinitionParser(false))),
                optional(functionField(Producers.UNTIL, "A predicate that returns true to indicate producing should stop.", new PredicateDefinitionParser(false))),
                topicField(Producers.TARGET, "The topic to produce to", new TopicDefinitionParser(resources(), false)),
                optional(longField(Producers.COUNT, "The number of messages to produce.")),
                optional(longField(Producers.BATCH_SIZE, 1L, "The size of batches")),
                optional(durationField(Producers.INTERVAL, "The interval with which the generator is called")),
                (generator, condition, until, target, count, batchSize, interval, tags) -> new ProducerDefinition(generator, condition, until, target, count, batchSize, interval));
    }
}
