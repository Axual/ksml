package io.axual.ksml.definition.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.definition.ToTopicNameExtractorDefinition;
import io.axual.ksml.definition.TopicNameExtractorDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;

public class ToTopicNameExtractorDefinitionParser extends ContextAwareParser<ToTopicNameExtractorDefinition> {
    public ToTopicNameExtractorDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    protected StructParser<ToTopicNameExtractorDefinition> parser() {
        return structParser(
                ToTopicNameExtractorDefinition.class,
                "",
                "Writes out pipeline messages to a topic as given by a topic name extractor",
                optional(lookupField(
                        "topic name extractor",
                        KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR,
                        "Reference to a pre-defined topic name extractor, or an inline definition of a topic name extractor and an optional stream partitioner",
                        (name, tags) -> resources().function(name),
                        new TopicNameExtractorDefinitionParser())),
                new StreamPartitionerDefinitionParser(),
                (tne, partitioner, tags) -> tne != null ? new ToTopicNameExtractorDefinition(new TopicNameExtractorDefinition(tne), partitioner) : null);
    }
}
