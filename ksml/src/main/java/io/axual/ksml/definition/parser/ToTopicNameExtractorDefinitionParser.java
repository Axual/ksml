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
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceAwareParser;

public class ToTopicNameExtractorDefinitionParser extends TopologyResourceAwareParser<ToTopicNameExtractorDefinition> {
    public ToTopicNameExtractorDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    protected StructsParser<ToTopicNameExtractorDefinition> parser() {
        return structsParser(
                ToTopicNameExtractorDefinition.class,
                "",
                "Writes out pipeline messages to a topic as given by a topic name extractor",
                optional(lookupField(
                        "topic name extractor",
                        KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR,
                        "Reference to a pre-defined topic name extractor, or an inline definition of a topic name extractor",
                        (name, tags) -> resources().function(name),
                        new TopicNameExtractorDefinitionParser(false))),
                optional(functionField(KSMLDSL.Operations.To.PARTITIONER, "A function that partitions the records in the output topic", new StreamPartitionerDefinitionParser(false))),
                (tne, partitioner, tags) -> tne != null ? new ToTopicNameExtractorDefinition(new TopicNameExtractorDefinition(tne), partitioner) : null);
    }
}
