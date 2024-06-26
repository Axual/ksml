package io.axual.ksml.operation.parser;

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

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.parser.ToTopicDefinitionParser;
import io.axual.ksml.definition.parser.ToTopicNameExtractorDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;

import java.util.ArrayList;
import java.util.List;

public class ToOperationParser extends OperationParser<ToOperation> {
    private final ToTopicDefinitionParser topicParser;
    private final ToTopicNameExtractorDefinitionParser tneParser;
    private final List<StructSchema> schemas = new ArrayList<>();

    public ToOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TO, resources);
        topicParser = new ToTopicDefinitionParser(resources());
        tneParser = new ToTopicNameExtractorDefinitionParser(resources());
        schemas.addAll(topicParser.schemas());
        schemas.addAll(tneParser.schemas());
    }

    private class ToOperationDefinitionParser extends DefinitionParser<ToOperation> {
        @Override
        protected StructsParser<ToOperation> parser() {
            return structsParser(
                    ToOperation.class,
                    "",
                    "Either a topic or topic name extractor that defines where to write pipeline messages to",
                    optional(topicParser),
                    optional(tneParser),
                    (toTopic, toTne, tags) -> {
                        if (toTopic != null && toTopic.topic() != null) {
                            return new ToOperation(operationConfig(null, tags), toTopic.topic(), toTopic.partitioner());
                        }
                        if (toTne != null && toTne.topicNameExtractor() != null) {
                            return new ToOperation(operationConfig(null, tags), toTne.topicNameExtractor(), toTne.partitioner());
                        }
                        throw new TopologyException("Unknown target for pipeline \"to\" operation");
                    });
        }
    }

    @Override
    public StructsParser<ToOperation> parser() {
        return lookupField(
                "topic",
                KSMLDSL.Operations.TO,
                "Ends the pipeline by sending all messages to a fixed topic, or to a topic returned by a topic name extractor function",
                (name, tags) -> {
                    // First try to find a corresponding topic definition
                    final var topic = resources().topic(name);
                    if (topic != null) {
                        return new ToOperation(operationConfig(null, tags), topic, null);
                    }
                    // Then try to find a corresponding topic name extractor function
                    final var tne = resources().function(name);
                    if (tne != null) {
                        return new ToOperation(operationConfig(null, tags), tne, null);
                    }
                    return null;
                },
                new ToOperationDefinitionParser());
    }
}
