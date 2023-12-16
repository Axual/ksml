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
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

public class ToOperationParser extends OperationParser<ToOperation> {
    private final ToTopicDefinitionParser topicParser;
    private final ToTopicNameExtractorDefinitionParser tneParser;
    private final StructSchema schema;

    public ToOperationParser(TopologyResources resources) {
        super("to", resources);
        topicParser = new ToTopicDefinitionParser(resources());
        tneParser = new ToTopicNameExtractorDefinitionParser(resources());
        final var fields = topicParser.fields();
        fields.addAll(tneParser.fields());
        schema = structSchema(
                ToOperation.class.getSimpleName(),
                "Ends the pipeline by sending all messages to a fixed topic, or to a topic returned by a topic name extractor function",
                fields);
    }

    private class ToOperationDefinitionParser extends DefinitionParser<ToOperation> {
        @Override
        protected StructParser<ToOperation> parser() {
            return structParser(
                    ToOperation.class,
                    "Either a topic or topic name extractor that defines where to write pipeline messages to",
                    topicParser,
                    tneParser,
                    (topic, tne) -> {
                        if (topic != null && topic.topic() != null) {
                            return new ToOperation(operationConfig(null), topic.topic(), topic.partitioner());
                        }
                        if (tne != null && tne.topicNameExtractor() != null) {
                            return new ToOperation(operationConfig(null), tne.topicNameExtractor(), tne.partitioner());
                        }
                        throw FatalError.topologyError("Unknown target for pipeline \"to\" operation");
                    });
        }
    }

    @Override
    public StructParser<ToOperation> parser() {
        return lookupField(
                "topic",
                KSMLDSL.Operations.TO,
                false,
                "Ends the pipeline by sending all messages to a fixed topic, or to a topic returned by a topic name extractor function",
                name -> {
                    final var topic = resources().topic(name);
                    if (topic != null)
                        return new ToOperation(operationConfig(null), topic, null);
                    return null;
                },
                new ToOperationDefinitionParser());
    }
}
