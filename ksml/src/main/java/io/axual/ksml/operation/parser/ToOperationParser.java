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
import io.axual.ksml.definition.parser.TopicOrTopicNameExtractorDefinitionParser;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.YamlNode;

public class ToOperationParser extends OperationParser<ToOperation> {
    private final TopicOrTopicNameExtractorDefinitionParser parser;
    private final StructSchema schema;

    public ToOperationParser(TopologyResources resources) {
        super("to", resources);
        // Set up the special parser for taking care of the heavy lifting for this operation
        parser = new TopicOrTopicNameExtractorDefinitionParser(resources);
        // Wrap the fields of the parser's schema into a nice definition with the operation's name
        schema = structSchema(
                ToOperation.class.getSimpleName(),
                "Ends the pipeline by sending all messages to a fixed topic, or to a topic returned by a topic name extractor function",
                parser.schema().fields());
    }

    @Override
    public StructParser<ToOperation> parser() {
        return new StructParser<>() {
            @Override
            public ToOperation parse(YamlNode node) {
                final var target = parser.parse(node);
                if (target != null) {
                    if (target.topic() != null) {
                        return new ToOperation(operationConfig(null), target.topic(), target.partitioner());
                    }
                    if (target.topicNameExtractor() != null) {
                        return new ToOperation(operationConfig(null), target.topicNameExtractor(), target.partitioner());
                    }
                }
                return null;
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
