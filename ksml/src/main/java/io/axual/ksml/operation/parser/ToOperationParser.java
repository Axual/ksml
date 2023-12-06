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

import io.axual.ksml.definition.TopicNameExtractorDefinition;
import io.axual.ksml.definition.parser.KeyValueMapperDefinitionParser;
import io.axual.ksml.definition.parser.TopicDefinitionParser;
import io.axual.ksml.definition.parser.TopicNameExtractorDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.parser.YamlNode;

public class ToOperationParser extends OperationParser<ToOperation> {
    public ToOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public ToOperation parse(YamlNode node) {
        if (node == null) return null;
        final var child = node.get(KSMLDSL.Operations.TO);
        if (child != null) {
            if (child.isString()) {
                final var reference = child.asString();
                // There is a reference to either a topic or a topic name extractor
                if (resources.topic(reference) != null) {
                    return new ToOperation(
                            operationConfig(node, name),
                            resources.topic(reference),
                            null);
                }
                if (resources.function(reference) != null) {
                    return new ToOperation(
                            operationConfig(node, name),
                            new TopicNameExtractorDefinition(resources.function(reference)),
                            null);
                }
            } else {
                final var topic = new TopicDefinitionParser().parse(child);
                if (topic != null && topic.topic != null) {
                    return new ToOperation(
                            operationConfig(child, name),
                            topic,
                            parseOptionalFunction(child, KSMLDSL.Operations.To.PARTITIONER, new KeyValueMapperDefinitionParser()));
                }

                if (child.get(KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR) != null) {
                    return new ToOperation(
                            operationConfig(child, name),
                            parseFunction(child, KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR, new TopicNameExtractorDefinitionParser()),
                            parseOptionalFunction(child, KSMLDSL.Operations.To.PARTITIONER, new KeyValueMapperDefinitionParser()));
                }
            }
        }
        throw FatalError.topologyError("To operation should send data to topic or topic name extractor, but neither were specified");
    }
}
