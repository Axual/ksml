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

import io.axual.ksml.definition.parser.ToTopicDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;

public class ToOperationParser extends OperationParser<ToOperation> {
    public ToOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TO_TOPIC, resources);
    }

    private class ToOperationDefinitionParser extends DefinitionParser<ToOperation> {
        @Override
        protected StructsParser<ToOperation> parser() {
            return structsParser(
                    ToOperation.class,
                    "",
                    "Inline defined topic to send output messages to",
                    new ToTopicDefinitionParser(resources()),
                    (inlineTopicAndPartitioner, tags) -> {
                        if (inlineTopicAndPartitioner != null && inlineTopicAndPartitioner.topic() != null) {
                            return new ToOperation(operationConfig(null, tags), inlineTopicAndPartitioner.topic(), inlineTopicAndPartitioner.partitioner());
                        }
                        throw new TopologyException("Unknown target for pipeline \"to\" operation");
                    });
        }
    }

    @Override
    public StructsParser<ToOperation> parser() {
        return lookupField(
                "target topic",
                KSMLDSL.Operations.TO_TOPIC,
                "Ends the pipeline by sending all messages to a stream, table or globalTable, or to an inline defined output topic and optional partitioner",
                (name, tags) -> {
                    // Try to find a corresponding topic definition
                    final var topic = resources().topic(name);
                    return topic != null ? new ToOperation(operationConfig(null, tags), topic, null) : null;
                },
                new ToOperationDefinitionParser());
    }
}
