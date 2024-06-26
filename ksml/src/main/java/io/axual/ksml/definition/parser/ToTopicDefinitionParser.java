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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.parser.ParserWithSchemas;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.ToTopicDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceAwareParser;

import java.util.ArrayList;
import java.util.List;

public class ToTopicDefinitionParser extends TopologyResourceAwareParser<ToTopicDefinition> {
    public ToTopicDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    protected StructsParser<ToTopicDefinition> parser() {
        return structsParser(
                ToTopicDefinition.class,
                "",
                "Writes out pipeline messages to a topic",
                optional(customField(KSMLDSL.Operations.To.TOPIC, "A reference to a stream, table or globalTable, or an inline definition of the output topic", topicParser())),
                optional(functionField(KSMLDSL.Operations.To.PARTITIONER, "A function that partitions the records in the output topic", new StreamPartitionerDefinitionParser(false))),
                (topic, partitioner, tags) -> topic != null ? new ToTopicDefinition(topic, partitioner) : null);
    }

    private ParserWithSchemas<TopicDefinition> topicParser() {
        final var nestedParser = new TopicDefinitionParser(resources(), false);
        final var schemas = new ArrayList<DataSchema>();
        schemas.add(DataSchema.stringSchema());
        schemas.addAll(nestedParser.schemas());
        return ParserWithSchemas.of(node -> node.isString() ? new TopicDefinition(node.asString(), UserType.UNKNOWN, UserType.UNKNOWN, null, null) : nestedParser.parse(node), schemas);
    }
}
