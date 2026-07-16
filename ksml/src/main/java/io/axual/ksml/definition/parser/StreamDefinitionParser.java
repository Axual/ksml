package io.axual.ksml.definition.parser;

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


import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyBaseResourceFields;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class StreamDefinitionParser extends DefinitionParser<StreamDefinition> {
    private static final String TOPIC_DOC = "The name of the Kafka topic for this stream";
    private final TopologyBaseResourceFields resourceFields;
    private final boolean isJoinTarget;

    public StreamDefinitionParser(TopologyBaseResources resources, boolean isJoinTarget) {
        this.resourceFields = new TopologyBaseResourceFields(resources);
        this.isJoinTarget = isJoinTarget;
    }

    @Override
    public StructsParser<StreamDefinition> parser() {
        final var keyField = FieldParsers.userTypeField(Streams.KEY_TYPE, "The key type of the stream", true);
        final var valueField = FieldParsers.userTypeField(Streams.VALUE_TYPE, "The value type of the stream", true);
        if (!isJoinTarget) return FieldParsers.structsParser(
                StreamDefinition.class,
                "",
                "Contains a definition of a Stream, which can be referenced by producers and pipelines",
                FieldParsers.stringField(Streams.TOPIC, TOPIC_DOC),
                keyField,
                valueField,
                FieldParsers.optional(FieldParsers.stringField(Streams.OFFSET_RESET_POLICY, "Policy that determines what to do when there is no initial offset in Kafka, or if the current offset does not exist any more on the server (e.g. because that data has been deleted)")),
                FieldParsers.optional(resourceFields.functionField(Streams.TIMESTAMP_EXTRACTOR, "A function extracts the event time from a consumed record", new TimestampExtractorDefinitionParser(false))),
                FieldParsers.optional(resourceFields.functionField(KSMLDSL.Streams.PARTITIONER, "A function that determines to which topic partition a given message needs to be written", new StreamPartitionerDefinitionParser(false))),
                (topic, keyType, valueType, resetPolicy, tsExtractor, partitioner, tags) -> {
                    final var policy = OffsetResetPolicyParser.parseResetPolicy(resetPolicy);
                    return new StreamDefinition(topic, TopologyBaseResourceFields.resolveUserType(keyType, topic, true), TopologyBaseResourceFields.resolveUserType(valueType, topic, false), policy, tsExtractor, partitioner);
                });
        return FieldParsers.structsParser(
                StreamDefinition.class,
                "AsJoinTarget",
                "Reference to a Stream in a join or merge operation",
                FieldParsers.stringField(Streams.TOPIC, TOPIC_DOC),
                FieldParsers.optional(keyField),
                FieldParsers.optional(valueField),
                (topic, keyType, valueType, tags) -> new StreamDefinition(topic, TopologyBaseResourceFields.resolveUserType(keyType, topic, true), TopologyBaseResourceFields.resolveUserType(valueType, topic, false), null, null, null));
    }
}
