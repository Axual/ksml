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


import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyBaseResourceAwareParser;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class TopicDefinitionParser extends TopologyBaseResourceAwareParser<TopicDefinition> {
    private static final String DOC = "Contains a definition of a Kafka topic, to be used by producers and pipelines";
    private static final String TOPIC_DOC = "The name of the Kafka topic";
    private final boolean isSource;

    public TopicDefinitionParser(TopologyBaseResources resources, boolean isSource) {
        super(resources);
        this.isSource = isSource;
    }

    @Override
    public StructsParser<TopicDefinition> parser() {
        final var keyField = userTypeField(Streams.KEY_TYPE, "The key type of the topic");
        final var valueField = userTypeField(Streams.VALUE_TYPE, "The value type of the topic");
        if (isSource) return structsParser(
                TopicDefinition.class,
                "Source",
                DOC,
                stringField(Streams.TOPIC, TOPIC_DOC),
                keyField,
                valueField,
                optional(functionField(Streams.TIMESTAMP_EXTRACTOR, "A function extracts the event time from a consumed record", new TimestampExtractorDefinitionParser(false))),
                optional(stringField(Streams.OFFSET_RESET_POLICY, "Policy that determines what to do when there is no initial offset in Kafka, or if the current offset does not exist any more on the server (e.g. because that data has been deleted)")),
                (topic, keyType, valueType, tsExtractor, resetPolicy, tags) -> topic != null ? new TopicDefinition(topic, keyType, valueType, tsExtractor, OffsetResetPolicyParser.parseResetPolicy(resetPolicy)) : null);
        return structsParser(
                TopicDefinition.class,
                "",
                DOC,
                stringField(Streams.TOPIC, TOPIC_DOC),
                optional(keyField),
                optional(valueField),
                (topic, keyType, valueType, tags) -> topic != null ? new TopicDefinition(topic, keyType, valueType, null, null) : null);
    }
}
