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


import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.TopologyBaseResourceAwareParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class StreamDefinitionParser extends TopologyBaseResourceAwareParser<StreamDefinition> {
    private static final String DOC = "Contains a definition of a Stream, which can be referenced by producers and pipelines";
    private static final String TOPIC_DOC = "The name of the Kafka topic for this stream";
    private final boolean isSource;

    public StreamDefinitionParser(TopologyBaseResources resources, boolean isSource) {
        super(resources);
        this.isSource = isSource;
    }

    @Override
    public StructParser<StreamDefinition> parser() {
        final var keyField = userTypeField(Streams.KEY_TYPE, "The key type of the stream");
        final var valueField = userTypeField(Streams.VALUE_TYPE, "The value type of the stream");
        if (isSource) return structParser(
                StreamDefinition.class,
                "Source",
                DOC,
                stringField(Streams.TOPIC, TOPIC_DOC),
                keyField,
                valueField,
                optional(functionField(Streams.TIMESTAMP_EXTRACTOR, "A function extracts the event time from a consumed record", new TimestampExtractorDefinitionParser())),
                optional(stringField(Streams.OFFSET_RESET_POLICY, "Policy that determines what to do when there is no initial offset in Kafka, or if the current offset does not exist any more on the server (e.g. because that data has been deleted)")),
                (topic, keyType, valueType, tsExtractor, resetPolicy, tags) -> {
                    final var policy = OffsetResetPolicyParser.parseResetPolicy(resetPolicy);
                    return new StreamDefinition(topic, keyType, valueType, tsExtractor, policy);
                });
        return structParser(
                StreamDefinition.class,
                "",
                DOC,
                stringField(Streams.TOPIC, TOPIC_DOC),
                optional(keyField),
                optional(valueField),
                (topic, keyType, valueType, tags) -> new StreamDefinition(topic, keyType, valueType, null, null));
    }
}
