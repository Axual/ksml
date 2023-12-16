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
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class TopicDefinitionParser extends DefinitionParser<TopicDefinition> {
    private final boolean requireKeyValueType;

    public TopicDefinitionParser(boolean requireKeyValueType) {
        super(null);
        this.requireKeyValueType = requireKeyValueType;
    }

    @Override
    public StructParser<TopicDefinition> parser() {
        return structParser(
                TopicDefinition.class,
                "Contains a definition of a Topic, which can be referenced by producers and pipelines",
                stringField(Streams.TOPIC, true, "The name of the Kafka topic"),
                userTypeField(Streams.KEY_TYPE, requireKeyValueType, "The key type of the topic"),
                userTypeField(Streams.VALUE_TYPE, requireKeyValueType, "The value type of the topic"),
                (topic, keyType, valueType) -> new TopicDefinition("Topic", topic, keyType, valueType));
    }
}
