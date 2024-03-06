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
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class StreamDefinitionParser extends DefinitionParser<StreamDefinition> {
    private final boolean requireKeyValueType;

    public StreamDefinitionParser(boolean requireKeyValueType) {
        this.requireKeyValueType = requireKeyValueType;
    }

    @Override
    public StructParser<StreamDefinition> parser() {
        final var keyField = userTypeField(Streams.KEY_TYPE, "The key type of the stream");
        final var valueField = userTypeField(Streams.VALUE_TYPE, "The value type of the stream");
        return structParser(
                StreamDefinition.class,
                requireKeyValueType ? "" : "WithOptionalTypes",
                "Contains a definition of a Stream, which can be referenced by producers and pipelines",
                stringField(Streams.TOPIC, "The name of the Kafka topic for this stream"),
                requireKeyValueType ? keyField : optional(keyField),
                requireKeyValueType ? valueField : optional(valueField),
                StreamDefinition::new);
    }
}
