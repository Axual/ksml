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


import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

public class GlobalTableDefinitionParser extends DefinitionParser<GlobalTableDefinition> {
    private final boolean requireKeyValueType;

    public GlobalTableDefinitionParser(boolean requireKeyValueType) {
        this.requireKeyValueType = requireKeyValueType;
    }

    @Override
    public StructParser<GlobalTableDefinition> parser() {
        final var keyField = userTypeField(KSMLDSL.Streams.KEY_TYPE, "The key type of the global table");
        final var valueField = userTypeField(KSMLDSL.Streams.VALUE_TYPE, "The value type of the global table");
        return structParser(
                GlobalTableDefinition.class,
                requireKeyValueType ? "" : "WithOptionalTypes",
                "Contains a definition of a GlobalTable, which can be referenced by producers and pipelines",
                stringField(KSMLDSL.Streams.TOPIC, "The name of the Kafka topic for this global table"),
                requireKeyValueType ? keyField : optional(keyField),
                requireKeyValueType ? valueField : optional(valueField),
                GlobalTableDefinition::new);
    }
}
