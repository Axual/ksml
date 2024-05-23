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


import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.store.StoreType;

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
                storeField(),
                (topic, keyType, valueType, store, tags) -> {
                    keyType = keyType != null ? keyType : UserType.UNKNOWN;
                    valueType = valueType != null ? valueType : UserType.UNKNOWN;
                    if (store != null) {
                        if (!keyType.dataType().isAssignableFrom(store.keyType().dataType())) {
                            throw new TopologyException("Incompatible key types between globalTable '" + topic + "' and its corresponding state store: " + keyType.dataType() + " and " + store.keyType().dataType());
                        }
                        if (!valueType.dataType().isAssignableFrom(store.valueType().dataType())) {
                            throw new TopologyException("Incompatible value types between globalTable '" + topic + "' and its corresponding state store: " + valueType.dataType() + " and " + store.valueType().dataType());
                        }
                    }
                    return new GlobalTableDefinition(topic, keyType, valueType, store);
                });
    }

    private StructParser<KeyValueStateStoreDefinition> storeField() {
        final var storeParser = new StateStoreDefinitionParser(StoreType.KEYVALUE_STORE);
        final var resourceParser = new TopologyResourceParser<>("state store", KSMLDSL.Streams.STORE, "KeyValue state store definition", null, storeParser);
        final var schema = optional(resourceParser).schema();
        return new StructParser<>() {
            @Override
            public KeyValueStateStoreDefinition parse(ParseNode node) {
                storeParser.defaultName(node.longName());
                final var resource = resourceParser.parse(node);
                if (resource != null && resource.definition() instanceof KeyValueStateStoreDefinition def) return def;
                return null;
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
