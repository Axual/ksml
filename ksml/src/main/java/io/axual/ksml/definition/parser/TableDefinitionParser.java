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
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.store.StoreType;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.Streams;

public class TableDefinitionParser extends DefinitionParser<TableDefinition> {
    private final boolean requireKeyValueType;

    public TableDefinitionParser(boolean requireKeyValueType) {
        this.requireKeyValueType = requireKeyValueType;
    }

    @Override
    public StructParser<TableDefinition> parser() {
        return structParser(
                TableDefinition.class,
                "Contains a definition of a Table, which can be referenced by producers and pipelines",
                stringField(Streams.TOPIC, true, "The name of the Kafka topic for this table"),
                userTypeField(Streams.KEY_TYPE, requireKeyValueType, "The key type of the table"),
                userTypeField(Streams.VALUE_TYPE, requireKeyValueType, "The value type of the table"),
                storeField(),
                (topic, keyType, valueType, store) -> {
                    keyType = keyType != null ? keyType : UserType.UNKNOWN;
                    valueType = valueType != null ? valueType : UserType.UNKNOWN;
                    if (store != null) {
                        if (!keyType.dataType().isAssignableFrom(store.keyType().dataType())) {
                            throw new TopologyException("Incompatible key types between table '" + topic + "' and its corresponding state store: " + keyType.dataType() + " and " + store.keyType().dataType());
                        }
                        if (!valueType.dataType().isAssignableFrom(store.valueType().dataType())) {
                            throw new TopologyException("Incompatible value types between table '" + topic + "' and its corresponding state store: " + valueType.dataType() + " and " + store.valueType().dataType());
                        }
                    }
                    return new TableDefinition(topic, keyType, valueType, store);
                });
    }

    private StructParser<KeyValueStateStoreDefinition> storeField() {
        final var resourceParser = new TopologyResourceParser<>("state store", Streams.STORE, "State store definition", null, new StateStoreDefinitionParser(StoreType.KEYVALUE_STORE));
        final var field = new DataField(Streams.STORE, new UnionSchema(DataSchema.nullSchema(), resourceParser.schema()), "Definition of the keyValue state store associated with the table", DataField.NO_INDEX, false);
        final var schema = structSchema(KeyValueStateStoreDefinition.class, field.doc(), List.of(field));
        return new StructParser<>() {
            @Override
            public KeyValueStateStoreDefinition parse(ParseNode node) {
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
