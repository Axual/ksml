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


import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.store.StoreType;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TableDefinitionParser extends BaseParser<TableDefinition> {
    @Override
    public TableDefinition parse(YamlNode node) {
        if (node == null) return null;

        // Use the topic name as the default store name
        var topic = parseString(node, TOPIC_ATTRIBUTE);

        // Parse the key and value types
        var keyType = UserTypeParser.parse(parseString(node, KEYTYPE_ATTRIBUTE));
        var valueType = UserTypeParser.parse(parseString(node, VALUETYPE_ATTRIBUTE));

        // Parse an optional key value state store
        var parsedStore = new StateStoreDefinitionParser(StoreType.KEYVALUE_STORE, topic).parse(node.get(STORE_ATTRIBUTE));

        // If there is no state store, then return just the table definition
        if (!(parsedStore instanceof KeyValueStateStoreDefinition kvStore)) {
            return new TableDefinition(topic, keyType, valueType, null);
        }

        // Ensure that the store definition is in line with the given key and value types
        kvStore = new KeyValueStateStoreDefinition(
                kvStore.name(),
                kvStore.persistent(),
                kvStore.timestamped(),
                kvStore.versioned(),
                kvStore.historyRetention(),
                kvStore.segmentInterval(),
                kvStore.keyType() != null && kvStore.keyType().dataType() != DataType.UNKNOWN ? kvStore.keyType() : keyType,
                kvStore.valueType() != null && kvStore.valueType().dataType() != DataType.UNKNOWN ? kvStore.valueType() : valueType,
                kvStore.caching(),
                kvStore.logging());
        if (!keyType.dataType().isAssignableFrom(kvStore.keyType().dataType())) {
            throw new KSMLParseException(node, "Incompatible key types between table \'" + topic + "\' and its corresponding state store: " + keyType.dataType() + " and " + kvStore.keyType().dataType());
        }
        if (!valueType.dataType().isAssignableFrom(kvStore.valueType().dataType())) {
            throw new KSMLParseException(node, "Incompatible value types between table \'" + topic + "\' and its corresponding state store: " + valueType.dataType() + " and " + kvStore.valueType().dataType());
        }

        // Create and return the table definition with the state store
        return new TableDefinition(topic, keyType, valueType, kvStore);
    }
}
