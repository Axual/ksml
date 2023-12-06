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


import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.store.StoreType;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class StateStoreDefinitionParser extends BaseParser<StateStoreDefinition> {
    private final StoreType expectedType;

    public StateStoreDefinitionParser() {
        this(null);
    }

    public StateStoreDefinitionParser(StoreType expectedType) {
        this(expectedType, null);
    }

    public StateStoreDefinitionParser(StoreType expectedType, String defaultName) {
        this.expectedType = expectedType;
        setDefaultName(defaultName);
    }

    @Override
    public StateStoreDefinition parse(YamlNode node) {
        if (node == null) return null;
        var type = storeTypeOf(parseString(node, Stores.TYPE));
        if (type == null) {
            if (expectedType == null) {
                throw FatalError.parseError(node, "State store type not specified");
            }
            type = expectedType;
        }

        if (expectedType != null && type != expectedType) {
            throw FatalError.parseError(node, "Expected state store type " + expectedType + " but got " + type);
        }

        return parseStore(node, type);
    }

    private StoreType storeTypeOf(String type) {
        if (type == null) return null;
        return switch (type) {
            case Stores.TYPE_KEYVALUE -> StoreType.KEYVALUE_STORE;
            case Stores.TYPE_SESSION -> StoreType.SESSION_STORE;
            case Stores.TYPE_WINDOW -> StoreType.WINDOW_STORE;
            default -> null;
        };
    }

    private StateStoreDefinition parseStore(YamlNode node, StoreType type) {
        return switch (type) {
            case KEYVALUE_STORE -> new KeyValueStateStoreDefinition(
                    parseString(node, Stores.NAME, getDefaultName()),
                    parseBoolean(node, Stores.PERSISTENT),
                    parseBoolean(node, Stores.TIMESTAMPED),
                    parseBoolean(node, Stores.VERSIONED),
                    parseDuration(node, Stores.HISTORY_RETENTION),
                    parseDuration(node, Stores.SEGMENT_INTERVAL),
                    UserTypeParser.parse(parseString(node, Stores.KEY_TYPE)),
                    UserTypeParser.parse(parseString(node, Stores.VALUE_TYPE)),
                    parseBoolean(node, Stores.CACHING),
                    parseBoolean(node, Stores.LOGGING));
            case SESSION_STORE -> new SessionStateStoreDefinition(
                    parseString(node, Stores.NAME, getDefaultName()),
                    parseBoolean(node, Stores.PERSISTENT),
                    parseBoolean(node, Stores.TIMESTAMPED),
                    parseDuration(node, Stores.RETENTION),
                    UserTypeParser.parse(parseString(node, Stores.KEY_TYPE)),
                    UserTypeParser.parse(parseString(node, Stores.VALUE_TYPE)),
                    parseBoolean(node, Stores.CACHING),
                    parseBoolean(node, Stores.LOGGING));
            case WINDOW_STORE -> new WindowStateStoreDefinition(
                    parseString(node, Stores.NAME, getDefaultName()),
                    parseBoolean(node, Stores.PERSISTENT),
                    parseBoolean(node, Stores.TIMESTAMPED),
                    parseDuration(node, Stores.RETENTION),
                    parseDuration(node, Stores.WINDOWSIZE),
                    parseBoolean(node, Stores.RETAIN_DUPLICATES),
                    UserTypeParser.parse(parseString(node, Stores.KEY_TYPE)),
                    UserTypeParser.parse(parseString(node, Stores.VALUE_TYPE)),
                    parseBoolean(node, Stores.CACHING),
                    parseBoolean(node, Stores.LOGGING));
        };
    }
}
