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
        this(null, null);
    }

    public StateStoreDefinitionParser(StoreType expectedType, String defaultName) {
        this.expectedType = expectedType;
        setDefaultName(defaultName);
    }

    @Override
    public StateStoreDefinition parse(YamlNode node) {
        if (node == null) return null;
        var type = storeTypeOf(parseString(node, STORE_TYPE_ATTRIBUTE));
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
            case STORE_TYPE_KEYVALUE -> StoreType.KEYVALUE_STORE;
            case STORE_TYPE_SESSION -> StoreType.SESSION_STORE;
            case STORE_TYPE_WINDOW -> StoreType.WINDOW_STORE;
            default -> null;
        };
    }

    private StateStoreDefinition parseStore(YamlNode node, StoreType type) {
        return switch (type) {
            case KEYVALUE_STORE -> new KeyValueStateStoreDefinition(
                    parseString(node, STORE_NAME_ATTRIBUTE, getDefaultName()),
                    parseBoolean(node, STORE_PERSISTENT_ATTRIBUTE),
                    parseBoolean(node, STORE_TIMESTAMPED_ATTRIBUTE),
                    parseBoolean(node, STORE_VERSIONED_ATTRIBUTE),
                    parseDuration(node, STORE_HISTORY_RETENTION_ATTRIBUTE),
                    parseDuration(node, STORE_SEGMENT_INTERVAL_ATTRIBUTE),
                    UserTypeParser.parse(parseString(node, STORE_KEYTYPE_ATTRIBUTE)),
                    UserTypeParser.parse(parseString(node, STORE_VALUETYPE_ATTRIBUTE)),
                    parseBoolean(node, STORE_CACHING_ATTRIBUTE),
                    parseBoolean(node, STORE_LOGGING_ATTRIBUTE));
            case SESSION_STORE -> new SessionStateStoreDefinition(
                    parseString(node, STORE_NAME_ATTRIBUTE, getDefaultName()),
                    parseBoolean(node, STORE_PERSISTENT_ATTRIBUTE),
                    parseBoolean(node, STORE_TIMESTAMPED_ATTRIBUTE),
                    parseDuration(node, STORE_RETENTION_ATTRIBUTE),
                    UserTypeParser.parse(parseString(node, STORE_KEYTYPE_ATTRIBUTE)),
                    UserTypeParser.parse(parseString(node, STORE_VALUETYPE_ATTRIBUTE)),
                    parseBoolean(node, STORE_CACHING_ATTRIBUTE),
                    parseBoolean(node, STORE_LOGGING_ATTRIBUTE));
            case WINDOW_STORE -> new WindowStateStoreDefinition(
                    parseString(node, STORE_NAME_ATTRIBUTE, getDefaultName()),
                    parseBoolean(node, STORE_PERSISTENT_ATTRIBUTE),
                    parseBoolean(node, STORE_TIMESTAMPED_ATTRIBUTE),
                    parseDuration(node, STORE_RETENTION_ATTRIBUTE),
                    parseDuration(node, STORE_WINDOWSIZE_ATTRIBUTE),
                    parseBoolean(node, STORE_RETAINDUPLICATES_ATTRIBUTE),
                    UserTypeParser.parse(parseString(node, STORE_KEYTYPE_ATTRIBUTE)),
                    UserTypeParser.parse(parseString(node, STORE_VALUETYPE_ATTRIBUTE)),
                    parseBoolean(node, STORE_CACHING_ATTRIBUTE),
                    parseBoolean(node, STORE_LOGGING_ATTRIBUTE));
        };
    }
}
