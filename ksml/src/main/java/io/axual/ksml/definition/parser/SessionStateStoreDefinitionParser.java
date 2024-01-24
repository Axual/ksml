package io.axual.ksml.definition.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

public class SessionStateStoreDefinitionParser extends DefinitionParser<SessionStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireType;
    private String defaultName;

    public SessionStateStoreDefinitionParser(boolean requireType) {
        this.requireType = requireType;
    }

    @Override
    protected StructParser<SessionStateStoreDefinition> parser() {
        return structParser(
                SessionStateStoreDefinition.class,
                "Definition of a session state store",
                fixedStringField(KSMLDSL.Stores.TYPE, requireType, StoreType.SESSION_STORE.externalName(), "The type of the state store"),
                stringField(KSMLDSL.Stores.NAME, false, null, "The name of the session store. If this field is not defined, then the name is derived from the context."),
                booleanField(KSMLDSL.Stores.PERSISTENT, false, "\"true\" if this session store needs to be stored on disk, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.TIMESTAMPED, false, "\"true\" if elements in the store are timestamped, \"false\" otherwise"),
                durationField(KSMLDSL.Stores.RETENTION, false, "The duration for which elements in the session store are retained"),
                userTypeField(KSMLDSL.Stores.KEY_TYPE, false, "The key type of the session store"),
                userTypeField(KSMLDSL.Stores.VALUE_TYPE, false, "The value type of the session store"),
                booleanField(KSMLDSL.Stores.CACHING, false, "\"true\" if changed to the session store need to be buffered and periodically released, \"false\" to emit all changes directly"),
                booleanField(KSMLDSL.Stores.LOGGING, false, "\"true\" if a changelog topic should be set up on Kafka for this session store, \"false\" otherwise"),
                (type, name, persistent, timestamped, retention, keyType, valueType, caching, logging) -> {
                    // Validate the type field if one was provided
                    if (type != null && !StoreType.SESSION_STORE.externalName().equals(type)) {
                        return parseError("Expected store type \"" + StoreType.SESSION_STORE.externalName() + "\"");
                    }
                    name = validateName("Session state store", name, defaultName);
                    return new SessionStateStoreDefinition(name, persistent, timestamped, retention, keyType, valueType, caching, logging);
                });
    }

    @Override
    public void defaultName(String name) {
        defaultName = name;
    }
}
