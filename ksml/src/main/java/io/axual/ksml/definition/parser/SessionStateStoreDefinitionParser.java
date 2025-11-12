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
import io.axual.ksml.parser.StructsParser;

public class SessionStateStoreDefinitionParser extends DefinitionParser<SessionStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireStoreType;
    private final boolean requireKeyValueType;
    private String defaultShortName;

    public SessionStateStoreDefinitionParser(boolean requireStoreType, boolean requireKeyValueType) {
        this.requireStoreType = requireStoreType;
        this.requireKeyValueType = requireKeyValueType;
    }

    @Override
    protected StructsParser<SessionStateStoreDefinition> parser() {
        final var nameField = optional(stringField(KSMLDSL.Stores.NAME, false, "The name of the session store. If this field is not defined, then the name is derived from the context."));
        final var persistentField = optional(booleanField(KSMLDSL.Stores.PERSISTENT, "\"true\" if this session store needs to be stored on disk, \"false\" otherwise"));
        final var timestampedField = optional(booleanField(KSMLDSL.Stores.TIMESTAMPED, "\"true\" if elements in the store are timestamped, \"false\" otherwise"));
        final var retentionField = optional(durationField(KSMLDSL.Stores.RETENTION, "The duration for which elements in the session store are retained"));
        final var keyTypeField = userTypeField(KSMLDSL.Stores.KEY_TYPE, "The key type of the session store");
        final var valueTypeField = userTypeField(KSMLDSL.Stores.VALUE_TYPE, "The value type of the session store");
        final var cachingField = optional(booleanField(KSMLDSL.Stores.CACHING, "\"true\" if changed to the session store need to be buffered and periodically released, \"false\" to emit all changes directly"));
        final var loggingField = optional(booleanField(KSMLDSL.Stores.LOGGING, "\"true\" if a changelog topic should be set up on Kafka for this session store, \"false\" otherwise"));

        // Determine this parser's name by the two input booleans
        final var parserPostfix = (requireStoreType ? "" : KSMLDSL.Types.WITH_IMPLICIT_STORE_TYPE_POSTFIX)
                + (requireKeyValueType ? "" : KSMLDSL.Types.WITH_IMPLICIT_KEY_AND_VALUE_TYPE);

        if (requireKeyValueType) return structsParser(
                SessionStateStoreDefinition.class,
                parserPostfix,
                "Definition of a session state store",
                nameField,
                persistentField,
                timestampedField,
                retentionField,
                keyTypeField,
                valueTypeField,
                cachingField,
                loggingField,
                (name, persistent, timestamped, retention, keyType, valueType, caching, logging, tags) -> {
                    name = validateName("Session state store", name, defaultShortName);
                    return new SessionStateStoreDefinition(name, persistent, timestamped, retention, keyType, valueType, caching, logging);
                });

        return structsParser(
                SessionStateStoreDefinition.class,
                parserPostfix,
                "Definition of a session state store",
                nameField,
                persistentField,
                timestampedField,
                retentionField,
                cachingField,
                loggingField,
                (name, persistent, timestamped, retention, caching, logging, tags) -> {
                    name = validateName("Session state store", name, defaultShortName);
                    return new SessionStateStoreDefinition(name, persistent, timestamped, retention, null, null, caching, logging);
                });
    }

    @Override
    public void defaultShortName(String name) {
        defaultShortName = name;
    }
}
