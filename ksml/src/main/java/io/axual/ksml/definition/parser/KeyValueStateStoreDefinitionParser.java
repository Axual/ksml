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

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructsParser;

public class KeyValueStateStoreDefinitionParser extends DefinitionParser<KeyValueStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireStoreType;
    private final boolean isTableBackingStore;
    private String defaultShortName;

    public KeyValueStateStoreDefinitionParser(boolean requireStoreType, boolean isTableBackingStore) {
        this.requireStoreType = requireStoreType;
        this.isTableBackingStore = isTableBackingStore;
    }

    @Override
    protected StructsParser<KeyValueStateStoreDefinition> parser() {
        final var nameField = optional(stringField(KSMLDSL.Stores.NAME, false, null, "The name of the keyValue store. If this field is not defined, then the name is derived from the context."));
        final var persistentField = optional(booleanField(KSMLDSL.Stores.PERSISTENT, "\"true\" if this keyValue store needs to be stored on disk, \"false\" otherwise"));
        final var timestampField = optional(booleanField(KSMLDSL.Stores.TIMESTAMPED, "\"true\" if elements in the store are timestamped, \"false\" otherwise"));
        final var versionedField = optional(booleanField(KSMLDSL.Stores.VERSIONED, "\"true\" if elements in the store are versioned, \"false\" otherwise"));
        final var historyRetentionField = optional(durationField(KSMLDSL.Stores.HISTORY_RETENTION, "(Versioned only) The duration for which old record versions are available for query (cannot be negative)"));
        final var segmentIntervalField = optional(durationField(KSMLDSL.Stores.SEGMENT_INTERVAL, "Size of segments for storing old record versions (must be positive). Old record versions for the same key in a single segment are stored (updated and accessed) together. The only impact of this parameter is performance. If segments are large and a workload results in many record versions for the same key being collected in a single segment, performance may degrade as a result. On the other hand, historical reads (which access older segments) and out-of-order writes may slow down if there are too many segments."));
        final var keyTypeField = optional(userTypeField(KSMLDSL.Stores.KEY_TYPE, "The key type of the keyValue store"));
        final var valueTypeField = optional(userTypeField(KSMLDSL.Stores.VALUE_TYPE, "The value type of the keyValue store"));
        final var cachingField = optional(booleanField(KSMLDSL.Stores.CACHING, "\"true\" if changed to the keyValue store need to be buffered and periodically released, \"false\" to emit all changes directly"));
        final var loggingField = optional(booleanField(KSMLDSL.Stores.LOGGING, "\"true\" if a changelog topic should be set up on Kafka for this keyValue store, \"false\" otherwise"));

        // Determine this parser's name by the two input booleans
        final var parserPostfix = (requireStoreType ? "" : KSMLDSL.Types.WITH_IMPLICIT_STORE_TYPE_POSTFIX) + (isTableBackingStore ? KSMLDSL.Types.WITH_IMPLICIT_KEY_AND_VALUE_TYPE : "");

        if (!isTableBackingStore) return structsParser(
                // Parse the state store including name, keyType and valueType
                KeyValueStateStoreDefinition.class,
                parserPostfix,
                "Definition of a keyValue state store",
                nameField,
                persistentField,
                timestampField,
                versionedField,
                historyRetentionField,
                segmentIntervalField,
                keyTypeField,
                valueTypeField,
                cachingField,
                loggingField,
                (name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging, tags) -> {
                    name = validateName("KeyValue state store", name, defaultShortName);
                    return new KeyValueStateStoreDefinition(name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging);
                });

        // Parse the state store without name, keyType and valueType
        return structsParser(
                KeyValueStateStoreDefinition.class,
                parserPostfix,
                "Definition of a keyValue state store",
                persistentField,
                timestampField,
                versionedField,
                historyRetentionField,
                segmentIntervalField,
                cachingField,
                loggingField,
                (persistent, timestamped, versioned, history, segment, caching, logging, tags) -> {
                    final var name = validateName("KeyValue state store", null, defaultShortName);
                    return new KeyValueStateStoreDefinition(name, persistent, timestamped, versioned, history, segment, null, null, caching, logging);
                });
    }

    @Override
    public void defaultShortName(String name) {
        defaultShortName = name;
    }
}
