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

import io.axual.ksml.data.parser.NamedObjectParser;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructsParser;

public class WindowStateStoreDefinitionParser extends DefinitionParser<WindowStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireType;
    private String defaultName;

    public WindowStateStoreDefinitionParser(boolean requireType) {
        this.requireType = requireType;
    }

    @Override
    protected StructsParser<WindowStateStoreDefinition> parser() {
        return structsParser(
                WindowStateStoreDefinition.class,
                requireType ? "" : KSMLDSL.Types.WITH_IMPLICIT_TYPE_POSTFIX,
                "Definition of a window state store",
                optional(stringField(KSMLDSL.Stores.NAME, false, null, "The name of the window store. If this field is not defined, then the name is derived from the context.")),
                optional(booleanField(KSMLDSL.Stores.PERSISTENT, "\"true\" if this window store needs to be stored on disk, \"false\" otherwise")),
                optional(booleanField(KSMLDSL.Stores.TIMESTAMPED, "\"true\" if elements in the store are timestamped, \"false\" otherwise")),
                optional(durationField(KSMLDSL.Stores.RETENTION, "The duration for which elements in the window store are retained")),
                optional(durationField(KSMLDSL.Stores.WINDOW_SIZE, "Size of the windows (cannot be negative)")),
                optional(booleanField(KSMLDSL.Stores.RETAIN_DUPLICATES, "Whether or not to retain duplicates")),
                optional(userTypeField(KSMLDSL.Stores.KEY_TYPE, "The key type of the window store")),
                optional(userTypeField(KSMLDSL.Stores.VALUE_TYPE, "The value type of the window store")),
                optional(booleanField(KSMLDSL.Stores.CACHING, "\"true\" if changed to the window store need to be buffered and periodically released, \"false\" to emit all changes directly")),
                optional(booleanField(KSMLDSL.Stores.LOGGING, "\"true\" if a changelog topic should be set up on Kafka for this window store, \"false\" otherwise")),
                (name, persistent, timestamped, retention, windowSize, retainDuplicates, keyType, valueType, caching, logging, tags) -> {
                    name = validateName("Window state store", name, defaultName);
                    return new WindowStateStoreDefinition(name, persistent, timestamped, retention, windowSize, retainDuplicates, keyType, valueType, caching, logging);
                });
    }

    @Override
    public void defaultName(String name) {
        defaultName = name;
    }
}
