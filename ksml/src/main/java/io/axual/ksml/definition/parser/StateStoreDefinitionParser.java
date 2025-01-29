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


import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.parser.ChoiceParser;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

import java.util.HashMap;
import java.util.Map;

import static io.axual.ksml.dsl.KSMLDSL.Stores;

public class StateStoreDefinitionParser extends ChoiceParser<StateStoreDefinition> {
    public StateStoreDefinitionParser(boolean isTableBackingStore) {
        this(null, isTableBackingStore);
    }

    public StateStoreDefinitionParser(StoreType expectedType, boolean isTableBackingStore) {
        super(Stores.TYPE, StoreType.class.getSimpleName(), "state store", null, types(expectedType, isTableBackingStore));
    }

    private static Map<String, StructsParser<? extends StateStoreDefinition>> types(StoreType expectedType, boolean isTableBackingStore) {
        final var result = new HashMap<String, StructsParser<? extends StateStoreDefinition>>();
        if (expectedType == null || expectedType == StoreType.KEYVALUE_STORE) {
            result.put(StoreType.KEYVALUE_STORE.externalName(), new KeyValueStateStoreDefinitionParser(expectedType == null, isTableBackingStore));
        }
        if (expectedType == null || expectedType == StoreType.SESSION_STORE) {
            result.put(StoreType.SESSION_STORE.externalName(), new SessionStateStoreDefinitionParser(expectedType == null));
        }
        if (expectedType == null || expectedType == StoreType.WINDOW_STORE) {
            result.put(StoreType.WINDOW_STORE.externalName(), new WindowStateStoreDefinitionParser(expectedType == null));
        }
        return result;
    }
}
