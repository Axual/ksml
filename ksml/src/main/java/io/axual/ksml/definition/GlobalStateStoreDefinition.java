package io.axual.ksml.definition;

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

import io.axual.ksml.store.StoreType;
import io.axual.ksml.type.UserType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public abstract class GlobalStateStoreDefinition extends AbstractDefinition {
    private final StoreType type;
    private final String name;
    private final boolean persistent;
    private final boolean timestamped;
    private final UserType keyType;
    private final UserType valueType;
    private final boolean caching;
    private final boolean logging;

    protected GlobalStateStoreDefinition(StoreType type, String name, Boolean persistent, Boolean timestamped, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        this.type = type;
        this.name = name;
        this.persistent = persistent != null && persistent;
        this.timestamped = timestamped != null && timestamped;
        this.keyType = keyType != null ? keyType : UserType.UNKNOWN;
        this.valueType = valueType != null ? valueType : UserType.UNKNOWN;
        this.caching = caching != null && caching;
        this.logging = logging != null && logging;
    }

    @Override
    public String toString() {
        return super.toString() + " [name=" + (name == null ? "Unnamed" : name) + "]";
    }
}
