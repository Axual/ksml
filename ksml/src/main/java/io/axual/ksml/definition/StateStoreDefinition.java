package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.store.StoreType;

public abstract class StateStoreDefinition {
    private final StoreType type;
    private final String name;
    private final Boolean persistent;
    private final Boolean timestamped;
    private final UserType keyType;
    private final UserType valueType;
    private final Boolean caching;
    private final Boolean logging;

    public StateStoreDefinition(StoreType type, String name, Boolean persistent, Boolean timestamped, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        this.type = type;
        this.name = name;
        this.persistent = persistent;
        this.timestamped = timestamped;
        this.keyType = keyType;
        this.valueType = valueType;
        this.caching = caching;
        this.logging = logging;
    }

    public String toString() {
        var type = getClass().getSimpleName();
        if (type.toLowerCase().endsWith("definition")) {
            type = type.substring(0, type.length() - 10);
        }
        return type + " [name=" + (name == null ? "Unnamed" : name) + "]";
    }

    public StoreType type() {
        return type;
    }

    public String name() {
        return name;
    }

    public boolean persistent() {
        return persistent != null && persistent;
    }

    public boolean timestamped() {
        return timestamped != null && timestamped;
    }

    public UserType keyType() {
        return keyType;
    }

    public UserType valueType() {
        return valueType;
    }

    public boolean caching() {
        return caching != null && caching;
    }

    public boolean logging() {
        return logging != null && logging;
    }
}
