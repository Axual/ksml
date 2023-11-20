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

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.store.StoreType;

import java.time.Duration;
import java.util.Objects;

public class KeyValueStateStoreDefinition extends StateStoreDefinition {
    private final Boolean versioned;
    private final Duration historyRetention;
    private final Duration segmentInterval;

    public KeyValueStateStoreDefinition(String name, UserType keyType, UserType valueType) {
        this(name, false, false, false, Duration.ZERO, Duration.ZERO, keyType, valueType, false, false);
    }

    public KeyValueStateStoreDefinition(String name, Boolean persistent, Boolean timestamped, Boolean versioned, Duration historyRetention, Duration segmentInterval, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        super(StoreType.KEYVALUE_STORE, name, persistent, timestamped, keyType, valueType, caching, logging);
        this.versioned = versioned;
        this.historyRetention = historyRetention;
        this.segmentInterval = segmentInterval;
    }

    public boolean versioned() {
        return versioned != null && versioned;
    }

    public Duration historyRetention() {
        return historyRetention != null ? historyRetention : Duration.ZERO;
    }

    public Duration segmentInterval() {
        return segmentInterval != null ? segmentInterval : Duration.ZERO;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof KeyValueStateStoreDefinition def)) return false;
        return (super.equals(other)
                && Objects.equals(versioned, def.versioned)
                && Objects.equals(historyRetention, def.historyRetention)
                && Objects.equals(segmentInterval, def.segmentInterval));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), versioned, historyRetention, segmentInterval);
    }
}
