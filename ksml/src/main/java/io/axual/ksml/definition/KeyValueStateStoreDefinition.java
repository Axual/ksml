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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.store.StoreType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.time.Duration;

@Getter
@EqualsAndHashCode
public class KeyValueStateStoreDefinition extends StateStoreDefinition {
    private final boolean versioned;
    private final Duration historyRetention;
    private final Duration segmentInterval;

    public KeyValueStateStoreDefinition(String name, UserType keyType, UserType valueType) {
        this(name, false, false, false, null, null, keyType, valueType, false, false);
    }

    public KeyValueStateStoreDefinition(String name, Boolean persistent, Boolean timestamped, Boolean versioned, Duration historyRetention, Duration segmentInterval, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        super(StoreType.KEYVALUE_STORE, name, persistent, timestamped, keyType, valueType, caching, logging);
        this.versioned = versioned != null && versioned;
        this.historyRetention = historyRetention != null ? historyRetention : Duration.ZERO;
        this.segmentInterval = segmentInterval != null ? segmentInterval : Duration.ZERO;
    }

    public KeyValueStateStoreDefinition with(String name, UserType keyType, UserType valueType) {
        return new KeyValueStateStoreDefinition(name, persistent(), timestamped(), versioned(), historyRetention(), segmentInterval(), keyType, valueType, caching(), logging());
    }
}
