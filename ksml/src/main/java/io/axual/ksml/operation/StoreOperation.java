package io.axual.ksml.operation;

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
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.type.UserType;
import lombok.Getter;

@Getter
public class StoreOperation extends BaseOperation {
    private final StateStoreDefinition store;

    public StoreOperation(StoreOperationConfig config) {
        super(config);
        this.store = config.store;
    }

    @Override
    public String toString() {
        return super.toString() + (store != null && store.name() != null ? " [storeName=\"" + store.name() + "\"]" : "");
    }

    protected KeyValueStateStoreDefinition validateKeyValueStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateKeyValueStore(store, keyType.userType(), valueType.userType());
    }

    protected KeyValueStateStoreDefinition validateKeyValueStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof KeyValueStateStoreDefinition keyValueStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : keyValueStore.keyType();
            final var storeValueType = valueType != null ? valueType : keyValueStore.valueType();
            return new KeyValueStateStoreDefinition(
                    keyValueStore.name(),
                    keyValueStore.persistent(),
                    keyValueStore.timestamped(),
                    keyValueStore.versioned(),
                    keyValueStore.historyRetention(),
                    keyValueStore.segmentInterval(),
                    storeKeyType,
                    storeValueType,
                    keyValueStore.caching(),
                    keyValueStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'keyValue'");
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateSessionStore(store, keyType.userType(), valueType.userType());
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof SessionStateStoreDefinition sessionStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : sessionStore.keyType();
            final var storeValueType = valueType != null ? valueType : sessionStore.valueType();
            return new SessionStateStoreDefinition(
                    sessionStore.name(),
                    sessionStore.persistent(),
                    sessionStore.timestamped(),
                    sessionStore.retention(),
                    storeKeyType,
                    storeValueType,
                    sessionStore.caching(),
                    sessionStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'session'");
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateWindowStore(store, keyType.userType(), valueType.userType());
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof WindowStateStoreDefinition windowStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : windowStore.keyType();
            final var storeValueType = valueType != null ? valueType : windowStore.valueType();
            return new WindowStateStoreDefinition(
                    windowStore.name(),
                    windowStore.persistent(),
                    windowStore.timestamped(),
                    windowStore.retention(),
                    windowStore.windowSize(),
                    windowStore.retainDuplicates(),
                    storeKeyType,
                    storeValueType,
                    windowStore.caching(),
                    windowStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'window'");
    }

    private void validateStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        validateStoreTypeWithStreamType("key", store.keyType(), keyType);
        validateStoreTypeWithStreamType("value", store.valueType(), valueType);
    }

    private void validateStoreTypeWithStreamType(String keyOrValue, UserType storeKeyOrValueType, UserType streamKeyOrValueType) {
        if (streamKeyOrValueType == null) {
            if (storeKeyOrValueType == null) {
                throw new ExecutionException("State store '" + store.name() + "' does not have a defined " + keyOrValue + " type");
            }
            return;
        }

        if (storeKeyOrValueType != null && !storeKeyOrValueType.dataType().isAssignableFrom(streamKeyOrValueType.dataType())) {
            throw new ExecutionException("Incompatible " + keyOrValue + " types for state store '" + store.name() + "': " + storeKeyOrValueType + " and " + streamKeyOrValueType);
        }
    }
}
