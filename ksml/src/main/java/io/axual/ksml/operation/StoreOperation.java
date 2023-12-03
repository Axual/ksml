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


import io.axual.ksml.data.type.UserType;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.StreamDataType;
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
        validateStore(store, keyType, valueType);
        if (store instanceof KeyValueStateStoreDefinition def) {
            return new KeyValueStateStoreDefinition(
                    def.name(),
                    def.persistent(),
                    def.timestamped(),
                    def.versioned(),
                    def.historyRetention(),
                    def.segmentInterval(),
                    keyType != null ? keyType : def.keyType(),
                    valueType != null ? valueType : def.valueType(),
                    def.caching(),
                    def.logging());
        }
        throw FatalError.executionError(this + " requires a  state store of type 'keyValue'");
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateSessionStore(store, keyType.userType(), valueType.userType());
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        validateStore(store, keyType, valueType);
        if (store instanceof SessionStateStoreDefinition def) {
            return new SessionStateStoreDefinition(
                    def.name(),
                    def.persistent(),
                    def.timestamped(),
                    def.retention(),
                    keyType != null ? keyType : def.keyType(),
                    valueType != null ? valueType : def.valueType(),
                    def.caching(),
                    def.logging());
        }
        throw FatalError.executionError(this + " requires a  state store of type 'session'");
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateWindowStore(store, keyType.userType(), valueType.userType());
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        validateStore(store, keyType, valueType);
        if (store instanceof WindowStateStoreDefinition def) {
            return new WindowStateStoreDefinition(
                    def.name(),
                    def.persistent(),
                    def.timestamped(),
                    def.retention(),
                    def.windowSize(),
                    def.retainDuplicates(),
                    keyType != null ? keyType : def.keyType(),
                    valueType != null ? valueType : def.valueType(),
                    def.caching(),
                    def.logging());
        }
        throw FatalError.executionError(this + " requires a  state store of type 'window'");
    }

    private void validateStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        validateStoreTypeWithStreamType("key", store.keyType(), keyType);
        validateStoreTypeWithStreamType("value", store.valueType(), valueType);
    }

    private void validateStoreTypeWithStreamType(String keyOrValue, UserType storeKeyOrValueType, UserType streamKeyOrValueType) {
        if (streamKeyOrValueType == null) {
            if (storeKeyOrValueType == null) {
                throw FatalError.executionError("State store '" + store.name() + "' does not have a defined " + keyOrValue + " type");
            }
            return;
        }

        if (storeKeyOrValueType != null && !storeKeyOrValueType.dataType().isAssignableFrom(streamKeyOrValueType.dataType())) {
            throw FatalError.executionError("Incompatible " + keyOrValue + " types for state store '" + store.name() + "': " + storeKeyOrValueType + " and " + streamKeyOrValueType);
        }
    }
}
