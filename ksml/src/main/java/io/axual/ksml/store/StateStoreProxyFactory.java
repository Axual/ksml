package io.axual.ksml.store;

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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Factory class for creating proxy wrappers around Kafka Streams state stores.
 * The proxies delegate all operations to the underlying store while providing
 * a controlled interface that can be safely exposed to user code (e.g., Python functions).
 */
public class StateStoreProxyFactory {

    private StateStoreProxyFactory() {
        // Utility class, prevent instantiation
    }

    /**
     * Wraps a state store in an appropriate proxy based on its type.
     *
     * @param store the state store to wrap
     * @return a proxy wrapper around the store, or the original store if no proxy is available
     */
    @SuppressWarnings("unchecked")
    public static StateStore wrap(StateStore store) {
        if (store instanceof VersionedKeyValueStore<?, ?> versionedStore) {
            return new VersionedKeyValueStoreProxy<>((VersionedKeyValueStore<Object, Object>) versionedStore);
        } else if (store instanceof KeyValueStore<?, ?> kvStore) {
            return new KeyValueStoreProxy<>((KeyValueStore<Object, Object>) kvStore);
        } else if (store instanceof SessionStore<?, ?> sessionStore) {
            return new SessionStoreProxy<>((SessionStore<Object, Object>) sessionStore);
        } else if (store instanceof WindowStore<?, ?> windowStore) {
            return new WindowStoreProxy<>((WindowStore<Object, Object>) windowStore);
        }
        // Return unwrapped for unknown store types
        return store;
    }
}
