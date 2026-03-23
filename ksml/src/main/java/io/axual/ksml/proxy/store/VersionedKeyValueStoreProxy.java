package io.axual.ksml.proxy.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy wrapper around a Kafka Streams VersionedKeyValueStore that delegates all operations
 * to the underlying store. This proxy exposes store methods to Python code via @HostAccess.Export.
 */
public class VersionedKeyValueStoreProxy extends AbstractStateStoreProxy<VersionedKeyValueStore<Object, Object>> {

    public VersionedKeyValueStoreProxy(VersionedKeyValueStore<Object, Object> delegate) {
        super(delegate);
    }

    // ==================== VersionedKeyValueStore methods ====================

    @HostAccess.Export
    public Object delete(Object key, long timestamp) {
        return ProxyUtil.toPython(delegate.delete(NATIVE_MAPPER.fromPython(key), timestamp));
    }

    @HostAccess.Export
    public Object get(Object key) {
        return ProxyUtil.toPython(delegate.get(NATIVE_MAPPER.fromPython(key)));
    }

    @HostAccess.Export
    public Object get(Object key, long asOfTimestamp) {
        return ProxyUtil.toPython(delegate.get(NATIVE_MAPPER.fromPython(key), asOfTimestamp));
    }

    @HostAccess.Export
    public Object put(Object key, Object value, long timestamp) {
        return ProxyUtil.toPython(delegate.put(NATIVE_MAPPER.fromPython(key), NATIVE_MAPPER.fromPython(value), timestamp));
    }
}
