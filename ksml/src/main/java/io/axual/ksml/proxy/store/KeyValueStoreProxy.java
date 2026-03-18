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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.graalvm.polyglot.HostAccess;

import java.util.List;

/**
 * A proxy for accessing Kafka Streams KeyValueStore in Python code. This proxy mediates between Python and Java data
 * types and delegates all operations to the underlying store.
 */
public class KeyValueStoreProxy extends AbstractStateStoreProxy<KeyValueStore<Object, Object>> {

    public KeyValueStoreProxy(KeyValueStore<Object, Object> delegate) {
        super(delegate);
    }

    // ==================== ReadOnlyKeyValueStore methods ====================

    @HostAccess.Export
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }

    @HostAccess.Export
    public KeyValueIterator<Object, Object> all() {
        throw new UnsupportedOperationException("all() is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public Object get(Object key) {
        return ProxyUtil.toPython(delegate.get(NATIVE_MAPPER.fromPython(key)));
    }

    @HostAccess.Export
    public KeyValueIterator<Object, Object> range(Object from, Object to) {
        throw new UnsupportedOperationException("range(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Object, Object> reverseAll() {
        throw new UnsupportedOperationException("reverseAll() is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Object, Object> reverseRange(Object from, Object to) {
        throw new UnsupportedOperationException("reverseRange(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public <PS extends Serializer<P>, P> KeyValueIterator<Object, Object> prefixScan(P prefix, PS prefixKeySerializer) {
        throw new UnsupportedOperationException("prefixScan(P, PS) is not supported by this proxy (" + getClass() + ")");
    }

    // ==================== KeyValueStore methods ====================

    @HostAccess.Export
    public Object delete(Object key) {
        return ProxyUtil.toPython(delegate.delete(NATIVE_MAPPER.fromPython(key)));
    }

    @HostAccess.Export
    public void put(Object key, Object value) {
        delegate.put(key, value);
    }

    @HostAccess.Export
    public void putAll(List<KeyValue<Object, Object>> entries) {
        throw new UnsupportedOperationException("putAll(List<KeyValue<K, V>>) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value) {
        return ProxyUtil.toPython(delegate.putIfAbsent(NATIVE_MAPPER.fromPython(key), NATIVE_MAPPER.fromPython(value)));
    }
}
