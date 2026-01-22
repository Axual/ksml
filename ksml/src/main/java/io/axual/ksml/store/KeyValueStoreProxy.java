package io.axual.ksml.store;

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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.graalvm.polyglot.HostAccess;

import java.util.List;

/**
 * A proxy wrapper around a Kafka Streams KeyValueStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class KeyValueStoreProxy<K, V> extends AbstractStateStoreProxy<KeyValueStore<K,V>> implements KeyValueStore<K, V> {

    public KeyValueStoreProxy(KeyValueStore<K, V> delegate) {
        super(delegate);
    }

    // ==================== KeyValueStore methods ====================

    @Override
    @HostAccess.Export
    public void put(K key, V value) {
        delegate.put(key, value);
    }

    @Override
    @HostAccess.Export
    public V putIfAbsent(K key, V value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    @HostAccess.Export
    public void putAll(List<KeyValue<K, V>> entries) {
        delegate.putAll(entries);
    }

    @Override
    @HostAccess.Export
    public V delete(K key) {
        return delegate.delete(key);
    }

    // ==================== ReadOnlyKeyValueStore methods ====================

    @Override
    @HostAccess.Export
    public V get(K key) {
        return delegate.get(key);
    }

    @Override
    @HostAccess.Export
    public KeyValueIterator<K, V> range(K from, K to) {
        throw new UnsupportedOperationException("range(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @Override
    @HostAccess.Export
    public KeyValueIterator<K, V> reverseRange(K from, K to) {
        throw new UnsupportedOperationException("reverseRange(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @Override
    @HostAccess.Export
    public KeyValueIterator<K, V> all() {
        throw new UnsupportedOperationException("all() is not supported by this proxy (" + getClass() + ")");
    }

    @Override
    @HostAccess.Export
    public KeyValueIterator<K, V> reverseAll() {
        throw new UnsupportedOperationException("reverseAll() is not supported by this proxy (" + getClass() + ")");
    }

    @Override
    @HostAccess.Export
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(P prefix, PS prefixKeySerializer) {
        throw new UnsupportedOperationException("prefixScan(P, PS) is not supported by this proxy (" + getClass() + ")");
    }

    @Override
    @HostAccess.Export
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }

}
