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

import java.util.List;

/**
 * A proxy wrapper around a Kafka Streams KeyValueStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class KeyValueStoreProxy<K, V> implements KeyValueStore<K, V> {
    private final KeyValueStore<K, V> delegate;

    public KeyValueStoreProxy(KeyValueStore<K, V> delegate) {
        this.delegate = delegate;
    }

    // ==================== KeyValueStore methods ====================

    @Override
    public void put(K key, V value) {
        delegate.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        delegate.putAll(entries);
    }

    @Override
    public V delete(K key) {
        return delegate.delete(key);
    }

    // ==================== ReadOnlyKeyValueStore methods ====================

    @Override
    public V get(K key) {
        return delegate.get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return delegate.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(K from, K to) {
        return delegate.reverseRange(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return delegate.all();
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return delegate.reverseAll();
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(P prefix, PS prefixKeySerializer) {
        return delegate.prefixScan(prefix, prefixKeySerializer);
    }

    @Override
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }

    // ==================== StateStore methods ====================

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        delegate.init(context, root);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return delegate.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return delegate.getPosition();
    }
}
