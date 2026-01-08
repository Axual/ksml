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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

/**
 * A proxy wrapper around a Kafka Streams WindowStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class WindowStoreProxy<K, V> implements WindowStore<K, V> {
    private final WindowStore<K, V> delegate;

    public WindowStoreProxy(WindowStore<K, V> delegate) {
        this.delegate = delegate;
    }

    // ==================== WindowStore methods ====================

    @Override
    public void put(K key, V value, long windowStartTimestamp) {
        delegate.put(key, value, windowStartTimestamp);
    }

    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        return delegate.fetch(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<V> fetch(K key, Instant timeFrom, Instant timeTo) {
        return delegate.fetch(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(K key, long timeFrom, long timeTo) {
        return delegate.backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(K key, Instant timeFrom, Instant timeTo) {
        return delegate.backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return delegate.fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) {
        return delegate.fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return delegate.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) {
        return delegate.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom, long timeTo) {
        return delegate.fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(Instant timeFrom, Instant timeTo) {
        return delegate.fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(long timeFrom, long timeTo) {
        return delegate.backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(Instant timeFrom, Instant timeTo) {
        return delegate.backwardFetchAll(timeFrom, timeTo);
    }

    // ==================== ReadOnlyWindowStore methods ====================

    @Override
    public V fetch(K key, long time) {
        return delegate.fetch(key, time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return delegate.all();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        return delegate.backwardAll();
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
