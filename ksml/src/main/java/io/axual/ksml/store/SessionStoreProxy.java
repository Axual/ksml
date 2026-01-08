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
import org.apache.kafka.streams.state.SessionStore;

import java.time.Instant;

/**
 * A proxy wrapper around a Kafka Streams SessionStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <AGG> the type of aggregated values
 */
public class SessionStoreProxy<K, AGG> implements SessionStore<K, AGG> {
    private final SessionStore<K, AGG> delegate;

    public SessionStoreProxy(SessionStore<K, AGG> delegate) {
        this.delegate = delegate;
    }

    // ==================== SessionStore methods ====================

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        return delegate.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        return delegate.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        return delegate.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom, K keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        return delegate.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        return delegate.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        return delegate.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        return delegate.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K keyFrom, K keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        return delegate.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public AGG fetchSession(K key, long sessionStartTime, long sessionEndTime) {
        return delegate.fetchSession(key, sessionStartTime, sessionEndTime);
    }

    @Override
    public AGG fetchSession(K key, Instant sessionStartTime, Instant sessionEndTime) {
        return delegate.fetchSession(key, sessionStartTime, sessionEndTime);
    }

    @Override
    public void put(Windowed<K> sessionKey, AGG aggregate) {
        delegate.put(sessionKey, aggregate);
    }

    @Override
    public void remove(Windowed<K> sessionKey) {
        delegate.remove(sessionKey);
    }

    // ==================== ReadOnlySessionStore methods ====================

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(K key) {
        return delegate.fetch(key);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(K keyFrom, K keyTo) {
        return delegate.fetch(keyFrom, keyTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFetch(K key) {
        return delegate.backwardFetch(key);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> backwardFetch(K keyFrom, K keyTo) {
        return delegate.backwardFetch(keyFrom, keyTo);
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
